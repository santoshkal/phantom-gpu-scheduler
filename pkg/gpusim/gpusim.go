// Package gpusim implements a Kubernetes scheduler plugin that performs
// GPU-aware placement using labels instead of real GPU resources.
//
// Node labels:
//
//	gpusim=true                 // node advertises simulated GPUs
//	gpusim-capacity=<int>       // total simulated GPUs on the node
//
// Pod labels:
//
//	requires-gpusim=true        // pod opts into gpusim scheduling
//	gpusim-request=<int>        // simulated GPUs required by the pod
//
// The plugin implements PreFilter (one-time pod validation), Filter
// (admission), Score (bin-packing), Reserve (in-flight capacity claim),
// and Unreserve (claim release) extension points. Allocated capacity per
// node is derived from the scheduler's NodeInfo snapshot plus an in-flight
// reservation map that prevents over-commit when multiple scheduling cycles
// run concurrently.
//
// Concurrency model
//
// In-flight reservations are tracked in a
//
//	map[string]map[types.UID]int64
//
// protected by sync.Mutex. The pendingReservations helper subtracts
// reservations for pods that have already appeared in the NodeInfo
// snapshot (i.e., were bound), preventing double-counting when the same
// pod is visible in both the snapshot and the in-flight map.
package gpusim

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	// Name is the plugin name used in KubeSchedulerConfiguration.
	Name = "GPUSim"

	// StateKey is the key under which the PreFilter result is stored
	// in framework.CycleState.
	StateKey = "gpusim.v1beta1"

	NodeLabelGPUSim         = "gpusim"
	NodeLabelGPUSimCapacity = "gpusim-capacity"
	PodLabelRequiresGPUSim  = "requires-gpusim"
	PodLabelGPUSimRequest   = "gpusim-request"
)

// preFilterState carries the parsed GPU request for a pod through the
// scheduling cycle. Produced by PreFilter, consumed by Filter, Score,
// Reserve, and Unreserve.
type preFilterState struct {
	request int64
	wants   bool
}

// Clone returns a copy of the state. Required by framework.StateData.
func (s *preFilterState) Clone() framework.StateData {
	return &preFilterState{request: s.request, wants: s.wants}
}

// GPUSim is a PreFilter+Filter+Score+Reserve+Unreserve plugin that
// schedules pods claiming simulated GPUs. In-flight capacity reservations
// are tracked by pod UID to prevent double-counting when a reservation
// later becomes a bound pod in the NodeInfo snapshot.
type GPUSim struct {
	handle framework.Handle

	mu sync.Mutex
	// reservations tracks pending GPU claims that have passed Reserve but not
	// yet appeared in the NodeInfo snapshot. Keyed by node name, then pod UID.
	reservations map[string]map[types.UID]int64
}

var (
	_ framework.PreFilterPlugin      = (*GPUSim)(nil)
	_ framework.FilterPlugin         = (*GPUSim)(nil)
	_ framework.ScorePlugin          = (*GPUSim)(nil)
	_ framework.ReservePlugin        = (*GPUSim)(nil)
	_ framework.EnqueueExtensions    = (*GPUSim)(nil)
)

// New is the plugin factory registered with the scheduler framework.
func New(_ context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	return &GPUSim{
		handle:       h,
		reservations: make(map[string]map[types.UID]int64),
	}, nil
}

// Name returns the plugin's registered name.
func (p *GPUSim) Name() string { return Name }

// ---------------------------------------------------------------------------
// PreFilter
// ---------------------------------------------------------------------------

// PreFilter validates the pod's GPU labels once per scheduling cycle and
// stores the parsed result in CycleState so that downstream extension points
// (Filter, Score, Reserve, Unreserve) can read it without re-parsing.
func (p *GPUSim) PreFilter(ctx context.Context, state *framework.CycleState, pod *v1.Pod) (*framework.PreFilterResult, *framework.Status) {
	request, wants, err := parseGPUSimRequest(pod)
	if err != nil {
		return nil, framework.NewStatus(framework.UnschedulableAndUnresolvable, err.Error())
	}
	state.Write(StateKey, &preFilterState{request: request, wants: wants})
	return nil, nil
}

// PreFilterExtensions returns nil because this plugin has no AddPod/RemovePod
// logic to maintain during the pre-filter score cache updates.
func (p *GPUSim) PreFilterExtensions() framework.PreFilterExtensions {
	return nil
}

// readPreFilterState retrieves the preFilterState previously stored by
// PreFilter. Returns a zero-valued state (wants=false) if no data was found
// — this can occur when the pod bypasses PreFilter (e.g., a non-GPU pod).
func readPreFilterState(state *framework.CycleState) *preFilterState {
	data, err := state.Read(StateKey)
	if err != nil {
		return &preFilterState{}
	}
	s, ok := data.(*preFilterState)
	if !ok {
		return &preFilterState{}
	}
	return s
}

// ---------------------------------------------------------------------------
// Filter
// ---------------------------------------------------------------------------

// Filter rejects nodes that cannot host the pod's GPU request.
func (p *GPUSim) Filter(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeInfo *framework.NodeInfo) *framework.Status {
	s := readPreFilterState(state)
	if !s.wants {
		return nil
	}

	node := nodeInfo.Node()
	if node == nil {
		return framework.NewStatus(framework.Error, "node not found in nodeInfo")
	}

	capacity, hasGPUSim, err := parseNodeGPUSim(node)
	if err != nil {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable,
			fmt.Sprintf("node %q has malformed gpusim labels: %v", node.Name, err))
	}
	if !hasGPUSim {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable,
			fmt.Sprintf("node %q does not advertise simulated GPUs", node.Name))
	}
	if capacity <= 0 {
		return framework.NewStatus(framework.Unschedulable,
			fmt.Sprintf("node %q has zero gpusim capacity", node.Name))
	}

	used := allocatedGPUSim(nodeInfo)
	reserved := p.pendingReservations(node.Name, nodeInfo)
	free := capacity - used - reserved
	if s.request > free {
		return framework.NewStatus(framework.Unschedulable,
			fmt.Sprintf("insufficient GPUs on %q: need %d, free %d/%d (used=%d, reserved=%d)",
				node.Name, s.request, free, capacity, used, reserved))
	}

	klog.V(4).InfoS("GPUSim filter pass",
		"pod", klog.KObj(pod), "node", node.Name,
		"request", s.request, "free", free, "capacity", capacity,
		"used", used, "reserved", reserved)
	return nil
}

// ---------------------------------------------------------------------------
// Score
// ---------------------------------------------------------------------------

// Score prefers nodes with the tightest fit after placing the pod (bin-packing).
// Score is in [0, framework.MaxNodeScore]; higher means better.
func (p *GPUSim) Score(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) (int64, *framework.Status) {
	s := readPreFilterState(state)
	if !s.wants {
		return 0, nil
	}

	nodeInfo, err2 := p.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err2 != nil {
		return 0, framework.NewStatus(framework.Error,
			fmt.Sprintf("getting nodeInfo %q: %v", nodeName, err2))
	}
	node := nodeInfo.Node()
	if node == nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("node %q not found", nodeName))
	}

	capacity, hasGPUSim, err := parseNodeGPUSim(node)
	if err != nil || !hasGPUSim || capacity == 0 {
		return 0, nil
	}

	used := allocatedGPUSim(nodeInfo)
	reserved := p.pendingReservations(nodeName, nodeInfo)
	freeAfter := capacity - used - reserved - s.request
	if freeAfter < 0 {
		return 0, nil
	}

	// Bin-pack: the more the node is occupied after placement, the higher
	// the score. score = (capacity - freeAfter) / capacity * MaxNodeScore.
	score := (capacity - freeAfter) * framework.MaxNodeScore / capacity
	klog.V(5).InfoS("GPUSim score",
		"pod", klog.KObj(pod), "node", nodeName,
		"request", s.request, "freeAfter", freeAfter, "capacity", capacity, "score", score,
		"used", used, "reserved", reserved)
	return score, nil
}

// ScoreExtensions returns nil; no NormalizeScore is needed because Score is
// already produced in the [0, MaxNodeScore] range.
func (p *GPUSim) ScoreExtensions() framework.ScoreExtensions { return nil }

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// parseGPUSimRequest reads the pod's gpusim labels.
// Returns wants=false when the pod does not opt in; in that case request and
// err are both zero-valued.
func parseGPUSimRequest(pod *v1.Pod) (request int64, wants bool, err error) {
	if pod.Labels[PodLabelRequiresGPUSim] != "true" {
		return 0, false, nil
	}
	raw, ok := pod.Labels[PodLabelGPUSimRequest]
	if !ok {
		// Opting in without a count means "one GPU".
		return 1, true, nil
	}
	req, perr := strconv.ParseInt(raw, 10, 64)
	if perr != nil {
		return 0, true, fmt.Errorf("invalid %s=%q: %w", PodLabelGPUSimRequest, raw, perr)
	}
	if req <= 0 {
		return 0, true, fmt.Errorf("%s=%d must be positive", PodLabelGPUSimRequest, req)
	}
	return req, true, nil
}

// parseNodeGPUSim reads the node's gpusim labels.
// Returns hasGPUSim=false when the node does not advertise simulated GPUs.
// Returns an error when gpusim=true is set but gpusim-capacity is missing
// or invalid (not a parsable integer or negative).
// Note: zero capacity is NOT an error here — the caller distinguishes
// Unschedulable (retryable, capacity may be updated) from
// UnschedulableAndUnresolvable (malformed, never retry).
func parseNodeGPUSim(node *v1.Node) (capacity int64, hasGPUSim bool, err error) {
	if node.Labels[NodeLabelGPUSim] != "true" {
		return 0, false, nil
	}
	raw, ok := node.Labels[NodeLabelGPUSimCapacity]
	if !ok {
		return 0, true, fmt.Errorf("node %q has %s=true but no %s label",
			node.Name, NodeLabelGPUSim, NodeLabelGPUSimCapacity)
	}
	cap, perr := strconv.ParseInt(raw, 10, 64)
	if perr != nil {
		return 0, true, fmt.Errorf("invalid %s=%q: %w", NodeLabelGPUSimCapacity, raw, perr)
	}
	if cap < 0 {
		return 0, true, fmt.Errorf("%s=%d must not be negative", NodeLabelGPUSimCapacity, cap)
	}
	return cap, true, nil
}

// allocatedGPUSim sums the GPU requests of every pod the scheduler has
// placed on the node (including in-flight assumed ones, because the snapshot
// carries them).
func allocatedGPUSim(nodeInfo *framework.NodeInfo) int64 {
	var total int64
	for _, p := range nodeInfo.Pods {
		req, wants, err := parseGPUSimRequest(p.Pod)
		if err != nil || !wants {
			continue
		}
		total += req
	}
	return total
}
