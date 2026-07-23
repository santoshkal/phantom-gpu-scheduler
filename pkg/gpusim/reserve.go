package gpusim

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// EventsToRegister returns the set of events that should trigger a retry of
// pods rejected by this plugin. When a node's gpusim labels change or a new
// gpusim-capable node appears, pods waiting on GPU capacity should re-queue.
func (p *GPUSim) EventsToRegister(_ context.Context) ([]framework.ClusterEventWithHint, error) {
	return []framework.ClusterEventWithHint{
		{Event: framework.ClusterEvent{Resource: framework.Node, ActionType: framework.Add | framework.UpdateNodeLabel}},
	}, nil
}

// ---------------------------------------------------------------------------
// Reserve / Unreserve
// ---------------------------------------------------------------------------

// Reserve claims capacity on the chosen node, preventing over-commit from
// concurrent scheduling cycles. Returns Unschedulable if the node no longer
// has sufficient capacity after Filter ran (due to another pod having been
// reserved first).
//
// This addresses the TOCTOU (time-of-check-time-of-use) race: between the
// moment Filter passes a node and the moment the binding is written, another
// scheduling cycle may reserve the last available unit. Reserve is the
// second, atomic check that closes this window.
func (p *GPUSim) Reserve(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) *framework.Status {
	s := readPreFilterState(state)
	if !s.wants {
		return nil
	}

	nodeInfo, err2 := p.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err2 != nil {
		return framework.NewStatus(framework.Error,
			fmt.Sprintf("getting nodeInfo %q: %v", nodeName, err2))
	}
	node := nodeInfo.Node()
	if node == nil {
		return framework.NewStatus(framework.Error, fmt.Sprintf("node %q not found", nodeName))
	}
	capacity, _, err := parseNodeGPUSim(node)
	if err != nil {
		return framework.NewStatus(framework.UnschedulableAndUnresolvable,
			fmt.Sprintf("node %q malformed labels: %v", nodeName, err))
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	used := allocatedGPUSim(nodeInfo)
	reserved := p.sumReservationsLocked(nodeName)
	if used+reserved+s.request > capacity {
		return framework.NewStatus(framework.Unschedulable,
			fmt.Sprintf("insufficient GPUs on %q: need %d, remaining %d/%d after reservations",
				nodeName, s.request, capacity-used-reserved, capacity))
	}

	if p.reservations[nodeName] == nil {
		p.reservations[nodeName] = make(map[types.UID]int64)
	}
	p.reservations[nodeName][pod.UID] = s.request

	klog.V(5).InfoS("GPUSim reserve",
		"pod", klog.KObj(pod), "node", nodeName,
		"request", s.request, "totalAfter", used+reserved+s.request)
	return nil
}

// Unreserve releases a previously claimed capacity reservation. Called by the
// framework when a pod's scheduling cycle fails after Reserve.
//
// Unreserve is intentionally idempotent: if the pod was never reserved (e.g.,
// Reserve was skipped because the pod does not request GPUs), deleting from
// the map is a safe no-op.
func (p *GPUSim) Unreserve(ctx context.Context, state *framework.CycleState, pod *v1.Pod, nodeName string) {
	s := readPreFilterState(state)
	if !s.wants {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	delete(p.reservations[nodeName], pod.UID)
	if len(p.reservations[nodeName]) == 0 {
		delete(p.reservations, nodeName)
	}

	klog.V(5).InfoS("GPUSim unreserve",
		"pod", klog.KObj(pod), "node", nodeName,
		"request", s.request)
}

// ---------------------------------------------------------------------------
// Reservation helpers
// ---------------------------------------------------------------------------

// pendingReservations returns the sum of GPU reservations on nodeName for pods
// that are NOT yet reflected in nodeInfo.Pods. This avoids double-counting a
// reservation once the pod has been bound and appears in the snapshot.
//
// The reconciliation works by building a set of pod UIDs from NodeInfo and
// subtracting any reservation whose UID already appears there. Without this,
// a reserved-then-bound pod would be counted twice: once in allocatedGPUSim
// (which reads from NodeInfo.Pods) and once in reservations[nodeName].
func (p *GPUSim) pendingReservations(nodeName string, nodeInfo *framework.NodeInfo) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	podMap := make(map[types.UID]struct{}, len(nodeInfo.Pods))
	for _, pi := range nodeInfo.Pods {
		podMap[pi.Pod.UID] = struct{}{}
	}

	var total int64
	for uid, req := range p.reservations[nodeName] {
		if _, exists := podMap[uid]; !exists {
			total += req
		}
	}
	return total
}

// sumReservationsLocked returns the total GPU reservations for a node from the
// in-flight map. Must be called with p.mu held.
func (p *GPUSim) sumReservationsLocked(nodeName string) int64 {
	var total int64
	for _, req := range p.reservations[nodeName] {
		total += req
	}
	return total
}
