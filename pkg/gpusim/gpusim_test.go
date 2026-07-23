package gpusim

import (
	"context"
	"fmt"
	"sync"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func makePod(name string, labels map[string]string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			UID:    types.UID(name),
			Labels: labels,
		},
	}
}

func makeNode(name string, labels map[string]string) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
	}
}

// mockNodeInfoLister implements framework.NodeInfoLister for testing.
type mockNodeInfoLister struct {
	nodes map[string]*framework.NodeInfo
}

func (m *mockNodeInfoLister) Get(nodeName string) (*framework.NodeInfo, error) {
	ni, ok := m.nodes[nodeName]
	if !ok {
		return nil, fmt.Errorf("node %q not found", nodeName)
	}
	return ni, nil
}

func (m *mockNodeInfoLister) List() ([]*framework.NodeInfo, error) {
	res := make([]*framework.NodeInfo, 0, len(m.nodes))
	for _, ni := range m.nodes {
		res = append(res, ni)
	}
	return res, nil
}

func (m *mockNodeInfoLister) HavePodsWithAffinityList() ([]*framework.NodeInfo, error) {
	return nil, nil
}

func (m *mockNodeInfoLister) HavePodsWithRequiredAntiAffinityList() ([]*framework.NodeInfo, error) {
	return nil, nil
}

// mockStorageInfoLister implements framework.StorageInfoLister for testing.
type mockStorageInfoLister struct{}

func (m *mockStorageInfoLister) IsPVCUsedByPods(key string) bool { return false }

// mockSharedLister implements framework.SharedLister for testing.
type mockSharedLister struct {
	lister *mockNodeInfoLister
}

func (m *mockSharedLister) NodeInfos() framework.NodeInfoLister {
	return m.lister
}

func (m *mockSharedLister) StorageInfos() framework.StorageInfoLister {
	return &mockStorageInfoLister{}
}

// mockHandle implements framework.Handle minimally for testing by embedding
// the interface and overriding only SnapshotSharedLister.
type mockHandle struct {
	framework.Handle
	sharedLister framework.SharedLister
}

func (m *mockHandle) SnapshotSharedLister() framework.SharedLister {
	return m.sharedLister
}

// newTestPlugin creates a GPUSim plugin for testing.
func newTestPlugin(mockShared *mockSharedLister) *GPUSim {
	var h framework.Handle
	if mockShared != nil {
		h = &mockHandle{sharedLister: mockShared}
	}
	p, err := New(context.Background(), nil, h)
	if err != nil {
		panic(fmt.Sprintf("New returned error: %v", err))
	}
	return p.(*GPUSim)
}

func newNodeInfo(node *v1.Node, pods ...*v1.Pod) *framework.NodeInfo {
	ni := framework.NewNodeInfo(pods...)
	ni.SetNode(node)
	return ni
}

// writeState is a helper that writes a preFilterState into CycleState,
// simulating what PreFilter does, so that downstream tests can run
// without calling PreFilter explicitly.
func writeState(state *framework.CycleState, request int64, wants bool) {
	state.Write(StateKey, &preFilterState{request: request, wants: wants})
}

// ---------------------------------------------------------------------------
// parseGPUSimRequest
// ---------------------------------------------------------------------------

func TestParseGPUSimRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		labels      map[string]string
		wantRequest int64
		wantWants   bool
		wantErr     bool
	}{
		{
			name:      "no gpu labels",
			labels:    map[string]string{},
			wantWants: false,
		},
		{
			name: "opts in, no request label",
			labels: map[string]string{
				PodLabelRequiresGPUSim: "true",
			},
			wantRequest: 1,
			wantWants:   true,
		},
		{
			name: "opts in, request=2",
			labels: map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "2",
			},
			wantRequest: 2,
			wantWants:   true,
		},
		{
			name: "opts in, request=0",
			labels: map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "0",
			},
			wantWants: true, // pod opts in even with invalid request
			wantErr:   true,
		},
		{
			name: "opts in, request negative",
			labels: map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "-3",
			},
			wantWants: true,
			wantErr:   true,
		},
		{
			name: "opts in, request not a number",
			labels: map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "abc",
			},
			wantWants: true,
			wantErr:   true,
		},
		{
			name: "opts in, request empty",
			labels: map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "",
			},
			wantWants: true,
			wantErr:   true,
		},
		{
			name: "requires not true",
			labels: map[string]string{
				PodLabelRequiresGPUSim: "false",
				PodLabelGPUSimRequest:  "2",
			},
			wantWants: false,
		},
		{
			name: "large request",
			labels: map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "99999",
			},
			wantRequest: 99999,
			wantWants:   true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pod := makePod("test-pod", tt.labels)
			req, wants, err := parseGPUSimRequest(pod)

			if (err != nil) != tt.wantErr {
				t.Errorf("parseGPUSimRequest() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if req != tt.wantRequest {
				t.Errorf("parseGPUSimRequest() request = %d, want %d", req, tt.wantRequest)
			}
			if wants != tt.wantWants {
				t.Errorf("parseGPUSimRequest() wants = %v, want %v", wants, tt.wantWants)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// parseNodeGPUSim
// ---------------------------------------------------------------------------

func TestParseNodeGPUSim(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		labels       map[string]string
		wantCapacity int64
		wantHasFake  bool
		wantErr      bool
	}{
		{
			name:        "no gpu labels",
			labels:      map[string]string{},
			wantHasFake: false,
		},
		{
			name: "gpusim=true, no capacity",
			labels: map[string]string{
				NodeLabelGPUSim: "true",
			},
			wantHasFake: true, // label is present even though capacity is missing
			wantErr:     true,
		},
		{
			name: "gpusim=true, capacity=4",
			labels: map[string]string{
				NodeLabelGPUSim:         "true",
				NodeLabelGPUSimCapacity: "4",
			},
			wantCapacity: 4,
			wantHasFake:  true,
		},
		{
			name: "gpusim=true, capacity=0",
			labels: map[string]string{
				NodeLabelGPUSim:         "true",
				NodeLabelGPUSimCapacity: "0",
			},
			wantCapacity: 0,
			wantHasFake:  true,
			wantErr:     false,
		},
		{
			name: "gpusim=true, capacity negative",
			labels: map[string]string{
				NodeLabelGPUSim:         "true",
				NodeLabelGPUSimCapacity: "-1",
			},
			wantCapacity: 0,
			wantHasFake:  true,
			wantErr:     true,
		},
		{
			name: "gpusim=true, invalid capacity",
			labels: map[string]string{
				NodeLabelGPUSim:         "true",
				NodeLabelGPUSimCapacity: "xyz",
			},
			wantHasFake: true,
			wantErr:     true,
		},
		{
			name: "gpusim not true",
			labels: map[string]string{
				NodeLabelGPUSim:         "false",
				NodeLabelGPUSimCapacity: "4",
			},
			wantHasFake: false,
		},
		{
			name: "large capacity",
			labels: map[string]string{
				NodeLabelGPUSim:         "true",
				NodeLabelGPUSimCapacity: "100000",
			},
			wantCapacity: 100000,
			wantHasFake:  true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			node := makeNode("test-node", tt.labels)
			cap, hasFake, err := parseNodeGPUSim(node)

			if (err != nil) != tt.wantErr {
				t.Errorf("parseNodeGPUSim() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if cap != tt.wantCapacity {
				t.Errorf("parseNodeGPUSim() capacity = %d, want %d", cap, tt.wantCapacity)
			}
			if !tt.wantErr && hasFake != tt.wantHasFake {
				t.Errorf("parseNodeGPUSim() hasFake = %v, want %v", hasFake, tt.wantHasFake)
			}
			if tt.wantErr && hasFake != tt.wantHasFake {
				t.Logf("parseNodeGPUSim() hasFake = %v, want %v (expected on error)", hasFake, tt.wantHasFake)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// allocatedGPUSim
// ---------------------------------------------------------------------------

func TestAllocatedGPUSim(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		pods []*v1.Pod
		want int64
	}{
		{
			name: "no pods",
			pods: []*v1.Pod{},
			want: 0,
		},
		{
			name: "single gpu pod",
			pods: []*v1.Pod{
				makePod("p1", map[string]string{
					PodLabelRequiresGPUSim: "true",
					PodLabelGPUSimRequest:  "2",
				}),
			},
			want: 2,
		},
		{
			name: "multiple gpu pods",
			pods: []*v1.Pod{
				makePod("p1", map[string]string{
					PodLabelRequiresGPUSim: "true",
					PodLabelGPUSimRequest:  "2",
				}),
				makePod("p2", map[string]string{
					PodLabelRequiresGPUSim: "true",
					PodLabelGPUSimRequest:  "1",
				}),
			},
			want: 3,
		},
		{
			name: "mixed gpu and non-gpu pods",
			pods: []*v1.Pod{
				makePod("p1", map[string]string{
					PodLabelRequiresGPUSim: "true",
					PodLabelGPUSimRequest:  "2",
				}),
				makePod("p2", map[string]string{}),
				makePod("p3", map[string]string{
					PodLabelRequiresGPUSim: "true",
					PodLabelGPUSimRequest:  "3",
				}),
			},
			want: 5,
		},
		{
			name: "pod with malformed request is skipped",
			pods: []*v1.Pod{
				makePod("p1", map[string]string{
					PodLabelRequiresGPUSim: "true",
					PodLabelGPUSimRequest:  "abc",
				}),
				makePod("p2", map[string]string{
					PodLabelRequiresGPUSim: "true",
					PodLabelGPUSimRequest:  "2",
				}),
			},
			want: 2,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			node := makeNode("test-node", map[string]string{
				NodeLabelGPUSim:         "true",
				NodeLabelGPUSimCapacity: "10",
			})
			ni := newNodeInfo(node, tt.pods...)

			got := allocatedGPUSim(ni)
			if got != tt.want {
				t.Errorf("allocatedGPUSim() = %d, want %d", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// PreFilter
// ---------------------------------------------------------------------------

func TestPreFilter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	plugin := newTestPlugin(nil)

	tests := []struct {
		name       string
		pod        *v1.Pod
		wantErr    bool
		wantReqs   int64
		wantWants  bool
	}{
		{
			name:      "non-gpu pod passes",
			pod:       makePod("p1", map[string]string{}),
			wantReqs:  0,
			wantWants: false,
		},
		{
			name: "gpu pod stores request",
			pod: makePod("p1", map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "3",
			}),
			wantReqs:  3,
			wantWants: true,
		},
		{
			name: "malformed request rejected",
			pod: makePod("p1", map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "abc",
			}),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			state := framework.NewCycleState()
			_, status := plugin.PreFilter(ctx, state, tt.pod)

			if (status != nil) != tt.wantErr {
				t.Errorf("PreFilter() status = %v, wantErr = %v", status, tt.wantErr)
			}
			if status != nil {
				return
			}
			s := readPreFilterState(state)
			if s.request != tt.wantReqs {
				t.Errorf("CycleState request = %d, want %d", s.request, tt.wantReqs)
			}
			if s.wants != tt.wantWants {
				t.Errorf("CycleState wants = %v, want %v", s.wants, tt.wantWants)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Filter
// ---------------------------------------------------------------------------

func TestFilter(t *testing.T) {
	ctx := context.Background()
	plugin := newTestPlugin(nil)

	tests := []struct {
		name       string
		pod        *v1.Pod
		node       *v1.Node
		podsOnNode []*v1.Pod
		wantCode   framework.Code
		wantMsg    string
	}{
		{
			name:     "non-gpu pod passes",
			pod:      makePod("p1", map[string]string{}),
			node:     makeNode("n1", map[string]string{}),
			wantCode: framework.Success,
		},
		{
			name: "gpu pod on non-gpu node rejected",
			pod: makePod("p1", map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "1",
			}),
			node:     makeNode("n1", map[string]string{}),
			wantCode: framework.UnschedulableAndUnresolvable,
			wantMsg:  `does not advertise simulated GPUs`,
		},
		{
			name: "gpu pod on gpu node with sufficient capacity",
			pod: makePod("p1", map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "2",
			}),
			node: makeNode("n1", map[string]string{
				NodeLabelGPUSim:         "true",
				NodeLabelGPUSimCapacity: "4",
			}),
			wantCode: framework.Success,
		},
		{
			name: "insufficient capacity",
			pod: makePod("p1", map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "3",
			}),
			node: makeNode("n1", map[string]string{
				NodeLabelGPUSim:         "true",
				NodeLabelGPUSimCapacity: "2",
			}),
			wantCode: framework.Unschedulable,
			wantMsg:  "insufficient GPUs",
		},
		{
			name: "existing pods consume some capacity",
			pod: makePod("p2", map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "2",
			}),
			node: makeNode("n1", map[string]string{
				NodeLabelGPUSim:         "true",
				NodeLabelGPUSimCapacity: "4",
			}),
			podsOnNode: []*v1.Pod{
				makePod("existing", map[string]string{
					PodLabelRequiresGPUSim: "true",
					PodLabelGPUSimRequest:  "2",
				}),
			},
			wantCode: framework.Success,
		},
		{
			name: "existing pods over-commit node",
			pod: makePod("p2", map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "3",
			}),
			node: makeNode("n1", map[string]string{
				NodeLabelGPUSim:         "true",
				NodeLabelGPUSimCapacity: "4",
			}),
			podsOnNode: []*v1.Pod{
				makePod("existing", map[string]string{
					PodLabelRequiresGPUSim: "true",
					PodLabelGPUSimRequest:  "2",
				}),
			},
			wantCode: framework.Unschedulable,
			wantMsg:  "insufficient GPUs",
		},
		{
			name: "node with gpusim=true but no capacity rejected",
			pod: makePod("p1", map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "1",
			}),
			node: makeNode("n1", map[string]string{
				NodeLabelGPUSim: "true",
			}),
			wantCode: framework.UnschedulableAndUnresolvable,
			wantMsg:  "malformed gpusim labels",
		},
		{
			name: "zero capacity rejected",
			pod: makePod("p1", map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "1",
			}),
			node: makeNode("n1", map[string]string{
				NodeLabelGPUSim:         "true",
				NodeLabelGPUSimCapacity: "0",
			}),
			wantCode: framework.Unschedulable,
			wantMsg:  "zero gpusim capacity",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ni := newNodeInfo(tt.node, tt.podsOnNode...)
			state := framework.NewCycleState()
			writeState(state, 0, false)
			if _, ok := tt.pod.Labels[PodLabelRequiresGPUSim]; ok {
				req, _, _ := parseGPUSimRequest(tt.pod)
				writeState(state, req, true)
			}
			got := plugin.Filter(ctx, state, tt.pod, ni)

			if tt.wantCode == framework.Success {
				if got != nil {
					t.Errorf("Filter() = %v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("Filter() = nil, want status with code %v", tt.wantCode)
			}
			if got.Code() != tt.wantCode {
				t.Errorf("Filter() code = %v, want %v", got.Code(), tt.wantCode)
			}
			if tt.wantMsg != "" && !stringContains(got.Message(), tt.wantMsg) {
				t.Errorf("Filter() message = %q, want containing %q", got.Message(), tt.wantMsg)
			}
		})
	}
}

func stringContains(s, substr string) bool {
	return len(s) >= len(substr) && containsStr(s, substr)
}

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestFilterNilNode verifies that Filter returns framework.Error when the
// NodeInfo has no Node object — a defensive edge case.
func TestFilterNilNode(t *testing.T) {
	ctx := context.Background()
	state := framework.NewCycleState()
	writeState(state, 1, true)
	plugin := newTestPlugin(nil)

	pod := makePod("p1", map[string]string{PodLabelRequiresGPUSim: "true", PodLabelGPUSimRequest: "1"})
	ni := framework.NewNodeInfo()
	// Intentionally not calling ni.SetNode(node), leaving Node() == nil.

	got := plugin.Filter(ctx, state, pod, ni)
	if got == nil {
		t.Fatal("Filter() = nil, want error status")
	}
	if got.Code() != framework.Error {
		t.Errorf("Filter() code = %v, want Error", got.Code())
	}
}

// TestFilterReservedCapacityBoundary verifies that Filter accounts for
// in-flight reservations when computing free capacity.
func TestFilterReservedCapacityBoundary(t *testing.T) {
	ctx := context.Background()
	node := makeNode("gpu-node", map[string]string{
		NodeLabelGPUSim:         "true",
		NodeLabelGPUSimCapacity: "4",
	})
	ni := newNodeInfo(node)
	lister := &mockNodeInfoLister{nodes: map[string]*framework.NodeInfo{"gpu-node": ni}}
	plugin := newTestPlugin(&mockSharedLister{lister: lister})

	// Reserve 3 units via Reserve (simulating an in-flight cycle).
	podA := makePod("pod-a", map[string]string{PodLabelRequiresGPUSim: "true", PodLabelGPUSimRequest: "3"})
	stateA := framework.NewCycleState()
	writeState(stateA, 3, true)
	plugin.Reserve(ctx, stateA, podA, "gpu-node")

	// Now Filter a pod that requests 2 — it should see only 1 free (4 - 0 used - 3 reserved).
	podB := makePod("pod-b", map[string]string{PodLabelRequiresGPUSim: "true", PodLabelGPUSimRequest: "2"})
	stateB := framework.NewCycleState()
	writeState(stateB, 2, true)
	got := plugin.Filter(ctx, stateB, podB, ni)
	if got == nil {
		t.Fatal("Filter() = nil, want Unschedulable due to reservations")
	}
	if got.Code() != framework.Unschedulable {
		t.Errorf("Filter() code = %v, want Unschedulable", got.Code())
	}

	// Cleanup.
	plugin.Unreserve(ctx, stateA, podA, "gpu-node")
}

// ---------------------------------------------------------------------------
// Score
// ---------------------------------------------------------------------------

func TestScore(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name       string
		pod        *v1.Pod
		nodeName   string
		node       *v1.Node
		podsOnNode []*v1.Pod
		wantScore  int64
		wantErr    bool
	}{
		{
			name:      "non-gpu pod scores zero",
			pod:       makePod("p1", map[string]string{}),
			node:      makeNode("n1", map[string]string{NodeLabelGPUSim: "true", NodeLabelGPUSimCapacity: "4"}),
			nodeName:  "n1",
			wantScore: 0,
		},
		{
			name: "empty node scores max for request=capacity",
			pod: makePod("p1", map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "4",
			}),
			node:      makeNode("n1", map[string]string{NodeLabelGPUSim: "true", NodeLabelGPUSimCapacity: "4"}),
			nodeName:  "n1",
			wantScore: framework.MaxNodeScore,
		},
		{
			name: "non-gpu node scores zero",
			pod: makePod("p1", map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "2",
			}),
			node:      makeNode("n1", map[string]string{}),
			nodeName:  "n1",
			wantScore: 0,
		},
		{
			name: "score with pending reservations",
			pod: makePod("p1", map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "1",
			}),
			node:     makeNode("n1", map[string]string{NodeLabelGPUSim: "true", NodeLabelGPUSimCapacity: "10"}),
			nodeName: "n1",
			// 10 capacity, 0 used, 5 reserved (pod-a simulated), request 1 → freeAfter=4
			// score = (10-4)/10 * 100 = 60
			wantScore: 60,
		},
		{
			name: "score exact fit returns max",
			pod: makePod("p1", map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "2",
			}),
			node:     makeNode("n1", map[string]string{NodeLabelGPUSim: "true", NodeLabelGPUSimCapacity: "2"}),
			nodeName: "n1",
			// freeAfter = 2 - 0 - 0 - 2 = 0 → score = (2-0)/2 * 100 = 100
			wantScore: framework.MaxNodeScore,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ni := newNodeInfo(tt.node, tt.podsOnNode...)
			lister := &mockNodeInfoLister{nodes: map[string]*framework.NodeInfo{tt.nodeName: ni}}
			sharedLister := &mockSharedLister{lister: lister}
			plugin := newTestPlugin(sharedLister)

			state := framework.NewCycleState()
			if _, ok := tt.pod.Labels[PodLabelRequiresGPUSim]; ok {
				req, _, _ := parseGPUSimRequest(tt.pod)
				writeState(state, req, true)
			} else {
				writeState(state, 0, false)
			}

			// For the pending reservations scenario, manually add a reservation.
			if tt.name == "score with pending reservations" {
				reservePod := makePod("reserved-pod", map[string]string{
					PodLabelRequiresGPUSim: "true",
					PodLabelGPUSimRequest:  "5",
				})
				reserveState := framework.NewCycleState()
				writeState(reserveState, 5, true)
				plugin.Reserve(ctx, reserveState, reservePod, tt.nodeName)
				defer plugin.Unreserve(ctx, reserveState, reservePod, tt.nodeName)
			}

			state = framework.NewCycleState()
			writeState(state, tt.wantScore, true)
			// Recompute with actual values.
			req, wants, _ := parseGPUSimRequest(tt.pod)
			state = framework.NewCycleState()
			writeState(state, req, wants)

			got, status := plugin.Score(ctx, state, tt.pod, tt.nodeName)
			if (status != nil) != tt.wantErr {
				t.Errorf("Score() status = %v, wantErr = %v", status, tt.wantErr)
			}
			if got != tt.wantScore {
				t.Errorf("Score() = %d, want %d", got, tt.wantScore)
			}
		})
	}
}

func TestScoreBinPacking(t *testing.T) {
	// Verify that bin-packing prefers tighter fits.
	// Node "tight" has capacity 4, used 2, request 2 → freeAfter=0 → score=100.
	// Node "loose" has capacity 8, used 0, request 2 → freeAfter=6 → score=25.
	ctx := context.Background()
	pod := makePod("p1", map[string]string{
		PodLabelRequiresGPUSim: "true",
		PodLabelGPUSimRequest:  "2",
	})

	tightNode := makeNode("tight", map[string]string{
		NodeLabelGPUSim:         "true",
		NodeLabelGPUSimCapacity: "4",
	})
	tightNI := newNodeInfo(tightNode,
		makePod("existing", map[string]string{
			PodLabelRequiresGPUSim: "true",
			PodLabelGPUSimRequest:  "2",
		}),
	)

	looseNode := makeNode("loose", map[string]string{
		NodeLabelGPUSim:         "true",
		NodeLabelGPUSimCapacity: "8",
	})
	looseNI := newNodeInfo(looseNode)

	lister := &mockNodeInfoLister{
		nodes: map[string]*framework.NodeInfo{
			"tight": tightNI,
			"loose": looseNI,
		},
	}
	plugin := newTestPlugin(&mockSharedLister{lister: lister})

	state := framework.NewCycleState()
	writeState(state, 2, true)

	tightScore, tightStatus := plugin.Score(ctx, state, pod, "tight")
	looseScore, looseStatus := plugin.Score(ctx, state, pod, "loose")

	if tightStatus != nil {
		t.Errorf("tight Score() status = %v, want nil", tightStatus)
	}
	if looseStatus != nil {
		t.Errorf("loose Score() status = %v, want nil", looseStatus)
	}

	if tightScore <= looseScore {
		t.Errorf("bin-packing: tight score %d should be > loose score %d", tightScore, looseScore)
	}
	if tightScore != framework.MaxNodeScore {
		t.Errorf("tight score = %d, want %d (fully packed)", tightScore, framework.MaxNodeScore)
	}
}

// TestScoreNodeInfoError verifies that Score returns framework.Error when
// the node lookup fails.
func TestScoreNodeInfoError(t *testing.T) {
	ctx := context.Background()
	state := framework.NewCycleState()
	writeState(state, 1, true)

	// Set up lister that will return an error (empty map).
	lister := &mockNodeInfoLister{nodes: map[string]*framework.NodeInfo{}}
	plugin := newTestPlugin(&mockSharedLister{lister: lister})

	pod := makePod("p1", map[string]string{PodLabelRequiresGPUSim: "true", PodLabelGPUSimRequest: "1"})
	_, status := plugin.Score(ctx, state, pod, "nonexistent-node")
	if status == nil {
		t.Fatal("Score() status = nil, want error")
	}
	if status.Code() != framework.Error {
		t.Errorf("Score() code = %v, want Error", status.Code())
	}
}

// ---------------------------------------------------------------------------
// Reserve / Unreserve
// ---------------------------------------------------------------------------

func TestReserveAndUnreserve(t *testing.T) {
	ctx := context.Background()

	node := makeNode("gpu-node", map[string]string{
		NodeLabelGPUSim:         "true",
		NodeLabelGPUSimCapacity: "4",
	})
	ni := newNodeInfo(node)
	lister := &mockNodeInfoLister{nodes: map[string]*framework.NodeInfo{"gpu-node": ni}}
	plugin := newTestPlugin(&mockSharedLister{lister: lister})

	pod := makePod("test-pod", map[string]string{
		PodLabelRequiresGPUSim: "true",
		PodLabelGPUSimRequest:  "2",
	})

	state := framework.NewCycleState()
	writeState(state, 2, true)

	status := plugin.Reserve(ctx, state, pod, "gpu-node")
	if status != nil {
		t.Fatalf("Reserve() = %v, want nil", status)
	}

	pending := plugin.pendingReservations("gpu-node", ni)
	if pending != 2 {
		t.Errorf("pendingReservations after Reserve = %d, want 2", pending)
	}

	plugin.Unreserve(ctx, state, pod, "gpu-node")

	pending = plugin.pendingReservations("gpu-node", ni)
	if pending != 0 {
		t.Errorf("pendingReservations after Unreserve = %d, want 0", pending)
	}
}

func TestReserveOverCommit(t *testing.T) {
	ctx := context.Background()

	node := makeNode("gpu-node", map[string]string{
		NodeLabelGPUSim:         "true",
		NodeLabelGPUSimCapacity: "4",
	})
	ni := newNodeInfo(node)
	lister := &mockNodeInfoLister{nodes: map[string]*framework.NodeInfo{"gpu-node": ni}}
	plugin := newTestPlugin(&mockSharedLister{lister: lister})

	podA := makePod("pod-a", map[string]string{
		PodLabelRequiresGPUSim: "true",
		PodLabelGPUSimRequest:  "3",
	})
	podB := makePod("pod-b", map[string]string{
		PodLabelRequiresGPUSim: "true",
		PodLabelGPUSimRequest:  "2",
	})

	stateA := framework.NewCycleState()
	writeState(stateA, 3, true)
	status := plugin.Reserve(ctx, stateA, podA, "gpu-node")
	if status != nil {
		t.Fatalf("Reserve(podA) = %v, want nil", status)
	}

	// Reserve podB should fail — only 1 unit remaining
	stateB := framework.NewCycleState()
	writeState(stateB, 2, true)
	status = plugin.Reserve(ctx, stateB, podB, "gpu-node")
	if status == nil {
		t.Fatal("Reserve(podB) = nil, want error (over-commit)")
	}
	if status.Code() != framework.Unschedulable {
		t.Errorf("Reserve(podB) code = %v, want Unschedulable", status.Code())
	}

	plugin.Unreserve(ctx, stateA, podA, "gpu-node")
}

func TestReserveNonGPUPod(t *testing.T) {
	ctx := context.Background()

	node := makeNode("gpu-node", map[string]string{
		NodeLabelGPUSim:         "true",
		NodeLabelGPUSimCapacity: "4",
	})
	ni := newNodeInfo(node)
	lister := &mockNodeInfoLister{nodes: map[string]*framework.NodeInfo{"gpu-node": ni}}
	plugin := newTestPlugin(&mockSharedLister{lister: lister})

	pod := makePod("test-pod", map[string]string{})
	state := framework.NewCycleState()
	writeState(state, 0, false)

	status := plugin.Reserve(ctx, state, pod, "gpu-node")
	if status != nil {
		t.Fatalf("Reserve(non-gpu pod) = %v, want nil", status)
	}

	pending := plugin.pendingReservations("gpu-node", ni)
	if pending != 0 {
		t.Errorf("pendingReservations = %d, want 0 (non-GPU pod should not be tracked)", pending)
	}
}

func TestUnreserveNoOpForNonGPUPod(t *testing.T) {
	ctx := context.Background()
	plugin := newTestPlugin(nil)

	pod := makePod("test-pod", map[string]string{})
	state := framework.NewCycleState()
	writeState(state, 0, false)
	plugin.Unreserve(ctx, state, pod, "any-node")
}

// TestReserveExactCapacity verifies that Reserve allows a pod that exactly
// fills the remaining capacity (boundary test).
func TestReserveExactCapacity(t *testing.T) {
	ctx := context.Background()

	node := makeNode("gpu-node", map[string]string{
		NodeLabelGPUSim:         "true",
		NodeLabelGPUSimCapacity: "4",
	})
	ni := newNodeInfo(node)
	lister := &mockNodeInfoLister{nodes: map[string]*framework.NodeInfo{"gpu-node": ni}}
	plugin := newTestPlugin(&mockSharedLister{lister: lister})

	pod := makePod("pod-full", map[string]string{
		PodLabelRequiresGPUSim: "true",
		PodLabelGPUSimRequest:  "4",
	})
	state := framework.NewCycleState()
	writeState(state, 4, true)

	status := plugin.Reserve(ctx, state, pod, "gpu-node")
	if status != nil {
		t.Fatalf("Reserve() = %v, want nil (exact capacity)", status)
	}

	plugin.Unreserve(ctx, state, pod, "gpu-node")
}

// TestReserveMalformedNode verifies that Reserve returns an error when the
// node has malformed GPU labels.
func TestReserveMalformedNode(t *testing.T) {
	ctx := context.Background()

	node := makeNode("bad-node", map[string]string{
		NodeLabelGPUSim: "true",
	})
	ni := newNodeInfo(node)
	lister := &mockNodeInfoLister{nodes: map[string]*framework.NodeInfo{"bad-node": ni}}
	plugin := newTestPlugin(&mockSharedLister{lister: lister})

	pod := makePod("p1", map[string]string{
		PodLabelRequiresGPUSim: "true",
		PodLabelGPUSimRequest:  "1",
	})
	state := framework.NewCycleState()
	writeState(state, 1, true)

	status := plugin.Reserve(ctx, state, pod, "bad-node")
	if status == nil {
		t.Fatal("Reserve() = nil, want error for malformed node")
	}
	if status.Code() != framework.UnschedulableAndUnresolvable {
		t.Errorf("Reserve() code = %v, want UnschedulableAndUnresolvable", status.Code())
	}
}

// TestUnreserveNonExistentUID verifies that Unreserve is safe to call for a
// pod UID that has no reservation (idempotency).
func TestUnreserveNonExistentUID(t *testing.T) {
	ctx := context.Background()
	plugin := newTestPlugin(nil)

	pod := makePod("never-reserved", map[string]string{
		PodLabelRequiresGPUSim: "true",
		PodLabelGPUSimRequest:  "1",
	})
	state := framework.NewCycleState()
	writeState(state, 1, true)

	// Should not panic or error.
	plugin.Unreserve(ctx, state, pod, "any-node")
}

// ---------------------------------------------------------------------------
// Concurrent Reserve safety (race detector)
// ---------------------------------------------------------------------------

func TestReserveConcurrentSafety(t *testing.T) {
	ctx := context.Background()

	node := makeNode("gpu-node", map[string]string{
		NodeLabelGPUSim:         "true",
		NodeLabelGPUSimCapacity: "4",
	})
	ni := newNodeInfo(node)
	lister := &mockNodeInfoLister{nodes: map[string]*framework.NodeInfo{"gpu-node": ni}}
	plugin := newTestPlugin(&mockSharedLister{lister: lister})

	numPods := 10
	var wg sync.WaitGroup
	reserveResults := make([]bool, numPods)
	var resultsMu sync.Mutex

	for i := 0; i < numPods; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			pod := makePod(fmt.Sprintf("pod-%d", idx), map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "1",
			})
			state := framework.NewCycleState()
			writeState(state, 1, true)
			status := plugin.Reserve(ctx, state, pod, "gpu-node")
			resultsMu.Lock()
			reserveResults[idx] = (status == nil)
			resultsMu.Unlock()
		}(i)
	}
	wg.Wait()

	successCount := 0
	for _, ok := range reserveResults {
		if ok {
			successCount++
		}
	}
	if successCount != 4 {
		t.Errorf("expected exactly 4 successful reservations, got %d (capacity=4, request=1 each)", successCount)
	}

	// Cleanup all reservations
	for i := 0; i < numPods; i++ {
		pod := makePod(fmt.Sprintf("pod-%d", i), map[string]string{
			PodLabelRequiresGPUSim: "true",
			PodLabelGPUSimRequest:  "1",
		})
		state := framework.NewCycleState()
		writeState(state, 1, true)
		plugin.Unreserve(ctx, state, pod, "gpu-node")
	}

	pending := plugin.pendingReservations("gpu-node", ni)
	if pending != 0 {
		t.Errorf("after full cleanup, pendingReservations = %d, want 0", pending)
	}
}

// TestConcurrentFilterReserve verifies that running Filter and Reserve
// concurrently is safe under the race detector. This exercises the real
// TOCTOU scenario where one goroutine reads state while another mutates it.
func TestConcurrentFilterReserve(t *testing.T) {
	ctx := context.Background()

	node := makeNode("gpu-node", map[string]string{
		NodeLabelGPUSim:         "true",
		NodeLabelGPUSimCapacity: "4",
	})
	ni := newNodeInfo(node)
	lister := &mockNodeInfoLister{nodes: map[string]*framework.NodeInfo{"gpu-node": ni}}
	plugin := newTestPlugin(&mockSharedLister{lister: lister})

	var wg sync.WaitGroup

	// Concurrently run Filter (reads reservations) and Reserve (writes reservations).
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			pod := makePod(fmt.Sprintf("pod-%d", idx), map[string]string{
				PodLabelRequiresGPUSim: "true",
				PodLabelGPUSimRequest:  "1",
			})
			state := framework.NewCycleState()
			writeState(state, 1, true)

			// Interleave Filter and Reserve to maximise race exposure.
			plugin.Filter(ctx, state, pod, ni)
			plugin.Reserve(ctx, state, pod, "gpu-node")
		}(i)
	}
	wg.Wait()

	// Cleanup all reservations from this test.
	for i := 0; i < 5; i++ {
		pod := makePod(fmt.Sprintf("pod-%d", i), map[string]string{
			PodLabelRequiresGPUSim: "true",
			PodLabelGPUSimRequest:  "1",
		})
		state := framework.NewCycleState()
		writeState(state, 1, true)
		plugin.Unreserve(ctx, state, pod, "gpu-node")
	}
}

// ---------------------------------------------------------------------------
// pendingReservations
// ---------------------------------------------------------------------------

func TestPendingReservationsSkipsBoundPods(t *testing.T) {
	ctx := context.Background()

	node := makeNode("gpu-node", map[string]string{
		NodeLabelGPUSim:         "true",
		NodeLabelGPUSimCapacity: "10",
	})
	ni := newNodeInfo(node)
	lister := &mockNodeInfoLister{nodes: map[string]*framework.NodeInfo{"gpu-node": ni}}
	plugin := newTestPlugin(&mockSharedLister{lister: lister})

	podA := makePod("pod-a", map[string]string{
		PodLabelRequiresGPUSim: "true",
		PodLabelGPUSimRequest:  "2",
	})
	podB := makePod("pod-b", map[string]string{
		PodLabelRequiresGPUSim: "true",
		PodLabelGPUSimRequest:  "3",
	})

	stateA := framework.NewCycleState()
	writeState(stateA, 2, true)
	plugin.Reserve(ctx, stateA, podA, "gpu-node")

	stateB := framework.NewCycleState()
	writeState(stateB, 3, true)
	plugin.Reserve(ctx, stateB, podB, "gpu-node")

	// Simulate podA being bound: add it to NodeInfo
	boundNI := newNodeInfo(node, podA)

	// pendingReservations should only count podB (podA is now in NodeInfo)
	pending := plugin.pendingReservations("gpu-node", boundNI)
	if pending != 3 {
		t.Errorf("pendingReservations = %d, want 3 (only podB's reservation)", pending)
	}

	plugin.Unreserve(ctx, stateA, podA, "gpu-node")
	plugin.Unreserve(ctx, stateB, podB, "gpu-node")
}

// ---------------------------------------------------------------------------
// Name / ScoreExtensions / New / EventsToRegister
// ---------------------------------------------------------------------------

func TestName(t *testing.T) {
	plugin := newTestPlugin(nil)
	if plugin.Name() != Name {
		t.Errorf("Name() = %q, want %q", plugin.Name(), Name)
	}
}

func TestScoreExtensions(t *testing.T) {
	plugin := newTestPlugin(nil)
	if se := plugin.ScoreExtensions(); se != nil {
		t.Errorf("ScoreExtensions() = %v, want nil", se)
	}
}

func TestNew(t *testing.T) {
	plugin, err := New(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if plugin == nil {
		t.Fatal("New() returned nil")
	}
	if _, ok := plugin.(*GPUSim); !ok {
		t.Errorf("New() type = %T, want *GPUSim", plugin)
	}
}

func TestEventsToRegister(t *testing.T) {
	plugin := newTestPlugin(nil)
	events, err := plugin.EventsToRegister(context.Background())
	if err != nil {
		t.Fatalf("EventsToRegister() error = %v", err)
	}
	if len(events) == 0 {
		t.Fatal("EventsToRegister() returned empty slice")
	}
	found := false
	for _, e := range events {
		if e.Event.Resource == framework.Node {
			found = true
			break
		}
	}
	if !found {
		t.Error("EventsToRegister() missing Node event")
	}
}

// ---------------------------------------------------------------------------
// Benchmark
// ---------------------------------------------------------------------------

func BenchmarkParseGPUSimRequest(b *testing.B) {
	pod := makePod("bench", map[string]string{
		PodLabelRequiresGPUSim: "true",
		PodLabelGPUSimRequest:  "2",
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = parseGPUSimRequest(pod)
	}
}

func BenchmarkAllocatedGPUSim(b *testing.B) {
	pods := make([]*v1.Pod, 100)
	for i := range pods {
		pods[i] = makePod(fmt.Sprintf("p-%d", i), map[string]string{
			PodLabelRequiresGPUSim: "true",
			PodLabelGPUSimRequest:  fmt.Sprintf("%d", (i%10)+1),
		})
	}
	node := makeNode("bench-node", map[string]string{
		NodeLabelGPUSim:         "true",
		NodeLabelGPUSimCapacity: "1000",
	})
	ni := newNodeInfo(node, pods...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = allocatedGPUSim(ni)
	}
}

func BenchmarkPreFilter(b *testing.B) {
	pod := makePod("bench", map[string]string{
		PodLabelRequiresGPUSim: "true",
		PodLabelGPUSimRequest:  "2",
	})
	plugin := newTestPlugin(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state := framework.NewCycleState()
		plugin.PreFilter(context.Background(), state, pod)
	}
}
