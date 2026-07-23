# GPUSim Scheduler

A hands-on walkthrough of the Kubernetes Scheduler Framework, one extension point at a time.

This project builds a custom scheduler that schedules pods onto nodes with "GPUSim" capacity — simulated GPU resources expressed entirely through Kubernetes labels, no hardware required. Each plugin in the project deliberately reaches for a different part of the scheduler framework so the project grows from "can I write a plugin?" into a small collection that demonstrates how the framework actually composes.

---

## Table of Contents

- [Why This Exists](#why-this-exists)
- [The Kubernetes Scheduler Framework](#the-kubernetes-scheduler-framework)
  - [The Scheduling Cycle](#the-scheduling-cycle)
  - [Extension Points](#extension-points)
  - [How Plugins Compose](#how-plugins-compose)
- [Project Architecture](#project-architecture)
  - [The Label Contract](#the-label-contract)
  - [Shared State & Concurrency](#shared-state--concurrency)
- [Plugin Deep Dive — GPUSim](#plugin-deep-dive--gpusim)
  - [PreFilter — One-Time Validation](#0-prefilter--one-time-validation)
  - [Filter — Admission Control](#1-filter--admission-control)
  - [Score — Bin-Packing](#2-score--bin-packing)
  - [Reserve — Atomic Capacity Claim](#3-reserve--atomic-capacity-claim)
  - [Unreserve — Rollback on Failure](#4-unreserve--rollback-on-failure)
  - [EnqueueExtensions — Re-triggering](#5-enqueueextensions--re-triggering)
  - [The Concurrency Safety Net](#the-concurrency-safety-net)
- [Local Workflow](#local-workflow)
  - [Prerequisites](#prerequisites)
  - [Setup](#setup)
  - [Deploy](#deploy)
  - [Verification](#verification)
- [Code Walkthrough](#code-walkthrough)
- [What to Try Next](#what-to-try-next)
  - [Carbon-Aware Scheduling](#carbon-aware-scheduling)
  - [Gang Scheduling](#gang-scheduling)
  - [Multi-Profile Deployments](#multi-profile-deployments)
- [Further Resources](#further-resources)

---

## Why This Exists

The Kubernetes scheduler is one of the most complex components in the control plane. Before the Scheduler Framework (introduced as alpha in v1.15, stable in v1.19), extending the scheduler meant either forking the entire `k8s.io/kubernetes` repo or writing an out-of-process "extender" that received HTTP callbacks. Neither approach was ergonomic.

The Scheduler Framework changed that by defining a plugin API with well-defined extension points. You register your plugin, the framework calls the right method at the right time.

This project is my notebook for learning that plugin API. Each plugin picks an extension point the previous one didn't touch.

---

## The Kubernetes Scheduler Framework

### The Scheduling Cycle

Every pod goes through two phases: **scheduling** and **binding**.

The scheduling cycle selects a node. The binding cycle makes it official by writing the binding to the API server. These are decoupled — a pod can finish its scheduling cycle and wait in a binding queue while other pods are scheduled.

Within the scheduling cycle, the framework walks through a sequence of extension points:

```
Pod enters scheduling queue
        │
        ▼
┌─────────────────┐
│  PreFilter       │  ◀── Validate & precompute (once per cycle)
│  Filter          │  ◀── Reject unfit nodes
│  PostFilter      │  ◀── Run if all nodes filtered out
│  PreScore        │  ◀── Precompute scoring data
│  Score           │  ◀── Rank surviving nodes
│  NormalizeScore  │  ◀── Normalize scores across plugins
│  Reserve         │  ◀── Claim resources atomically
│  Permit          │  ◀── Approve or defer (for gang scheduling)
│  PreBind         │  ◀── Prepare before binding
│  Bind            │  ◀── Write binding to API server
│  PostBind        │  ◀── Cleanup after binding
└─────────────────┘
        │
        ▼
  Pod bound to node
```

If any extension point returns an error, the framework calls **Unreserve** (if Reserve had run) and the pod goes back to the queue.

### Extension Points

| Extension Point | Interface | Called When | Called For |
|----------------|-----------|-------------|------------|
| `PreFilter` | `PreFilterPlugin` | Before any node filtering | Each pod, once |
| `Filter` | `FilterPlugin` | Evaluating each node | Each (pod, node) pair |
| `PostFilter` | `PostFilterPlugin` | All nodes filtered out | Each pod that failed |
| `PreScore` | `PreScorePlugin` | Before scoring | Each pod, once |
| `Score` | `ScorePlugin` | Scoring each node | Each (pod, node) pair |
| `NormalizeScore` | `ScoreExtensions` | After all scores computed | Each node, once per plugin |
| `Reserve` | `ReservePlugin` | Before binding | The chosen pod+node |
| `Permit` | `PermitPlugin` | After reserve, before bind | The chosen pod+node |
| `PreBind` | `PreBindPlugin` | Before writing the binding | The chosen pod+node |
| `Bind` | `BindPlugin` | Write binding to API server | The chosen pod+node |
| `PostBind` | `PostBindPlugin` | After successful bind | The chosen pod+node |
| `Unreserve` | `ReservePlugin` | On scheduling failure | The failed pod+node |
| `EnqueueExtensions` | `EnqueueExtensions` | Register re-trigger events | (At plugin registration) |

### How Plugins Compose

Multiple plugins can register for the same extension point. The framework runs them in the order specified in `KubeSchedulerConfiguration`. For Filter, all plugins must pass for the node to survive. For Score, each plugin returns a score, and the framework combines them using the configured weights.

This composability is the key design insight — your custom plugin doesn't replace the built-in plugins, it joins them. The same scheduling profile mixes `NodeResourcesFit` (built-in), `NodeAffinity` (built-in), and `GPUSim` (yours), all weighing in on the same decision.

---

## Project Architecture

```
.
├── main.go                          # Entry point, registers plugins
├── Makefile                         # build / test / lint / deploy
├── deploy/manifests/
│   ├── kube-scheduler-config.yaml   # KubeSchedulerConfiguration ConfigMap
│   ├── deployment.yaml              # Scheduler Deployment
│   └── rbac.yaml                    # ServiceAccount, ClusterRole, ClusterRoleBinding
└── pkg/gpusim/
    ├── gpusim.go                    # **Stateless**: PreFilter, Filter, Score, label parsers
    ├── reserve.go                   # **Stateful**: Reserve, Unreserve, EnqueueExtensions,
    │                                #   reservation map with sync.Mutex
    └── gpusim_test.go               # Table-driven tests (94% coverage, race-detector clean)

The plugin code is split across two files to separate concerns: `gpusim.go` holds
the **stateless** admission and scoring logic plus pure helper functions, while
`reserve.go` owns the **stateful** reservation map and its concurrency guard —
all within a single Go package.

**PreFilter** was added to avoid redundant label parsing: the pod's GPU request
is validated once per scheduling cycle and stored in `framework.CycleState`. All
downstream extension points read from CycleState instead of parsing labels again.
```

### The Label Contract

Every scheduling decision starts from four labels. This is the interface between nodes, pods, and the scheduler — no `resources.limits`, no device plugins, just labels.

**Nodes** advertise GPUs:

```yaml
metadata:
  labels:
    gpusim: "true"            # Required. This node has simulated GPUs.
    gpusim-capacity: "4"      # Required with gpusim=true. Non-negative integer.
```

**Pods** request them:

```yaml
metadata:
  labels:
    requires-gpusim: "true"   # Required. This pod wants a GPU node.
    gpusim-request: "2"       # Optional, defaults to 1. Positive integer.
spec:
  schedulerName: kube-scheduler-gpusim  # Opt into the custom scheduler
```

### Shared State & Concurrency

The scheduler framework runs scheduling cycles concurrently. Without coordination, two pods could both see the same free capacity on a node and both get scheduled there — a classic TOCTOU race.

This project solves it with a pod-UID reservation map protected by `sync.Mutex`:

```
reservations map[string]map[types.UID]int64
              │              │            │
              │              │            └── GPU count requested
              │              └── Pod UID (prevents double-counting after bind)
              └── Node name
```

The `pendingReservations()` helper is the key reconciler. When a reservation is first created, the pod's UID isn't yet in the framework's NodeInfo snapshot (binding hasn't happened yet). After binding, the pod appears in NodeInfo.Pods, and `pendingReservations` skips it — no double-counting.

```
┌─ Filter/Score ─────────────────┐
│  used = allocatedGPUSim(ni)     │  ← bound pods from NodeInfo
│       + pendingReservations(n)   │  ← in-flight cycle reservations
│  free = capacity - used          │
│  reject if free < request       │
└─────────────────────────────────┘
```

---

## Plugin Deep Dive — GPUSim

### 0. PreFilter — One-Time Validation

**Interface:** `framework.PreFilterPlugin`
**Called:** Once per scheduling cycle, before any node is evaluated.

PreFilter runs before Filter, Score, or any other extension point. It validates the pod's GPU labels **once** and stores the parsed result in `framework.CycleState`. Every downstream extension point reads from CycleState instead of calling `parseGPUSimRequest` again.

```go
func (p *GPUSim) PreFilter(ctx, state, pod) (*framework.PreFilterResult, *framework.Status) {
    request, wants, err := parseGPUSimRequest(pod)
    if err != nil {
        return UnschedulableAndUnresolvable  // malformed labels → never schedule
    }
    state.Write("gpusim.v1beta1", &preFilterState{request, wants})
    return nil, nil  // success
}
```

This avoids redundant work: without PreFilter, a pod that requests GPUs would have its labels parsed once per Filter call (potentially hundreds of nodes), once per Score call (surviving nodes), once in Reserve, and once in Unreserve. With PreFilter, parsing happens **once**.

The `readPreFilterState` helper retrieves the stored data:

```go
func readPreFilterState(state *framework.CycleState) *preFilterState {
    data, err := state.Read("gpusim.v1beta1")
    if err != nil {
        return &preFilterState{}  // non-GPU pod: wants=false
    }
    return data.(*preFilterState)
}
```

### 1. Filter — Admission Control

**Interface:** `framework.FilterPlugin`
**Called:** Once for each (pod, node) pair during the Filter phase.
**Decision:** Unschedulable (retry later) vs UnschedulableAndUnresolvable (don't retry).

The Filter plugin answers one question: *can this pod land on this node?*

```go
func (p *GPUSim) Filter(ctx, state, pod, nodeInfo) *framework.Status {
```

It reads the pre-parsed GPU request from CycleState, reads the node's labels to find capacity, sums what's already allocated plus what's reserved in-flight, and compares against capacity.

Two status codes are used:
- **UnschedulableAndUnresolvable** — the node will never work (no `gpusim` label, malformed labels). The framework won't retry this node for this pod.
- **Unschedulable** — insufficient capacity right now but capacity might free up later. The framework re-queues the pod and retries on relevant events (node label changes, pod deletions).

This distinction matters for performance. If a node simply doesn't have GPUs, there's no point retrying. If capacity is temporarily exhausted, the scheduler should try again after an event that might free capacity.

### 2. Score — Bin-Packing

**Interface:** `framework.ScorePlugin`
**Called:** Once for each node that passed Filter.
**Returns:** An int64 in `[0, framework.MaxNodeScore]` (0-100).

Score ranks the surviving nodes. The formula implements **bin-packing** — prefer the node that will be most filled after placing the pod:

```
freeAfter = capacity - used - reserved - request
score     = (capacity - freeAfter) / capacity * MaxNodeScore
```

When `freeAfter = 0` (the pod fills the node completely), `score = MaxNodeScore`. When the node is mostly empty after placement, the score is low.

This is the opposite of "spreading" — bin-packing is useful when you want to leave whole nodes free for other workloads or power-saving.

`ScoreExtensions` returns nil because `NormalizeScore` isn't needed. Each Score call already produces a value in the correct range because the formula normalizes by capacity.

### 3. Reserve — Atomic Capacity Claim

**Interface:** `framework.ReservePlugin` (the `Reserve` method)
**Called:** After Score selects a winner, before Bind.

Filter and Score operate on a read-only snapshot. Between the time Filter passes a node and Bind commits the choice, another scheduling cycle can select the same node for a different pod. Reserve closes this window:

```go
func (p *GPUSim) Reserve(ctx, state, pod, nodeName) *framework.Status {
    p.mu.Lock()
    defer p.mu.Unlock()

    // Check: has another pod reserved the last unit?
    if used + reserved + request > capacity {
        return Unschedulable  // try next node
    }
    p.reservations[nodeName][pod.UID] = request
    return nil
}
```

If Reserve returns an error, the framework doesn't bind this pod. Instead it calls Unreserve (to release any state) and tries the next-highest-scored node.

This is the correct answer to the TOCTOU race. Without it, two pods requesting 2 GPUs each on a node with capacity 4 would both pass Filter (both see `free=4`) and both get bound.

### 4. Unreserve — Rollback on Failure

**Interface:** `framework.ReservePlugin` (the `Unreserve` method)
**Called:** When the scheduling cycle fails at any point after Reserve.

Unreserve is the safety net. If Permit denies the pod, or PreBind fails, or Bind itself fails, the framework calls Unreserve to release the claim:

```go
func (p *GPUSim) Unreserve(ctx, state, pod, nodeName) {
    p.mu.Lock()
    defer p.mu.Unlock()

    delete(p.reservations[nodeName], pod.UID)
    if len(p.reservations[nodeName]) == 0 {
        delete(p.reservations, nodeName)  // free the map entry
    }
}
```

Key design property: Unreserve is **idempotent**. If the pod was never reserved (e.g., Reserve wasn't called because the pod doesn't want GPUs), deleting from the map is a no-op.

### 5. EnqueueExtensions — Re-triggering

**Interface:** `framework.EnqueueExtensions`
**Called:** At plugin registration to register events that should re-queue rejected pods.

```go
func (p *GPUSim) EventsToRegister(_ context.Context) ([]framework.ClusterEventWithHint, error) {
    return []framework.ClusterEventWithHint{
        {Event: framework.ClusterEvent{Resource: framework.Node, ActionType: framework.Add | framework.UpdateNodeLabel}},
    }, nil
}
```

When a pod is rejected with `Unschedulable` (not `UnschedulableAndUnresolvable`), it goes to the back of the queue. The framework won't retry it until an event from this list occurs. By registering `Node Add | Node UpdateLabel`, we ensure that:

- A new GPU-capable node appears → waiting pods wake up
- A node's `gpusim-capacity` changes → waiting pods re-evaluate
- A node is deleted → waiting pods don't waste cycles on a node that no longer exists

Without this, a pod rejected for insufficient capacity would sit in the queue indefinitely, even after another pod finishes and frees capacity.

### The Concurrency Safety Net

The most subtle part of the implementation is `pendingReservations`, which prevents double-counting:

```go
func (p *GPUSim) pendingReservations(nodeName string, nodeInfo *framework.NodeInfo) int64 {
    p.mu.Lock()
    defer p.mu.Unlock()

    // Build a set of pod UIDs already in the NodeInfo snapshot
    podMap := make(map[types.UID]struct{}, len(nodeInfo.Pods))
    for _, pi := range nodeInfo.Pods {
        podMap[pi.Pod.UID] = struct{}{}
    }

    // Count only reservations whose pods have NOT yet appeared in NodeInfo
    var total int64
    for uid, req := range p.reservations[nodeName] {
        if _, exists := podMap[uid]; !exists {
            total += req
        }
    }
    return total
}
```

Without this reconciliation, a reserved pod that successfully binds would be counted **twice**: once in `allocatedGPUSim(nodeInfo)` (which reads from NodeInfo.Pods) and once in `reservations[nodeName]`. The reconciliation avoids that by consulting NodeInfo.Pods and skipping UIDs that are already there.

This is a pattern worth remembering: when you have two sources of truth (a snapshot and in-flight state), you need a reconciliation step — or the invariant breaks.

---

## Local Workflow

This is a learning project. You should run it on a local kind cluster, not production.

### Prerequisites

| Tool | Version | Why |
|------|---------|-----|
| Go | 1.22+ | Project targets k8s.io v0.31.0 |
| kind | Latest | Local Kubernetes cluster |
| kubectl | Matching cluster version | Cluster interaction |
| Docker | Any | Build scheduler image |
| golangci-lint | Optional | Linting |

### Setup

```bash
# 1. Create a kind cluster
kind create cluster

# 2. Label nodes with simulated GPU capacity
kubectl label nodes <node-name> gpusim=true
kubectl label nodes <node-name> gpusim-capacity=4

# Verify labels appear
kubectl get nodes -o json | jq '.items[].metadata.labels'
```

### Build

```bash
# Build the scheduler binary
make build

# Build a Docker image and load into kind
docker build -t gpusim-scheduler:latest .
kind load docker-image gpusim-scheduler:latest

# Verify
docker images gpusim-scheduler
```

### Deploy

```bash
# Apply RBAC, ConfigMap, and Deployment
kubectl apply -f deploy/manifests/

# Watch it come up
kubectl -n kube-system get pods -l component=gpusim-scheduler -w
```

### Verify

```bash
# Check scheduler logs
kubectl -n kube-system logs deployment/gpusim-scheduler

# Run a test pod (uses the custom scheduler)
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: gpu-test
  labels:
    requires-gpusim: "true"
    gpusim-request: "2"
spec:
  schedulerName: kube-scheduler-gpusim
  containers:
  - name: ctr
    image: nginx
EOF

# Check scheduling outcome
kubectl describe pod gpu-test | grep -E "Node:|Events:"
```

The pod should land on the node you labeled earlier.

### Debugging

**Pod stuck in Pending?**
- Verify `schedulerName` matches: `kubectl get pod gpu-test -o json | jq .spec.schedulerName`
- Check scheduler logs: `kubectl -n kube-system logs deployment/gpusim-scheduler`
- Verify node labels: `kubectl get nodes --show-labels`
- Confirm no other scheduler is running with the same name

**Scheduler doesn't start?**
- Check Deployment events: `kubectl -n kube-system describe deployment gpusim-scheduler`
- Verify ConfigMap was applied: `kubectl -n kube-system get configmap gpusim-scheduler-config`
- Check RBAC bindings: `kubectl -n kube-system describe clusterrolebinding gpusim-scheduler`

### Run Tests & Lint

```bash
make test    # 94% coverage, race detector
make vet     # No issues
make lint    # golangci-lint, 0 issues
```

---

## Code Walkthrough

### Entry Point (`main.go`)

The main function creates a scheduler command using the Kubernetes scaffolding and registers the GPUSim plugin:

```go
func main() {
    command := app.NewSchedulerCommand(
        app.WithPlugin(gpusim.Name, gpusim.New),
    )
    os.Exit(cli.Run(command))
}
```

`app.WithPlugin` registers the factory function with the scheduler framework. The framework calls `gpusim.New` (once) at startup, passing the configuration and a `framework.Handle` that gives access to cluster state.

### Plugin Factory (`gpusim.New`)

```go
func New(_ context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
    return &GPUSim{
        handle:       h,
        reservations: make(map[string]map[types.UID]int64),
    }, nil
}
```

The factory receives the raw plugin configuration (`runtime.Object`) and a handle for accessing the framework's shared state. The handle is needed by `Score` and `Reserve` to call `p.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)`.

### PreFilter and CycleState

The `PreFilter` extension point validates pod labels once per scheduling cycle and stores the parsed result in `framework.CycleState`. This is a key optimization: without it, every downstream extension point (Filter, Score, Reserve, Unreserve) would parse the same labels repeatedly.

The stored state is a `preFilterState` struct:

```go
type preFilterState struct {
    request int64
    wants   bool
}

func (s *preFilterState) Clone() framework.StateData {
    return &preFilterState{request: s.request, wants: s.wants}
}
```

Downstream methods retrieve it with `readPreFilterState(state)`. If no state was stored (the pod doesn't request GPUs), it returns a zero-value with `wants=false`, causing the extension point to short-circuit.

### Label Parsers

The pure functions `parseGPUSimRequest` and `parseNodeGPUSim` extract numeric values from labels. They're pure functions with no side effects and are tested separately. Key design choices:

- A pod that sets `requires-gpusim=true` without `gpusim-request` defaults to 1.
- A pod that omits `requires-gpusim=true` entirely is treated as "not GPU-aware" and passes through Filter without being evaluated.
- A node with `gpusim=true` but missing capacity returns an error rather than silently accepting zero. Zero capacity is allowed but results in `Unschedulable` (retryable).
- Negative capacity is rejected as `UnschedulableAndUnresolvable` (malformed).

### Interface Assertions

At the top of `gpusim.go`, compile-time assertions verify the plugin satisfies the required interfaces:

```go
var (
    _ framework.PreFilterPlugin      = (*GPUSim)(nil)
    _ framework.FilterPlugin         = (*GPUSim)(nil)
    _ framework.ScorePlugin          = (*GPUSim)(nil)
    _ framework.ReservePlugin        = (*GPUSim)(nil)
    _ framework.EnqueueExtensions    = (*GPUSim)(nil)
)
```

If the struct ever drifts from the interface (e.g., a method signature changes), the compiler catches it at build time rather than at runtime.

---

## What to Try Next

The project is designed to grow. Here are directions to explore, each adding a new extension point the current plugin doesn't use.

### Carbon-Aware Scheduling

**New extension point:** `PreFilter`

Add a `PreFilter` plugin that snapshots carbon intensity from a node label once per cycle and a `Score` plugin that prefers greener nodes. PreFilter runs once per scheduling cycle (not per node), making it the right place to fetch external data like a grid API.

```
PreFilter: fetch carbon intensity map → cache in CycleState
Score:     score = intensity[stat.NodeName]  (lower is greener)
```

This demonstrates:
- Passing data between extension points via `CycleState`.
- Running a one-time computation (PreFilter) instead of repeating it per node (Filter/Score).

### Gang Scheduling

**New extension point:** `Permit`

Schedule a group of pods (a "gang") only when all members are ready. The `Permit` plugin defers scheduling by putting pods in a waiting state until the gang reaches quorum.

```
Permit: if not all gang members are present → Wait (timeout)
        once all members are present → Approve all
```

This demonstrates:
- The `WaitingPod` API — pods can be deferred, approved, or rejected by Permit.
- Framework-managed timeout — pods that time out in Permit are re-queued.
- Coordinating decisions across independent scheduling cycles.

### Multi-Profile Deployments

Define multiple scheduler profiles in `KubeSchedulerConfiguration`, each with a different `schedulerName`:

```yaml
profiles:
  - schedulerName: gpu-scheduler
    plugins: { ... }
  - schedulerName: green-scheduler
    plugins: { ... }
  - schedulerName: gang-scheduler
    plugins: { ... }
```

Workloads opt into the right profile by setting `spec.schedulerName`. This is how real multi-strategy schedulers are deployed in production.

---

## Further Resources

| Resource | Link |
|----------|------|
| Scheduler Framework design doc | [kubernetes/enhancements/keps/624-scheduling-framework](https://github.com/kubernetes/enhancements/tree/master/keps/sig-scheduling/624-scheduling-framework) |
| `kube-scheduler` source | [pkg/scheduler](https://github.com/kubernetes/kubernetes/tree/master/pkg/scheduler) |
| `scheduler-plugins` repo | [kubernetes-sigs/scheduler-plugins](https://github.com/kubernetes-sigs/scheduler-plugins) |
| Scheduling Framework walkthrough | [kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework](https://kubernetes.io/docs/concepts/scheduling-eviction/scheduling-framework/) |
| Configure multiple schedulers | [kubernetes.io/docs/tasks/extend-kubernetes/configure-multiple-schedulers](https://kubernetes.io/docs/tasks/extend-kubernetes/configure-multiple-schedulers/) |
