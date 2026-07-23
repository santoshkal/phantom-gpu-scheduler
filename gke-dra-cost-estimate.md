# GKE Cost Estimate — NFD + GPU + DRA + DRANET Hands-on

The cost driver isn't NFD/DRA/DRANET themselves — those are free software bolted on top. It's the node shape you pick. Below are three realistic tiers, assuming `us-central1`, on-demand pricing, and the cluster is actually deleted after the experiment.

## Tier 1 — Wire up the stack, no real RDMA (~$5–15)

Goal: prove NFD labels appear, a GPU device plugin works, DRA resource claims schedule.

- 1× `g2-standard-8` with L4 GPU: ~$0.70/hr
- GKE control plane: $0.10/hr (one zonal cluster is free-tier eligible)
- 6–8 hour session: **~$5–10 total**

## Tier 2 — DRA with multi-GPU, still no RDMA fabric (~$30–60)

Goal: exercise DRA ResourceClaims across multiple GPUs on one node.

- 1× `a2-highgpu-2g` (2× A100): ~$7.35/hr
- 4–6 hours: **~$30–45 total**

## Tier 3 — Actual RDMA + NIC alignment via DRANET (~$150–700)

This is where it gets expensive. DRANET-style NIC-to-GPU alignment is meaningful only on A3 shapes that expose RDMA NICs (GPUDirect-TCPX/RDMA fabric). You can't fake this on a T4.

- 1× `a3-highgpu-8g` (8× H100 + RDMA NICs): ~$88/hr on-demand, ~$30–35/hr **Spot**
- Realistic hands-on window (setup + break + retry): 6–10 hours
- On-demand: **~$500–900**
- Spot: **~$180–350**

## Costs expected to be negligible

- Persistent disks: cents per hour for a 100 GB boot disk
- Egress during the experiment: pennies if you're not pulling giant images repeatedly
- GKE cluster management fee: $0.10/hr past the free tier cluster
- NFD, device plugins, DRA, DRANET: zero software cost

## Practical cost-control moves

1. **Use Spot** for everything GPU — it's ~65–70% off and a dev experiment tolerates preemption fine.
2. **Get quota first.** H100/A3 quota is the bottleneck far more than cost on a new project; request it days ahead. You'll be told "zero" by default.
3. **Scale the nodepool to zero** (`gcloud container clusters resize ... --num-nodes=0`) the moment you step away — an idle A3 costs $88/hr doing nothing.
4. **Delete, don't stop.** GKE doesn't have a "stop" for nodepools the way Compute Engine does for VMs; leaving a GPU nodepool up burns money by the minute.
5. **Use `us-central1` or `us-east4`** — Asia/EU regions are 10–25% pricier for A3.
6. **Pick the smallest A3 variant you can**; A3 Mega and A3 Ultra are meaningfully more per hour than A3 High.

## Rough budget to give yourself

- If the goal is "learn NFD + device plugin + DRA mechanics": **budget $20**, likely spend ~$10.
- If the goal is "learn DRANET with real RDMA alignment": **budget $300 on Spot, $800 on-demand** for a weekend of work. The non-negotiable cost is the A3.

## One last thing

GKE's DRA support tracks Kubernetes minor versions closely. Check the GKE release notes for which channel (Rapid/Regular/Stable) currently ships the DRA API level DRANET needs — picking the wrong channel is a silent waste of a paid hour.
