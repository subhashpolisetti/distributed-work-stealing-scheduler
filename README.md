# Distributed Work-Stealing Scheduler

A **fault-tolerant distributed task-scheduling system** built in Python over gRPC. A peer-elected leader distributes tasks across worker nodes using an **8-factor dynamic load-balancing heuristic**; idle nodes autonomously **steal work** from busy peers. The cluster self-heals — on leader failure, surviving nodes elect a new leader in **~3 seconds**.

---

## Benchmark Results

| Metric | Result |
|---|---|
| Single-node throughput (200ms tasks) | **~1,160 tasks/sec** |
| Thread-pool bottleneck fix | **~7× improvement** (156 → 1,160 tasks/sec) |
| Leader failure recovery time | **~3 seconds** |
| Nodes tested | 1 – 5 |
| Tasks per benchmark run | 1,500 – 4,000 |

> **Throughput improvement story:** Profiled the system and found three 32-thread pools (`grpc.server`, `Servicer.task_executor`, `Node.task_executor`) all serializing dispatch at ~32 concurrent. Raising them to 256 gave a ~7× throughput gain — a direct measure of removing a concurrency bottleneck.

---

## Architecture

```
  Client
  submit_tasks.py
       │
       │ SubmitTask RPC
       ▼
  ┌─────────────────────────────────────────────┐
  │              Leader Node                    │
  │  choose_targets() → 8-factor score          │
  │  assign_task()   → route to best node       │
  └──────────┬──────────────────────────────────┘
             │ ExecuteTask RPC
    ┌────────▼────────────────────┐
    │      Worker Nodes           │
    │  exec_task() → do work      │
    │  stealing_loop()            │
    │    → StealTasks from busy   │
    └─────────────────────────────┘

  hb_loop:     leader heartbeats every 1s (carries NodeStats)
  monitor_loop: no heartbeat for 3s → start_election()
  Election:    bully by weight → highest-weight node wins
```

---

## How It Works

### 1. Leader Election (Bully Algorithm)
- Node 0 bootstraps as leader on startup.
- Each follower runs `monitor_loop` — missing a heartbeat for **3 seconds** triggers `start_election()`.
- Candidate sends `Election` RPC to all peers with its **weight**. If any higher-weight peer responds `ok=True`, the candidate aborts and that peer runs its own election.
- If no higher-weight peer responds → `become_leader()` → broadcast heartbeats to all.
- **Tie-break:** same weight → lower node ID wins.

### 2. Heartbeat as Gossip Channel
Leader heartbeats every 1s carry full `NodeStats`:
```protobuf
message NodeStats {
  int32  queue_length;
  float  cpu_usage;
  float  memory_available;
  float  avg_response_time;
  int32  tasks_completed;
  int32  tasks_failed;
  map<string, float> specialization;  // task_type → success_rate
}
```
This keeps every node's `peer_stats` map up-to-date for scheduling and stealing decisions — no separate gossip protocol needed.

### 3. Task Routing — Two Modes

**Round-Robin (default):**
```python
target = all_nodes[counter % num_nodes]
```

**Advanced Weight Scoring (`--advanced-weight`):**
```
score(node) = weight × (1 / 1 + queue_penalty)
                     × (1 + specialization_bonus)
                     × (1 / 1 + distance_penalty)
```
Where `weight = calculate_advanced_weight()` — a weighted sum of 8 live signals:

| Factor | Weight |
|---|---|
| Capacity (CPU + memory + bandwidth) | 20% |
| Reliability (task success rate) | 20% |
| History (tasks completed) | 15% |
| Response time | 15% |
| Queue length | 10% |
| Network distance | 10% |
| Time decay | 5% |
| Task-type specialization | 5% |

### 4. Work Stealing
- Idle nodes (queue < threshold) call `StealTasks` on the busiest peer.
- Victim yields up to 10 tasks while keeping a minimum reserve.
- 0.5s cooldown between steal attempts to prevent thrashing.
- Stealer weight increases; victim weight decreases — self-regulating.

### 5. Failure Recovery
```
Leader killed
  → followers miss heartbeat for 3s
  → monitor_loop triggers start_election()
  → highest-weight survivor wins
  → new leader broadcasts heartbeats
  → cluster resumes task submission
  Total: ~3 seconds measured
```

---

## gRPC Services

```protobuf
service DwseNode {
  rpc Heartbeat   (HeartbeatRequest)  returns (HeartbeatResponse);
  rpc Election    (ElectionRequest)   returns (ElectionResponse);
  rpc SubmitTask  (TaskRequest)       returns (TaskResult);
  rpc ExecuteTask (TaskAssignment)    returns (TaskResult);
  rpc StealTasks  (StealRequest)      returns (StolenTasks);
}
```

---

## Run It

### Setup
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install grpcio grpcio-tools psutil
./scripts/build_proto.sh
```

### Start 5-node cluster
```bash
PEERS="0:localhost:50050,1:localhost:50051,2:localhost:50052,3:localhost:50053,4:localhost:50054"

for i in 0 1 2 3 4; do
  python -m src.node --id $i --port $((50050+i)) \
    --peers "$PEERS" --mode consistency \
    --advanced-weight --enable-stealing &
done
```

### Submit tasks
```bash
python submit_tasks.py --leader-port 50050 --num-tasks 500 --payload 200
```

### Run benchmark (configurable concurrency)
```bash
python bench_client.py \
  --leader-port 50050 \
  --num-tasks 4000 \
  --payload 200 \
  --concurrency 300
```

### Run experiment suite
```bash
./run_failure_recovery_test.sh   # leader kill → re-election timing
./run_fairness_test.sh           # load imbalance → rebalance over time
./run_strong_scaling_test.sh     # fixed tasks, vary node count
./run_weak_scaling_test.sh       # fixed tasks/node, scale nodes
```

---

## Tech Stack

| | |
|---|---|
| Language | Python 3 |
| RPC | gRPC |
| Serialization | Protocol Buffers |
| Metrics | psutil (CPU, memory, network) |
| Concurrency | ThreadPoolExecutor (256 workers) |
| Analysis | pandas, matplotlib |

---

## Design Decisions and Trade-offs

| Decision | Why | Known Limitation |
|---|---|---|
| Bully election by weight | Simple, no shared log needed | Not partition-safe; weight is node-local |
| Heartbeat as gossip channel | Zero extra protocol overhead | Stats are 1s stale; leader is bottleneck for stats |
| Work stealing with cooldown | Prevents thrashing under high load | Cooldown may delay rebalancing under burst |
| ThreadPoolExecutor 256 | Removes the 32-thread dispatch ceiling (~7× gain) | Memory overhead at 256 idle threads |
| Single leader dispatch | Simple routing, no coordination | Leader is single point of throughput scaling |
