#!/usr/bin/env python3
"""High-concurrency benchmark client for DWSE.

Submits N tasks of fixed payload (ms of real work) to the leader using C
concurrent threads, and reports wall time + throughput + per-node distribution.
Unlike submit_tasks.py (capped at 100 threads), concurrency is configurable so
worker nodes can actually be saturated.
"""
import argparse, grpc, time, os, sys
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.abspath("src"))
import dwse_pb2 as pb
import dwse_pb2_grpc as pb_grpc


def one(stub, tid, payload, ttype):
    try:
        r = stub.SubmitTask(pb.TaskRequest(task_id=tid, payload=str(payload), task_type=ttype), timeout=30.0)
        return r.node_id
    except grpc.RpcError:
        return -1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--leader-port", type=int, default=50050)
    ap.add_argument("--num-tasks", type=int, default=1500)
    ap.add_argument("--payload", type=int, default=200)
    ap.add_argument("--concurrency", type=int, default=150)
    ap.add_argument("--task-type", default="default")
    a = ap.parse_args()

    ch = grpc.insecure_channel(f"localhost:{a.leader_port}")
    stub = pb_grpc.DwseNodeStub(ch)

    dist = {}
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=a.concurrency) as ex:
        futs = [ex.submit(one, stub, i, a.payload, a.task_type) for i in range(1, a.num_tasks + 1)]
        for f in as_completed(futs):
            nid = f.result()
            dist[nid] = dist.get(nid, 0) + 1
    dt = time.time() - t0

    ok = sum(v for k, v in dist.items() if k >= 0)
    print(f"tasks={a.num_tasks} payload={a.payload}ms concurrency={a.concurrency} "
          f"duration={dt:.3f}s throughput={ok/dt:.1f} tasks/sec ok={ok} failed={dist.get(-1,0)}")
    print("distribution:", {k: dist[k] for k in sorted(dist)})


if __name__ == "__main__":
    main()
