import argparse
import grpc
import time
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.abspath("src"))

import dwse_pb2 as pb
import dwse_pb2_grpc as pb_grpc

def submit_task(stub, task_id, payload, task_type):
    """Submit a single task and return the result."""
    try:
        req = pb.TaskRequest(task_id=task_id, payload=str(payload), task_type=task_type)
        res = stub.SubmitTask(req, timeout=10.0)
        print(f"Task {task_id} result: {res.result} (executed by node {res.node_id})")
        return (task_id, res.result, res.node_id)
    except grpc.RpcError as e:
        details = e.details() if hasattr(e, 'details') and callable(e.details) else ""
        if "NotLeader" in details:
            new_leader_id = -1
            try:
                parts = details.split("leader_id=")
                if len(parts) > 1:
                    new_leader_id = int(parts[1])
            except:
                new_leader_id = -1
            
            if new_leader_id >= 0:
                print(f"Task {task_id} redirected to new leader (Node {new_leader_id})")
                return (task_id, "REDIRECT", new_leader_id)
        print(f"Task {task_id} failed: {e}")
        return (task_id, "ERROR", -1)

def submit_tasks(leader_port, num_tasks, payload, task_type="default"):
    channel = grpc.insecure_channel(f"localhost:{leader_port}")
    stub = pb_grpc.DwseNodeStub(channel)
    
    print(f"Submitting {num_tasks} tasks to leader at localhost:{leader_port}")
    print(f"Each task has payload: {payload}ms of simulated work, type: {task_type}")
    
    results = []
    start_time = time.time()
    
    max_workers = min(num_tasks, 100) 
    
    # Submit tasks in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_task = {
            executor.submit(submit_task, stub, i, payload, task_type): i 
            for i in range(1, num_tasks + 1)
        }
        
        for future in as_completed(future_to_task):
            task_id, result, node_id = future.result()
            
            # Handle redirection to a new leader
            if result == "REDIRECT" and node_id >= 0:
                base_port = leader_port - (leader_port % 10)
                new_port = base_port + node_id
                
                results.append((task_id, f"Redirected to node {node_id}", node_id))
            else:
                results.append((task_id, result, node_id))
    
    end_time = time.time()
    total_time = end_time - start_time
    
    print("\nTask Submission Summary:")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Average time per task: {total_time / num_tasks:.2f} seconds")
    
    node_counts = {}
    for _, _, node_id in results:
        node_counts[node_id] = node_counts.get(node_id, 0) + 1
    
    print("\nTask distribution by node:")
    for node_id, count in sorted(node_counts.items()):
        print(f"Node {node_id}: {count} tasks ({count/len(results)*100:.1f}%)")
    
    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Submit tasks to DWSE cluster")
    parser.add_argument("--leader-port", type=int, default=50050, help="Port of the leader node")
    parser.add_argument("--num-tasks", type=int, default=10, help="Number of tasks to submit")
    parser.add_argument("--payload", type=int, default=100, help="Task payload (milliseconds of work)")
    parser.add_argument("--task-type", type=str, default="default", help="Task type for specialization")
    args = parser.parse_args()
    
    submit_tasks(args.leader_port, args.num_tasks, args.payload, args.task_type)
