#!/usr/bin/env python3

import time
import subprocess
import sys
import os

def run_with_precise_timing(task_count, payload, leader_port=50050, output_file=None):
   
    cmd = ["python", "submit_tasks.py", 
           "--leader-port", str(leader_port), 
           "--num-tasks", str(task_count), 
           "--payload", str(payload)]
    
    start_time = time.time()
    
    if output_file:
        with open(output_file, "w") as f:
            subprocess.run(cmd, stdout=f)
    else:
        subprocess.run(cmd)
    
    end_time = time.time()
    
    duration = end_time - start_time
    
    if duration < 0.000001:
        duration = 0.000001
    
    throughput = task_count / duration
    
    return duration, throughput

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python timing_helper.py <task_count> <payload> <leader_port> [output_file] [csv_file]")
        sys.exit(1)
    
    task_count = int(sys.argv[1])
    payload = int(sys.argv[2])
    leader_port = int(sys.argv[3])
    
    output_file = None
    if len(sys.argv) > 4:
        output_file = sys.argv[4]
    
    csv_file = None
    if len(sys.argv) > 5:
        csv_file = sys.argv[5]
    
    duration, throughput = run_with_precise_timing(task_count, payload, leader_port, output_file)
    
    print(f"Tasks: {task_count}, Duration: {duration:.6f} seconds, Throughput: {throughput:.2f} tasks/sec")
    
    if csv_file:
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)
        
        with open(csv_file, "a") as f:
            f.write(f"{task_count},{duration:.6f},{throughput:.6f}\n")
