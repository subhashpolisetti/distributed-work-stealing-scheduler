import re
import matplotlib.pyplot as plt
import numpy as np
import sys

try:
    with open('results/failure_recovery/timing.log', 'r') as f:
        lines = f.readlines()
        
    failure_time = None
    recovery_time = None
    recovery_duration = None
    
    for line in lines:
        if 'Failure time:' in line:
            failure_time = float(line.split(':')[1].strip())
        elif 'Recovery time:' in line:
            recovery_time = float(line.split(':')[1].strip())
        elif 'Recovery duration:' in line:
            recovery_duration = float(line.split(':')[1].strip().replace('seconds', ''))
    
    if recovery_duration is None and failure_time is not None and recovery_time is not None:
        recovery_duration = recovery_time - failure_time
    
    plt.figure(figsize=(10, 6))
    
    plt.plot([0, recovery_duration], [1, 1], 'k-', linewidth=2)
    plt.plot([0], [1], 'go', markersize=10, label='Failure Occurs')
    plt.plot([recovery_duration], [1], 'ro', markersize=10, label='New Leader Elected')
    
    plt.annotate(f'Recovery Time: {recovery_duration:.2f} seconds', 
                xy=(recovery_duration/2, 1.05), 
                xytext=(recovery_duration/2, 1.05),
                ha='center',
                fontsize=12)
    
    plt.ylim(0.5, 1.5)
    plt.xlim(-0.5, recovery_duration + 0.5)
    plt.yticks([])
    plt.xlabel('Time (seconds)')
    plt.title('Leader Failure Recovery Timeline')
    plt.legend()
    plt.grid(axis='x')
    
    plt.tight_layout()
    plt.savefig('results/failure_recovery/recovery_timeline.png')
    
    print("\nFailure Recovery Test Results:")
    print(f"Time to recover from leader failure: {recovery_duration:.2f} seconds")
    
    election_times = []
    
    for node_id in range(1, 5):  
        log_file = f'results/failure_recovery/node{node_id}.log'
        with open(log_file, 'r') as f:
            log_content = f.read()
            
       
        election_start = re.search(r'(\d+:\d+:\d+\.\d+).*Heartbeat timeout → election', log_content)
        became_leader = re.search(r'(\d+:\d+:\d+\.\d+).*Became leader', log_content)
        
        if election_start:
            print(f"Node {node_id} detected leader failure and started election")
        
        if became_leader:
            print(f"Node {node_id} became the new leader")
    
except Exception as e:
    print(f"Error analyzing results: {e}")
    sys.exit(1)
