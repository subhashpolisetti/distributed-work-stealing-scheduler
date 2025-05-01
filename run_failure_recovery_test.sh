#!/bin/bash


mkdir -p results/failure_recovery

echo "This test measures how quickly the system recovers from leader failures"

echo "Cleaning up any existing node processes..."

for PORT in 50050 50051 50052 50053 50054; do
    lsof -t -i :$PORT | xargs -r kill -9
done

pkill -f "python.*node.py" || true
sleep 2


echo "Starting 5 nodes with advanced features..."
PEERS="0:localhost:50050,1:localhost:50051,2:localhost:50052,3:localhost:50053,4:localhost:50054"

# Start nodes 
python -m src.node --id 0 --port 50050 --peers "$PEERS" --mode consistency --advanced-weight --log-level DEBUG > results/failure_recovery/node0.log 2>&1 &
PID_NODE0=$!
python -m src.node --id 1 --port 50051 --peers "$PEERS" --mode consistency --advanced-weight --log-level DEBUG > results/failure_recovery/node1.log 2>&1 &
python -m src.node --id 2 --port 50052 --peers "$PEERS" --mode consistency --advanced-weight --log-level DEBUG > results/failure_recovery/node2.log 2>&1 &
python -m src.node --id 3 --port 50053 --peers "$PEERS" --mode consistency --advanced-weight --log-level DEBUG > results/failure_recovery/node3.log 2>&1 &
python -m src.node --id 4 --port 50054 --peers "$PEERS" --mode consistency --advanced-weight --log-level DEBUG > results/failure_recovery/node4.log 2>&1 &


echo "Waiting for nodes to start..."
sleep 5

echo "Submitting initial tasks to verify system..."
python submit_tasks.py --leader-port 50050 --num-tasks 10 --payload 100 > results/failure_recovery/initial_tasks.log

echo "Killing leader node (Node 0)..."
FAILURE_TIME=$(date +%s)
echo "Failure time: $FAILURE_TIME" > results/failure_recovery/timing.log


echo "Using lsof to find and kill the process using port 50050..."
lsof -t -i :50050 | xargs -r kill -9

echo "Attempting to submit tasks until new leader is detected..."
NEW_LEADER_DETECTED=false
ATTEMPTS=0
MAX_ATTEMPTS=30
RECOVERY_TIME=""

while [ $ATTEMPTS -lt $MAX_ATTEMPTS ] && [ "$NEW_LEADER_DETECTED" = false ]; do
    ATTEMPTS=$((ATTEMPTS+1))
    
    for PORT in 50051 50052 50053 50054; do
        OUTPUT=$(python submit_tasks.py --leader-port $PORT --num-tasks 1 --payload 10 2>&1)
        if echo "$OUTPUT" | grep -q "Task 1 result"; then
            NEW_LEADER_DETECTED=true
            RECOVERY_TIME=$(date +%s)
            NEW_LEADER_PORT=$PORT
            echo "New leader detected at port $PORT"
            echo "Recovery time: $RECOVERY_TIME" >> results/failure_recovery/timing.log
            break
        fi
    done
    
    if [ "$NEW_LEADER_DETECTED" = false ]; then
        echo "Attempt $ATTEMPTS: No new leader detected yet..."
        sleep 0.5
    fi
done

if [ "$NEW_LEADER_DETECTED" = true ]; then
    RECOVERY_DURATION=$(echo "$RECOVERY_TIME - $FAILURE_TIME" | bc)
    echo "Recovery duration: $RECOVERY_DURATION seconds" >> results/failure_recovery/timing.log
    
    echo "Submitting tasks to new leader..."
    python submit_tasks.py --leader-port $NEW_LEADER_PORT --num-tasks 20 --payload 50 > results/failure_recovery/post_recovery_tasks.log
    
    cat > analyze_recovery.py << 'EOF'
import re
import matplotlib.pyplot as plt
import numpy as np
import sys

# Read timing information
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
    
    # Plot the results
    plt.figure(figsize=(10, 6))
    
    # Create a timeline visualization
    plt.plot([0, recovery_duration], [1, 1], 'k-', linewidth=2)
    plt.plot([0], [1], 'go', markersize=10, label='Failure Occurs')
    plt.plot([recovery_duration], [1], 'ro', markersize=10, label='New Leader Elected')
    
    # Add annotations
    plt.annotate(f'Recovery Time: {recovery_duration:.2f} seconds', 
                xy=(recovery_duration/2, 1.05), 
                xytext=(recovery_duration/2, 1.05),
                ha='center',
                fontsize=12)
    
    # Set plot properties
    plt.ylim(0.5, 1.5)
    plt.xlim(-0.5, recovery_duration + 0.5)
    plt.yticks([])
    plt.xlabel('Time (seconds)')
    plt.title('Leader Failure Recovery Timeline')
    plt.legend()
    plt.grid(axis='x')
    
    plt.tight_layout()
    plt.savefig('results/failure_recovery/recovery_timeline.png')
    
    # Print summary
    print("\nFailure Recovery Test Results:")
    print(f"Time to recover from leader failure: {recovery_duration:.2f} seconds")
    
    # Analyze node logs to find election messages
    election_times = []
    
    for node_id in range(1, 5):  # Nodes 1-4 (excluding the failed Node 0)
        log_file = f'results/failure_recovery/node{node_id}.log'
        with open(log_file, 'r') as f:
            log_content = f.read()
            
        # Look for election initiation and leader election messages
        election_start = re.search(r'(\d+:\d+:\d+\.\d+).*Heartbeat timeout → election', log_content)
        became_leader = re.search(r'(\d+:\d+:\d+\.\d+).*Became leader', log_content)
        
        if election_start:
            print(f"Node {node_id} detected leader failure and started election")
        
        if became_leader:
            print(f"Node {node_id} became the new leader")
    
except Exception as e:
    print(f"Error analyzing results: {e}")
    sys.exit(1)
EOF

    echo "Analyzing results..."
    python analyze_recovery.py
    
    echo "=== Failure Recovery Test Complete ==="
    echo "Results are in results/failure_recovery/ directory"
    echo "Check results/failure_recovery/recovery_timeline.png for visualization"
else
    echo "Failed to detect new leader after $MAX_ATTEMPTS attempts"
    echo "Test failed"
fi

echo "Cleaning up..."
echo "Using lsof to find and kill all processes using ports 50051-50054..."
for PORT in 50051 50052 50053 50054; do
    lsof -t -i :$PORT | xargs -r kill -9
done
pkill -f "python.*node.py" || true
