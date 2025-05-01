#!/bin/bash

mkdir -p results/weak_scaling


echo "This test measures how the system performs under increasing load with proportional resources"

TASKS_PER_NODE=20
NODE_CONFIGS=(1 2 3 4 5)
PAYLOAD=500

rm -f results/weak_scaling/results.csv

for NODE_COUNT in "${NODE_CONFIGS[@]}"; do
    echo "Testing with $NODE_COUNT nodes..."
    
    echo "Cleaning up any existing node processes..."
    for PORT in $(seq 50050 $((50050 + 9))); do
        lsof -t -i :$PORT | xargs -r kill -9
    done
    pkill -f "python.*node.py" || true
    sleep 2
    
    PEERS=""
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        if [ -n "$PEERS" ]; then
            PEERS="$PEERS,"
        fi
        PEERS="${PEERS}${i}:localhost:$((50050 + i))"
    done
    
    echo "Starting $NODE_COUNT nodes with advanced features..."
    
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        python -m src.node --id $i --port $((50050 + i)) --peers "$PEERS" --mode consistency --enable-stealing --log-level INFO > results/weak_scaling/node${i}.log 2>&1 &
    done
    
    echo "Waiting for nodes to start..."
    sleep 5
    
    TOTAL_TASKS=$((TASKS_PER_NODE * NODE_COUNT))
    
    echo "Testing with $NODE_COUNT nodes and $TOTAL_TASKS total tasks ($TASKS_PER_NODE tasks per node)..."
    
    python timing_helper.py $TOTAL_TASKS $PAYLOAD 50050 results/weak_scaling/load_${TOTAL_TASKS}.log results/weak_scaling/results.csv
    
    echo "Cleaning up..."
    for PORT in $(seq 50050 $((50050 + NODE_COUNT - 1))); do
        lsof -t -i :$PORT | xargs -r kill -9
    done
    pkill -f "python.*node.py" || true
    sleep 2
done

cat > analyze_weak_scaling.py << 'EOF'
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sys
import re
import os

try:
    # Read the results
    df = pd.read_csv('results/weak_scaling/results.csv', header=None, names=['task_count', 'duration', 'throughput'])
    
    # Add a nodes column based on the task count and tasks per node
    tasks_per_node = 20  # This should match TASKS_PER_NODE in the bash script
    df['nodes'] = df['task_count'] / tasks_per_node
    
    # Calculate average latency
    df['AvgLatency'] = df['duration'].astype(float) / df['task_count'].astype(float)
    
    # Calculate throughput per node (key metric for weak scaling)
    df['ThroughputPerNode'] = df['throughput'].astype(float) / df['nodes'].astype(float)
    
    # Calculate scaling efficiency (relative to single node performance)
    base_throughput_per_node = df.loc[df['nodes'] == 1, 'ThroughputPerNode'].values[0]
    df['ScalingEfficiency'] = (df['ThroughputPerNode'] / base_throughput_per_node) * 100
    
    # Create the plots
    plt.figure(figsize=(15, 10))
    
    # Plot 1: Total Throughput vs Node Count
    plt.subplot(2, 2, 1)
    plt.plot(df['nodes'], df['throughput'].astype(float), 'o-', linewidth=2)
    plt.xlabel('Number of Nodes')
    plt.ylabel('Total Throughput (tasks/sec)')
    plt.title('Total Throughput vs Node Count')
    plt.grid(True)
    
    # Plot 2: Throughput Per Node vs Node Count
    plt.subplot(2, 2, 2)
    plt.plot(df['nodes'], df['ThroughputPerNode'], 'o-', linewidth=2)
    plt.xlabel('Number of Nodes')
    plt.ylabel('Throughput Per Node (tasks/sec/node)')
    plt.title('Throughput Per Node vs Node Count')
    plt.grid(True)
    
    # Plot 3: Scaling Efficiency vs Node Count
    plt.subplot(2, 2, 3)
    plt.plot(df['nodes'], df['ScalingEfficiency'], 'o-', linewidth=2)
    plt.xlabel('Number of Nodes')
    plt.ylabel('Scaling Efficiency (%)')
    plt.title('Scaling Efficiency vs Node Count')
    plt.grid(True)
    
    # Plot 4: Average Latency vs Node Count
    plt.subplot(2, 2, 4)
    plt.plot(df['nodes'], df['AvgLatency'], 'o-', linewidth=2)
    plt.xlabel('Number of Nodes')
    plt.ylabel('Average Latency (sec/task)')
    plt.title('Average Latency vs Node Count')
    plt.grid(True)
    
    plt.tight_layout()
    plt.savefig('results/weak_scaling/weak_scaling_results.png')
    
    # Analyze task distribution across nodes
    node_tasks = {}
    
    for _, row in df.iterrows():
        load = int(row['task_count'])  # Ensure integer for filename
        log_file = f'results/weak_scaling/load_{load}.log'
        
        # Make sure the file exists
        if not os.path.exists(log_file):
            print(f"Warning: Log file {log_file} not found.")
            node_tasks[load] = {}
            continue
        node_counts = {}
        
        try:
            with open(log_file, 'r') as f:
                log_content = f.read()
                
            # Extract task distribution
            distribution_section = re.search(r'Task distribution by node:(.*?)$', log_content, re.DOTALL)
            if distribution_section:
                distribution_text = distribution_section.group(1)
                for line in distribution_text.strip().split('\n'):
                    match = re.search(r'Node (\d+): (\d+) tasks', line)
                    if match:
                        node_id = int(match.group(1))
                        count = int(match.group(2))
                        node_counts[node_id] = count
            
            node_tasks[load] = node_counts
        except Exception as e:
            print(f"Warning: Error processing log file {log_file}: {e}")
            node_tasks[load] = {}
    
    # Create a stacked bar chart of task distribution if we have data
    if any(node_tasks.values()):
        plt.figure(figsize=(12, 6))
        
        nodes = sorted(set(node_id for counts in node_tasks.values() for node_id in counts.keys()))
        loads = sorted(node_tasks.keys())
        
        bottom = np.zeros(len(loads))
        
        for node in nodes:
            values = [node_tasks[load].get(node, 0) for load in loads]
            plt.bar(loads, values, bottom=bottom, label=f'Node {node}')
            bottom += values
        
        plt.xlabel('Total Number of Tasks')
        plt.ylabel('Tasks Executed')
        plt.title('Task Distribution Across Nodes')
        plt.legend()
        plt.grid(True)
        
        plt.tight_layout()
        plt.savefig('results/weak_scaling/task_distribution.png')
    else:
        print("Warning: No task distribution data available. Skipping task distribution chart.")
    
    # Print summary
    print("\nWeak Scaling Test Results:")
    print(df)
    print("\nIdeal weak scaling would show constant throughput per node as node count increases.")
    
    # Print scaling efficiency
    print("\nScaling Efficiency (% of single-node throughput per node):")
    for _, row in df.iterrows():
        nodes = row['nodes']
        efficiency = row['ScalingEfficiency']
        print(f"{nodes} nodes: {efficiency:.1f}%")
    
except Exception as e:
    print(f"Error analyzing results: {e}")
    sys.exit(1)
EOF

echo "Analyzing results..."
python analyze_weak_scaling.py

echo "Cleaning up..."
echo "Using lsof to find and kill all processes using ports 50050-50054..."
for PORT in 50050 50051 50052 50053 50054; do
    lsof -t -i :$PORT | xargs -r kill -9
done
pkill -f "python.*node.py" || true

echo "=== Weak Scaling Test Complete ==="
echo "Results are in results/weak_scaling/ directory"
echo "Check results/weak_scaling/weak_scaling_results.png for visualization"
