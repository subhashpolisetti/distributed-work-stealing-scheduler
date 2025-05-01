#!/bin/bash
# Tests how the system performs with increasing number of nodes (strong scaling)

mkdir -p results/strong_scaling

echo "This test measures how the system performs with increasing number of nodes"

echo "Cleaning up any existing node processes..."
for PORT in 50050 50051 50052 50053 50054 50055 50056 50057 50058 50059; do
    lsof -t -i :$PORT | xargs -r kill -9
done
pkill -f "python.*node.py" || true
sleep 2

NODE_COUNTS=(1 2 3 4 5 6 7 8 9 10)
TASK_COUNT=5000 
PAYLOAD=500     

BASE_PORT=50050

rm -f results/strong_scaling/results.csv

echo "Running tests for different node counts..."
for NODE_COUNT in "${NODE_COUNTS[@]}"; do
    echo "Testing with $NODE_COUNT nodes..."
    
    PEERS=""
    for ((i=0; i<NODE_COUNT; i++)); do
        if [ "$i" -gt 0 ]; then
            PEERS="$PEERS,"
        fi
        PORT=$((BASE_PORT + i))
        PEERS="${PEERS}${i}:localhost:${PORT}"
    done
    
    for ((i=0; i<NODE_COUNT; i++)); do
        PORT=$((BASE_PORT + i))
        python -m src.node --id $i --port $PORT --peers "$PEERS" --mode consistency --enable-stealing --log-level DEBUG > results/strong_scaling/node${i}_${NODE_COUNT}nodes.log 2>&1 &
    done
    
    echo "Waiting for nodes to start..."
    sleep 5
    
    python timing_helper.py $TASK_COUNT $PAYLOAD $BASE_PORT results/strong_scaling/count_${NODE_COUNT}.log results/strong_scaling/results.csv
    
    echo "Cleaning up nodes..."
    echo "Using lsof to find and kill all processes using ports..."
    for ((i=0; i<NODE_COUNT; i++)); do
        PORT=$((BASE_PORT + i))
        lsof -t -i :$PORT | xargs -r kill -9
    done
    pkill -f "python.*node.py" || true
    sleep 2
done

cat > analyze_strong_scaling.py << 'EOF'
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sys
import re
import os
import glob

try:
    # Read the results
    df = pd.read_csv('results/strong_scaling/results.csv', header=None, names=['TaskCount', 'Duration', 'Throughput'])
    
    # Add a Nodes column based on the order of the rows
    # Assuming the results are in order of NODE_COUNTS
    node_counts = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    df['Nodes'] = node_counts[:len(df)]
    
    # Get the task count from the first row
    task_count = df['TaskCount'].iloc[0]
    
    # Calculate average latency and speedup
    df['AvgLatency'] = df['Duration'] / task_count
    df['Speedup'] = df['Duration'].iloc[0] / df['Duration']  # Speedup relative to 1 node
    df['Efficiency'] = df['Speedup'] / df['Nodes'] * 100  # Parallel efficiency as percentage
    
    # Create the plots
    plt.figure(figsize=(15, 10))
    
    # Plot 1: Throughput vs Nodes
    plt.subplot(2, 2, 1)
    plt.plot(df['Nodes'], df['Throughput'], 'o-', linewidth=2)
    plt.xlabel('Number of Nodes')
    plt.ylabel('Throughput (tasks/sec)')
    plt.title('Throughput vs Number of Nodes')
    plt.grid(True)
    
    # Plot 2: Average Latency vs Nodes
    plt.subplot(2, 2, 2)
    plt.plot(df['Nodes'], df['AvgLatency'], 'o-', linewidth=2)
    plt.xlabel('Number of Nodes')
    plt.ylabel('Average Latency (sec/task)')
    plt.title('Average Latency vs Number of Nodes')
    plt.grid(True)
    
    # Plot 3: Speedup vs Nodes
    plt.subplot(2, 2, 3)
    plt.plot(df['Nodes'], df['Speedup'], 'o-', linewidth=2, label='Actual')
    plt.plot(df['Nodes'], df['Nodes'], '--', linewidth=1, label='Ideal')
    plt.xlabel('Number of Nodes')
    plt.ylabel('Speedup')
    plt.title('Speedup vs Number of Nodes')
    plt.legend()
    plt.grid(True)
    
    # Plot 4: Parallel Efficiency vs Nodes
    plt.subplot(2, 2, 4)
    plt.plot(df['Nodes'], df['Efficiency'], 'o-', linewidth=2)
    plt.axhline(y=100, linestyle='--', color='r')
    plt.xlabel('Number of Nodes')
    plt.ylabel('Parallel Efficiency (%)')
    plt.title('Parallel Efficiency vs Number of Nodes')
    plt.grid(True)
    
    plt.tight_layout()
    plt.savefig('results/strong_scaling/strong_scaling_results.png')
    
    # Find all count log files
    count_logs = glob.glob('results/strong_scaling/count_*.log')
    
    # Analyze task distribution across nodes for each test
    for i, node_count in enumerate(df['Nodes']):
        # Find the corresponding log file
        log_file = f'results/strong_scaling/count_{int(node_count)}.log'
        if not os.path.exists(log_file):
            print(f"Warning: Log file {log_file} not found, skipping distribution analysis")
            continue
            
        node_counts = {}
        
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
        
        # Create a bar chart for this node count
        plt.figure(figsize=(8, 5))
        nodes = sorted(node_counts.keys())
        counts = [node_counts[node] for node in nodes]
        
        plt.bar(nodes, counts)
        plt.xlabel('Node ID')
        plt.ylabel('Tasks Executed')
        plt.title(f'Task Distribution with {int(node_count)} Nodes')
        plt.xticks(nodes)
        plt.grid(axis='y')
        
        plt.tight_layout()
        plt.savefig(f'results/strong_scaling/distribution_{int(node_count)}nodes.png')
    
    # Print summary
    print("\nStrong Scaling Test Results:")
    print(df)
    print("\nIdeal strong scaling would show linear speedup as nodes increase.")
    
    # Calculate Amdahl's Law fit
    from scipy.optimize import curve_fit
    
    def amdahl(p, s):
        """Amdahl's Law: speedup = 1 / (s + (1-s)/p) where s is serial fraction and p is processors"""
        return 1 / (s + (1-s)/p)
    
    try:
        # Fit Amdahl's Law to the speedup data
        popt, _ = curve_fit(amdahl, df['Nodes'], df['Speedup'])
        serial_fraction = popt[0]
        
        # Create a plot showing the fit
        plt.figure(figsize=(10, 6))
        nodes = np.linspace(1, max(df['Nodes'])*1.5, 100)
        plt.plot(df['Nodes'], df['Speedup'], 'o', label='Measured')
        plt.plot(nodes, amdahl(nodes, serial_fraction), '-', label=f"Amdahl's Law (s={serial_fraction:.3f})")
        plt.plot(nodes, nodes, '--', label='Ideal Linear')
        plt.xlabel('Number of Nodes')
        plt.ylabel('Speedup')
        plt.title("Strong Scaling: Amdahl's Law Fit")
        plt.legend()
        plt.grid(True)
        plt.savefig('results/strong_scaling/amdahl_fit.png')
        
        print(f"\nAmdahl's Law Analysis:")
        print(f"Estimated serial fraction: {serial_fraction:.3f}")
        print(f"Maximum theoretical speedup: {1/serial_fraction:.1f}x")
    except:
        print("\nCould not fit Amdahl's Law to the data")
    
except Exception as e:
    print(f"Error analyzing results: {e}")
    sys.exit(1)
EOF

echo "Analyzing results..."
python analyze_strong_scaling.py

echo "=== Strong Scaling Test Complete ==="
echo "Results are in results/strong_scaling/ directory"
echo "Check results/strong_scaling/strong_scaling_results.png for visualization"
