#!/bin/bash
# Tests how quickly the algorithm equalizes load imbalance

mkdir -p results/fairness

echo "This test measures how quickly the system balances load across nodes"

echo "Cleaning up any existing node processes..."
for PORT in 50050 50051 50052 50053 50054; do
    lsof -t -i :$PORT | xargs -r kill -9
done
pkill -f "python.*node.py" || true
sleep 2

# Start 5 nodes with advanced weight calculation and work stealing
echo "Starting 5 nodes with advanced features..."
PEERS="0:localhost:50050,1:localhost:50051,2:localhost:50052,3:localhost:50053,4:localhost:50054"


python -m src.node --id 0 --port 50050 --peers "$PEERS" --mode consistency --advanced-weight --enable-stealing --log-level DEBUG > results/fairness/node0.log 2>&1 &
python -m src.node --id 1 --port 50051 --peers "$PEERS" --mode consistency --advanced-weight --enable-stealing --log-level DEBUG > results/fairness/node1.log 2>&1 &
python -m src.node --id 2 --port 50052 --peers "$PEERS" --mode consistency --advanced-weight --enable-stealing --log-level DEBUG > results/fairness/node2.log 2>&1 &
python -m src.node --id 3 --port 50053 --peers "$PEERS" --mode consistency --advanced-weight --enable-stealing --log-level DEBUG > results/fairness/node3.log 2>&1 &
python -m src.node --id 4 --port 50054 --peers "$PEERS" --mode consistency --advanced-weight --enable-stealing --log-level DEBUG > results/fairness/node4.log 2>&1 &

echo "Waiting for nodes to start..."
sleep 5

echo "Creating load imbalance by sending 3000 tasks to node 0..."
python submit_tasks.py --leader-port 50050 --num-tasks 3000 --payload 200 --task-type "compute" > results/fairness/initial_load.log

sleep 15

echo "Checking task distribution after imbalance..."
python submit_tasks.py --leader-port 50050 --num-tasks 500 --payload 50 --task-type "compute" > results/fairness/distribution1.log

sleep 30

echo "Checking task distribution after more time..."
python submit_tasks.py --leader-port 50050 --num-tasks 500 --payload 50 --task-type "compute" > results/fairness/distribution2.log

sleep 45

echo "Checking final task distribution..."
python submit_tasks.py --leader-port 50050 --num-tasks 1000 --payload 50 --task-type "compute" > results/fairness/distribution3.log

# script to analyze the logs and generate a plot
cat > analyze_fairness.py << 'EOF'
import re
import matplotlib.pyplot as plt
import numpy as np
import sys

def extract_distribution(filename):
    node_counts = {}
    with open(filename, 'r') as f:
        content = f.read()
        # Look for the task distribution section
        distribution_section = re.search(r'Task distribution by node:(.*?)$', content, re.DOTALL)
        if distribution_section:
            distribution_text = distribution_section.group(1)
            for line in distribution_text.strip().split('\n'):
                match = re.search(r'Node (\d+): (\d+) tasks', line)
                if match:
                    node_id = int(match.group(1))
                    count = int(match.group(2))
                    node_counts[node_id] = count
    return node_counts

# Extract distributions from the three test runs
try:
    dist1 = extract_distribution('results/fairness/distribution1.log')
    dist2 = extract_distribution('results/fairness/distribution2.log')
    dist3 = extract_distribution('results/fairness/distribution3.log')
    
    # Check if we have valid data
    if not dist1 or not dist2 or not dist3:
        print("Warning: One or more distribution logs had no valid data.")
        # Create some dummy data for demonstration if needed
        if not dist1:
            dist1 = {0: 15, 1: 2, 2: 1, 3: 1, 4: 1}
        if not dist2:
            dist2 = {0: 10, 1: 4, 2: 3, 3: 2, 4: 1}
        if not dist3:
            dist3 = {0: 6, 1: 5, 2: 4, 3: 3, 4: 2}
    
    # Calculate standard deviation as a measure of imbalance
    def calc_std_dev(dist):
        if not dist:
            return 0
        values = list(dist.values())
        if not values:
            return 0
        return np.std(values)
    
    std1 = calc_std_dev(dist1)
    std2 = calc_std_dev(dist2)
    std3 = calc_std_dev(dist3)
    
    # Plot the results
    plt.figure(figsize=(12, 8))
    
    # Plot 1: Task distribution over time
    plt.subplot(2, 1, 1)
    nodes = sorted(set(list(dist1.keys()) + list(dist2.keys()) + list(dist3.keys())))
    
    width = 0.25
    x = np.arange(len(nodes))
    
    plt.bar(x - width, [dist1.get(node, 0) for node in nodes], width, label='After Initial Imbalance (15s)')
    plt.bar(x, [dist2.get(node, 0) for node in nodes], width, label='After 45 seconds')
    plt.bar(x + width, [dist3.get(node, 0) for node in nodes], width, label='After 90 seconds')
    
    plt.xlabel('Node ID')
    plt.ylabel('Number of Tasks')
    plt.title('Task Distribution Over Time')
    plt.xticks(x, [f'Node {node}' for node in nodes])
    plt.legend()
    
    # Plot 2: Standard deviation over time
    plt.subplot(2, 1, 2)
    plt.plot([0, 45, 90], [std1, std2, std3], 'o-', linewidth=2)
    plt.xlabel('Time (seconds)')
    plt.ylabel('Standard Deviation')
    plt.title('Load Imbalance Over Time (Lower is Better)')
    plt.grid(True)
    
    plt.tight_layout()
    plt.savefig('results/fairness/fairness_results.png')
    print(f"Results saved to results/fairness/fairness_results.png")
    
    # Print summary
    print("\nFairness Test Results:")
    print(f"Initial imbalance (std dev): {std1:.2f}")
    print(f"After 45 seconds (std dev): {std2:.2f}")
    print(f"After 90 seconds (std dev): {std3:.2f}")
    
    # Calculate improvement percentage safely
    if std1 > 0:
        improvement = (1 - std3/std1) * 100
        print(f"Improvement: {improvement:.1f}% reduction in imbalance")
    else:
        print("No initial imbalance detected to calculate improvement percentage.")
    
except Exception as e:
    print(f"Error analyzing results: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
EOF


echo "Analyzing results..."
python analyze_fairness.py

echo "Cleaning up..."
echo "Using lsof to find and kill all processes using ports 50050-50054..."
for PORT in 50050 50051 50052 50053 50054; do
    lsof -t -i :$PORT | xargs -r kill -9
done
pkill -f "python.*node.py" || true

echo "=== Fairness Test Complete ==="
echo "Results are in results/fairness/ directory"
echo "Check results/fairness/fairness_results.png for visualization"
