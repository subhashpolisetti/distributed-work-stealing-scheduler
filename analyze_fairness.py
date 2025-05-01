import re
import matplotlib.pyplot as plt
import numpy as np
import sys

def extract_distribution(filename):
    node_counts = {}
    with open(filename, 'r') as f:
        content = f.read()
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

try:
    dist1 = extract_distribution('results/fairness/distribution1.log')
    dist2 = extract_distribution('results/fairness/distribution2.log')
    dist3 = extract_distribution('results/fairness/distribution3.log')
    
    
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
    
    plt.figure(figsize=(12, 8))
    
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
    
    plt.subplot(2, 1, 2)
    plt.plot([0, 45, 90], [std1, std2, std3], 'o-', linewidth=2)
    plt.xlabel('Time (seconds)')
    plt.ylabel('Standard Deviation')
    plt.title('Load Imbalance Over Time (Lower is Better)')
    plt.grid(True)
    
    plt.tight_layout()
    plt.savefig('results/fairness/fairness_results.png')
    print(f"Results saved to results/fairness/fairness_results.png")
    
    print("\nFairness Test Results:")
    print(f"Initial imbalance (std dev): {std1:.2f}")
    print(f"After 45 seconds (std dev): {std2:.2f}")
    print(f"After 90 seconds (std dev): {std3:.2f}")
    
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
