import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sys
import re
import os
import glob

try:
    df = pd.read_csv('results/strong_scaling/results.csv', header=None, names=['TaskCount', 'Duration', 'Throughput'])
    
    node_counts = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    df['Nodes'] = node_counts[:len(df)]
    
    task_count = df['TaskCount'].iloc[0]
    
    df['AvgLatency'] = df['Duration'] / task_count
    df['Speedup'] = df['Duration'].iloc[0] / df['Duration'] 
    df['Efficiency'] = df['Speedup'] / df['Nodes'] * 100  
    
    plt.figure(figsize=(15, 10))
    
    plt.subplot(2, 2, 1)
    plt.plot(df['Nodes'], df['Throughput'], 'o-', linewidth=2)
    plt.xlabel('Number of Nodes')
    plt.ylabel('Throughput (tasks/sec)')
    plt.title('Throughput vs Number of Nodes')
    plt.grid(True)
    
    plt.subplot(2, 2, 2)
    plt.plot(df['Nodes'], df['AvgLatency'], 'o-', linewidth=2)
    plt.xlabel('Number of Nodes')
    plt.ylabel('Average Latency (sec/task)')
    plt.title('Average Latency vs Number of Nodes')
    plt.grid(True)
    
    plt.tight_layout()
    plt.savefig('results/strong_scaling/strong_scaling_results.png')
    
    count_logs = glob.glob('results/strong_scaling/count_*.log')
    
    for i, node_count in enumerate(df['Nodes']):
        log_file = f'results/strong_scaling/count_{int(node_count)}.log'
        if not os.path.exists(log_file):
            continue
            
        node_counts = {}
        
        with open(log_file, 'r') as f:
            log_content = f.read()
            
        distribution_section = re.search(r'Task distribution by node:(.*?)$', log_content, re.DOTALL)
        if distribution_section:
            distribution_text = distribution_section.group(1)
            for line in distribution_text.strip().split('\n'):
                match = re.search(r'Node (\d+): (\d+) tasks', line)
                if match:
                    node_id = int(match.group(1))
                    count = int(match.group(2))
                    node_counts[node_id] = count
        
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
    
    print("\nStrong Scaling Test Results:")
    print(df)

    
except Exception as e:
    print(f"Error analyzing results: {e}")
    sys.exit(1)
