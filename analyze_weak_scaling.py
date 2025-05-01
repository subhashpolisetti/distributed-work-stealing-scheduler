import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import sys
import re
import os

try:
    df = pd.read_csv('results/weak_scaling/results.csv', header=None, names=['task_count', 'duration', 'throughput'])
    
    tasks_per_node = 20 
    df['nodes'] = df['task_count'] / tasks_per_node
    
    df['AvgLatency'] = df['duration'].astype(float) / df['task_count'].astype(float)
    
    df['ThroughputPerNode'] = df['throughput'].astype(float) / df['nodes'].astype(float)
    
    base_throughput_per_node = df.loc[df['nodes'] == 1, 'ThroughputPerNode'].values[0]
    df['ScalingEfficiency'] = (df['ThroughputPerNode'] / base_throughput_per_node) * 100
    
    plt.figure(figsize=(15, 10))
    
    plt.subplot(2, 2, 1)
    plt.plot(df['nodes'], df['throughput'].astype(float), 'o-', linewidth=2)
    plt.xlabel('Number of Nodes')
    plt.ylabel('Total Throughput (tasks/sec)')
    plt.title('Total Throughput vs Node Count')
    plt.grid(True)
    
    plt.subplot(2, 2, 2)
    plt.plot(df['nodes'], df['ThroughputPerNode'], 'o-', linewidth=2)
    plt.xlabel('Number of Nodes')
    plt.ylabel('Throughput Per Node (tasks/sec/node)')
    plt.title('Throughput Per Node vs Node Count')
    plt.grid(True)
    
    plt.subplot(2, 2, 3)
    plt.plot(df['nodes'], df['ScalingEfficiency'], 'o-', linewidth=2)
    plt.xlabel('Number of Nodes')
    plt.ylabel('Scaling Efficiency (%)')
    plt.title('Scaling Efficiency vs Node Count')
    plt.grid(True)
    
    plt.subplot(2, 2, 4)
    plt.plot(df['nodes'], df['AvgLatency'], 'o-', linewidth=2)
    plt.xlabel('Number of Nodes')
    plt.ylabel('Average Latency (sec/task)')
    plt.title('Average Latency vs Node Count')
    plt.grid(True)
    
    plt.tight_layout()
    plt.savefig('results/weak_scaling/weak_scaling_results.png')
    
    node_tasks = {}
    
    for _, row in df.iterrows():
        load = int(row['task_count']) 
        log_file = f'results/weak_scaling/load_{load}.log'
        
        if not os.path.exists(log_file):
            print(f"Warning: Log file {log_file} not found.")
            node_tasks[load] = {}
            continue
        node_counts = {}
        
        try:
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
            
            node_tasks[load] = node_counts
        except Exception as e:
            print(f"Warning: Error processing log file {log_file}: {e}")
            node_tasks[load] = {}
    
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
   
    
    print("\nWeak Scaling Test Results:")
    print(df)
  
    
    print("\nScaling Efficiency (% of single-node throughput per node):")
    for _, row in df.iterrows():
        nodes = row['nodes']
        efficiency = row['ScalingEfficiency']
        print(f"{nodes} nodes: {efficiency:.1f}%")
    
except Exception as e:
    sys.exit(1)
