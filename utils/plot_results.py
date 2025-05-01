import sys, csv
import matplotlib.pyplot as plt

if len(sys.argv) < 2:
    print("Usage: python plot_results.py <results_csv_file>")
    sys.exit(1)

csv_file = sys.argv[1]
with open(csv_file, "r") as f:
    reader = csv.DictReader(f)
    data = [row for row in reader]

if not data:
    print("No data found in CSV.")
    sys.exit(1)

x_label = list(data[0].keys())[0]
x_values = []
throughput = []
latency = []
for row in data:
    try:
        x_val = float(row[x_label])
        if x_val.is_integer():
            x_val = int(x_val)
    except:
        x_val = row[x_label]
    x_values.append(x_val)
    throughput.append(float(row["Throughput"]))
    latency.append(float(row["AvgLatency"]))

fig, ax1 = plt.subplots()
ax1.set_xlabel(x_label)
ax1.set_ylabel("Throughput (tasks/sec)", color='tab:blue')
ax1.plot(x_values, throughput, marker='o', color='tab:blue', label='Throughput')
ax1.tick_params(axis='y', labelcolor='tab:blue')
if isinstance(x_values[0], int):
    ax1.set_xticks(x_values)

ax2 = ax1.twinx() 
ax2.set_ylabel("Avg Latency (s)", color='tab:red')
ax2.plot(x_values, latency, marker='s', linestyle='--', color='tab:red', label='Avg Latency')
ax2.tick_params(axis='y', labelcolor='tab:red')

if x_label.lower().startswith("task"):
    plt.title("Weak Scaling Performance")
elif x_label.lower().startswith("node"):
    plt.title("Strong Scaling Performance")
else:
    plt.title("Performance Results")

lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
plt.legend(lines1+lines2, labels1+labels2, loc='best')

plt.tight_layout()
plt.show()
