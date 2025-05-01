#!/bin/bash
# Start a 5-node DWSE cluster on ports 50050-50054

set -e

PEERS="0:localhost:50050,1:localhost:50051,2:localhost:50052,3:localhost:50053,4:localhost:50054"

echo "=== Starting DWSE 5-node cluster ==="

# Kill any existing nodes
for PORT in 50050 50051 50052 50053 50054; do
    lsof -t -i :$PORT | xargs -r kill -9 2>/dev/null || true
done
pkill -f "src.node" 2>/dev/null || true
sleep 1

# Start nodes
for i in 0 1 2 3 4; do
    PORT=$((50050 + i))
    python -m src.node \
        --id $i \
        --port $PORT \
        --peers "$PEERS" \
        --mode consistency \
        --advanced-weight \
        --enable-stealing \
        --log-level INFO > node${i}.log 2>&1 &
    echo "Started Node $i on port $PORT (pid $!)"
done

echo ""
echo "Waiting for cluster to initialize..."
sleep 4

echo ""
echo "=== Cluster ready ==="
echo "Leader: Node 0 (port 50050)"
echo "Submit tasks:  python submit_tasks.py --leader-port 50050 --num-tasks 100 --payload 200"
echo "Benchmark:     python bench_client.py  --leader-port 50050 --num-tasks 1000 --payload 200 --concurrency 150"
echo "Logs:          tail -f node0.log"
echo "Stop cluster:  ./stop_nodes.sh"
