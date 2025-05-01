#!/bin/bash
# Stop all DWSE cluster nodes

echo "=== Stopping DWSE cluster ==="

for PORT in 50050 50051 50052 50053 50054; do
    lsof -t -i :$PORT | xargs -r kill -9 2>/dev/null || true
done
pkill -f "src.node" 2>/dev/null || true

rm -f node*.log

echo "Cluster stopped."
