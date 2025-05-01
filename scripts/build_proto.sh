#!/usr/bin/env bash
python -m grpc_tools.protoc -I ./proto \
  --python_out=./src \
  --grpc_python_out=./src \
  proto/dwse.proto
echo "✔ gRPC code generated into ./src"
