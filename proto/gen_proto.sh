#!/usr/bin/env bash
set -euo pipefail

# Generate stubs from storage.proto
# Make sure that ytstorage is identical to one from ytstorage service!!

cd "$(dirname "$0")"

source ../.venv/bin/activate 

python -m grpc_tools.protoc \
  -I . \
  --python_out=. \
  --grpc_python_out=. \
  ytstorage.proto

sed -i 's/^import ytstorage_pb2 as ytstorage__pb2/from . import ytstorage_pb2 as ytstorage__pb2/' ytstorage_pb2_grpc.py

echo "Generated: ytstorage_pb2.py ytstorage_pb2_grpc.py in $(pwd)"
