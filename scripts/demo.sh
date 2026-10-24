#!/usr/bin/env bash
set -euo pipefail

echo "======================================================================"
echo "  RaftKV — Distributed Consensus Key-Value Store Real Live Demo"
echo "======================================================================"

mkdir -p bin
go build -o bin/raftkv-server ./cmd/raftkv-server
go build -o bin/raftkv-cli ./cmd/raftkv-cli

DEMO_DIR="data/demo"
rm -rf "$DEMO_DIR"
mkdir -p "$DEMO_DIR/node1" "$DEMO_DIR/node2" "$DEMO_DIR/node3"

cat <<EOF > "$DEMO_DIR/node1.yaml"
id: "node-1"
data_dir: "$DEMO_DIR/node1"
address: "127.0.0.1:56051"
min_election_timeout: "150ms"
max_election_timeout: "300ms"
heartbeat_interval: "50ms"
peers:
  node-2: "127.0.0.1:56052"
  node-3: "127.0.0.1:56053"
EOF

cat <<EOF > "$DEMO_DIR/node2.yaml"
id: "node-2"
data_dir: "$DEMO_DIR/node2"
address: "127.0.0.1:56052"
min_election_timeout: "150ms"
max_election_timeout: "300ms"
heartbeat_interval: "50ms"
peers:
  node-1: "127.0.0.1:56051"
  node-3: "127.0.0.1:56053"
EOF

cat <<EOF > "$DEMO_DIR/node3.yaml"
id: "node-3"
data_dir: "$DEMO_DIR/node3"
address: "127.0.0.1:56053"
min_election_timeout: "150ms"
max_election_timeout: "300ms"
heartbeat_interval: "50ms"
peers:
  node-1: "127.0.0.1:56051"
  node-2: "127.0.0.1:56052"
EOF

./bin/raftkv-server --config "$DEMO_DIR/node1.yaml" &
P1=$!
./bin/raftkv-server --config "$DEMO_DIR/node2.yaml" &
P2=$!
./bin/raftkv-server --config "$DEMO_DIR/node3.yaml" &
P3=$!

cleanup() {
  echo "Cleaning up processes..."
  kill -9 $P1 $P2 $P3 2>/dev/null || true
  rm -rf "$DEMO_DIR"
}
trap cleanup EXIT

sleep 2
ENDPOINTS="127.0.0.1:56051,127.0.0.1:56052,127.0.0.1:56053"

./bin/raftkv-cli --endpoints="$ENDPOINTS" put account:123 1000
./bin/raftkv-cli --endpoints="$ENDPOINTS" get account:123
./bin/raftkv-cli --endpoints="$ENDPOINTS" cas account:123 1000 1250

echo "Killing node 1..."
kill -9 $P1
sleep 1

./bin/raftkv-cli --endpoints="127.0.0.1:56052,127.0.0.1:56053" put account:123 2000
./bin/raftkv-cli --endpoints="127.0.0.1:56052,127.0.0.1:56053" get account:123

echo "Demo completed successfully!"
