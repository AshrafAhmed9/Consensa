#!/usr/bin/env bash
# demo.sh -- brings up a real 3-node Consensa cluster (three real OS processes), drives
# real gRPC traffic against it, kills a node's process mid-demo, and shows the cluster
# keeps working. Everything here is the real binary and real network calls -- nothing is
# mocked or simulated for this script.
set -euo pipefail
cd "$(dirname "$0")"

DATA_DIR="$(mktemp -d)"
PIDS=()

cleanup() {
  echo
  echo "--- shutting down and cleaning up ---"
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$DATA_DIR"
}
trap cleanup EXIT

section() { echo; echo "=== $1 ==="; }

section "building the real consensa binary and demo client"
go build -o "$DATA_DIR/consensa" ./cmd/consensa
go build -o "$DATA_DIR/democlient" ./cmd/democlient

PEERS="1=127.0.0.1:9001,2=127.0.0.1:9002,3=127.0.0.1:9003"
GRPC_ADDRS="127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083"

section "starting 3 real node processes (real TCP Raft, real on-disk storage)"
for id in 1 2 3; do
  raft_port=$((9000 + id))
  grpc_port=$((8080 + id))
  metrics_port=$((9090 + id))
  "$DATA_DIR/consensa" \
    -id "$id" -peers "$PEERS" \
    -data-dir "$DATA_DIR/node$id" \
    -grpc-listen ":$grpc_port" -metrics-listen ":$metrics_port" \
    -dimension 4 -tick-interval 20ms \
    > "$DATA_DIR/node$id.log" 2>&1 &
  PIDS+=($!)
  echo "  node $id: pid $! · raft :$raft_port · grpc :$grpc_port · metrics :$metrics_port"
done

echo "  waiting for the cluster to elect a leader..."
sleep 2

section "upserting 3 vectors and searching (real Raft consensus + HNSW index)"
"$DATA_DIR/democlient" --addrs "$GRPC_ADDRS" --action upsert-and-search

section "committing a transaction across two independent Raft-replicated ranges"
# A real, documented limitation, not a demo bug: this binary never forwards a write to
# another process's leader (docs/notes/05-api.md), and a cross-range transaction only
# commits through whichever ONE process is currently leading BOTH KV ranges at once. The
# vector index and the two KV ranges are 3 independently-elected Raft groups; usually the
# same process ends up leading all three, but that alignment isn't guaranteed, and this
# step can occasionally take a while (or not converge before this demo's own patience
# runs out) if it doesn't. That's a real property of the current design worth seeing, not
# something worth hiding behind a longer sleep -- see README.md's "what's not done yet".
if "$DATA_DIR/democlient" --addrs "$GRPC_ADDRS" --action txn --timeout 30s; then
  :
else
  echo "  (didn't converge in time on this run -- see the note above; continuing the demo)"
fi

section "killing node 3's real process"
kill "${PIDS[2]}"
echo "  node 3 (pid ${PIDS[2]}) killed -- 2 of 3 replicas remain"
sleep 2

section "the surviving majority still answers searches correctly"
"$DATA_DIR/democlient" --addrs "127.0.0.1:8081,127.0.0.1:8082" --action search-only

section "restarting node 3 -- it recovers from its own on-disk Raft log"
"$DATA_DIR/consensa" \
  -id 3 -peers "$PEERS" \
  -data-dir "$DATA_DIR/node3" \
  -grpc-listen ":8083" -metrics-listen ":9093" \
  -dimension 4 -tick-interval 20ms \
  > "$DATA_DIR/node3-restart.log" 2>&1 &
PIDS[2]=$!
echo "  node 3 restarted: pid ${PIDS[2]}"
sleep 4
"$DATA_DIR/democlient" --addrs "127.0.0.1:8083" --action search-only --timeout 20s

section "live metrics (real Prometheus text format, scraped from a running process)"
curl -s "http://127.0.0.1:9091/metrics" | grep -E "^consensa_" || true

echo
echo "Done. See README.md for the architecture diagram and docs/correctness.md for what's proven."
