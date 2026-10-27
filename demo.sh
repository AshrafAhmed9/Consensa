#!/usr/bin/env bash
# One-command demo of Consensa. Run:  ./demo.sh
#
# It starts a real 3-node cluster, stores and searches vectors, kills a node and brings it
# back, then runs the linearizability test, the recall benchmark and the TLA+ model check.
# Needs: go, python3 with numpy.
# For the TLA+ step also set TLA_JAR=/path/to/tla2tools.jar (and have java installed).
# Without it that step is skipped.
set -euo pipefail
cd "$(dirname "$0")"

WORK="$(mktemp -d)"
PIDS=()
cleanup() {
  for pid in "${PIDS[@]:-}"; do kill -9 "$pid" 2>/dev/null || true; done
  rm -rf "$WORK"
}
trap cleanup EXIT

step() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

# Print N free TCP ports on one line.
free_ports() {
  python3 - "$1" <<'PY'
import socket, sys
socks = [socket.socket() for _ in range(int(sys.argv[1]))]
for s in socks:
    s.bind(("127.0.0.1", 0))
print(" ".join(str(s.getsockname()[1]) for s in socks))
for s in socks:
    s.close()
PY
}
# Three ports for Raft traffic between nodes, three for client gRPC.
read -r R1 R2 R3 G1 G2 G3 <<<"$(free_ports 6)"
PEERS="1=127.0.0.1:$R1,2=127.0.0.1:$R2,3=127.0.0.1:$R3"
ADDRS="127.0.0.1:$G1,127.0.0.1:$G2,127.0.0.1:$G3"
GRPC=("" "127.0.0.1:$G1" "127.0.0.1:$G2" "127.0.0.1:$G3")

step "Build"
go build -o "$WORK/consensa" ./cmd/consensa
go build -o "$WORK/democlient" ./cmd/democlient
echo "  built the node binary and a small client"

# start_node <id>: run one node in the background and remember its process ID.
start_node() {
  "$WORK/consensa" -id "$1" -peers "$PEERS" -data-dir "$WORK/node$1" \
    -grpc-listen "${GRPC[$1]}" -dimension 4 -tick-interval 20ms >"$WORK/node$1.log" 2>&1 &
  PIDS[$1]=$!
  disown "$!" # keeps the shell from printing "Killed" when we stop nodes
}

step "1. Start a 3-node cluster (three separate OS processes, Raft over TCP)"
for i in 1 2 3; do start_node "$i"; done
sleep 2
"$WORK/democlient" status -addrs "$ADDRS"

step "2. Store vectors. Each write needs a majority of the 3 nodes to log it."
echo "  animals sit near (1,0,0,0), vehicles near (0,1,0,0), fruit near (0,0,1,0)"
for entry in "cat:1,0.1,0,0" "dog:0.9,0,0.1,0" "lion:1,0,0,0.1" \
             "car:0,1,0.1,0" "bus:0.1,0.9,0,0" "truck:0,1,0,0.1" \
             "apple:0,0,1,0.1" "pear:0.1,0,0.9,0"; do
  "$WORK/democlient" upsert -addrs "$ADDRS" -id "${entry%%:*}" -vec "${entry#*:}"
done

step "3. Search from every node. Each node answers from its own copy of the index."
for i in 1 2 3; do
  echo "  node $i, nearest 3 to (1,0,0,0):"
  "$WORK/democlient" search -addr "${GRPC[$i]}" -vec 1,0,0,0 -k 3 -expect cat | sed 's/^/  /'
done

step "4. Kill node 3 with SIGKILL. The other two still form a majority, so writes continue."
kill -9 "${PIDS[3]}"
"$WORK/democlient" upsert -addrs "${GRPC[1]},${GRPC[2]}" -id tiger -vec 0.95,0,0,0.05

step "5. Restart node 3 on its old data directory. It replays its own Raft log."
start_node 3
echo "  node 3, nearest 3 to (0.95,0,0,0.05), waiting for it to catch up:"
"$WORK/democlient" search -addr "${GRPC[3]}" -vec 0.95,0,0,0.05 -k 3 -expect tiger | sed 's/^/  /'

step "6. Linearizability under faults: 30 seeded runs of node isolations, history checked"
python3 -m harness.torture.cli run --nemesis partition,crash --seeds 30
echo "  all 30 histories are linearizable"

step "7. Search quality: HNSW vs exact brute-force search (5,000 synthetic vectors)"
python3 harness/bench/generate_dataset.py --num-vectors 5000 --dimension 128 \
  --num-queries 200 --num-clusters 20 --seed 1 --out "$WORK/dataset.json" >/dev/null
python3 harness/bench/run_recall_benchmark.py --dataset "$WORK/dataset.json" --repo-root . | sed -n '3,8p'

step "8. TLA+ model check of quorum intersection and range ownership"
if [ -z "${TLA_JAR:-}" ] || [ ! -f "$TLA_JAR" ]; then
  echo "  skipped: set TLA_JAR=/path/to/tla2tools.jar to run it"
elif ! command -v java >/dev/null; then
  echo "  skipped: java is not installed"
else
  make -s -C specs check TLA_JAR="$TLA_JAR" 2>&1 | grep -E "^---|No error|Error" || true
fi

step "Done"
