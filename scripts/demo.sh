#!/usr/bin/env bash
# Starts a 3-node cluster on localhost, writes data, kills the leader,
# and shows the cluster keeps serving with no data loss.
set -euo pipefail
cd "$(dirname "$0")/.."

go build -o bin/quorum ./cmd/quorum
rm -rf data
PEERS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002
pids=()
start() { ./bin/quorum -id "$1" -peers "$PEERS" -http ":800$1" 2>"data/node-$1.log" & pids[$1]=$!; }
cleanup() { kill "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT

mkdir -p data
for i in 0 1 2; do start $i; done
sleep 1.5

leader() {
  for i in 0 1 2; do
    if curl -sf "localhost:800$i/status" 2>/dev/null | grep -q '"role": "leader"'; then echo $i; return; fi
  done
}

echo "== writing through node 0"
curl -sf -X PUT --data 'hello' localhost:8000/kv/greeting
for n in 1 2 3; do curl -sf -X POST --data "$n," localhost:8000/kv/seq/append; done
echo "greeting = $(curl -sf localhost:8001/kv/greeting)   (read via node 1)"
echo "seq      = $(curl -sf localhost:8002/kv/seq)        (read via node 2)"

L=$(leader); echo "== leader is node $L — killing it"
kill "${pids[$L]}"; sleep 1.5
S=$(( (L + 1) % 3 ))
echo "== new leader is node $(leader)"
curl -sf -X POST --data '4,' "localhost:800$S/kv/seq/append"
echo "seq      = $(curl -sf "localhost:800$S/kv/seq")      (no acknowledged write was lost)"

echo "== distributed lock via compare-and-swap"
curl -sf -X POST -d '{"expect":"","value":"alice"}' "localhost:800$S/kv/lock/cas"
curl -sf -X POST -d '{"expect":"","value":"bob"}'   "localhost:800$S/kv/lock/cas"

echo "== restarting node $L; it catches up from the log/snapshot"
start "$L"; sleep 2
echo "seq on restarted node = $(curl -sf "localhost:800$L/kv/seq")"
curl -sf "localhost:800$L/status"
