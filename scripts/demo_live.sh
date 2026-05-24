#!/usr/bin/env bash
# Live 10-minute demo per §8.8 of CONTAINER_COORDINATION.md.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

source "$SCRIPT_DIR/lib.sh"

pause() {
    echo ""
    echo ">>> $* (Enter để tiếp tục)"
    read -r
}

trap cleanup_cluster EXIT

echo "==> [0:00] Bring up full stack (coord + 4 nodes + Prometheus + Grafana)"
dc up -d coordinator node0 node1 node2 node3 prometheus grafana
wait_all_healthy
echo "   Grafana:    http://localhost:3000"
echo "   Prometheus: http://localhost:9090"

pause "Mở Grafana trên trình duyệt, vào dashboard 'Watermark Tracker — Realtime'"

echo "==> [1:00] Bắn 10K synthetic events"
dc run --rm ingestor python ingest.py --n 10000 --seed 7 &
INGESTOR_PID=$!
sleep 3

pause "Throughput nên thấy ở 4 node trên Grafana"

echo "==> [2:00] Kill node2 giữa stream"
docker kill csdlpt-node2 >/dev/null
pause "Node2 throughput tụt về 0; coord diagnosis sau 15s sẽ chuyển sang DEAD"

curl -sf "http://localhost:8000/api/wait?timeout=5" | jq '.'

echo "==> [4:00] Revive node2"
dc up -d node2
wait_for_health "http://localhost:8103/health" 30
pause "Node2 trở lại; phase READY"

echo "==> [6:00] Inject 100ms delay vào node1"
docker exec csdlpt-node1 tc qdisc add dev eth0 root netem delay 100ms || true
pause "Latency báo cáo node1 tăng; coord vẫn coi alive (slow != dead)"

wait $INGESTOR_PID 2>/dev/null || true

echo "==> [8:00] Bắn lại ingestor để cluster đạt ALL_DONE"
dc run --rm ingestor python ingest.py --n 5000 --seed 42

echo "==> [9:00] Kết quả cuối"
curl -sf "http://localhost:8000/api/wait?timeout=30" | jq '.'

pause "Demo xong. Down cluster?"
echo "==> [10:00] Cleanup"
