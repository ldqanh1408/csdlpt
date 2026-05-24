# ARCHITECTURE — Container Deployment (Project 112)

> Tài liệu kiến trúc cho **tầng triển khai container** đã hiện thực hoá theo
> §9 của `pending/CONTAINER_COORDINATION.md`. Khác với
> `pending/ARCHITECTURE.md` (kiến trúc khái niệm single-process), file này
> mô tả hệ thống thật chạy trên Docker Compose với 4 node + 1 coordinator
> + 1 ingestor + Prometheus + Grafana.

---

## 1. Tổng quan

Hệ thống là một **distributed stream processor** với hai mặt phẳng tách bạch:

- **Data plane** — dòng event chảy ngang: `Ingestor → Nodes` (HTTP POST).
- **Control plane** — đồng bộ kết thúc + liveness: `Nodes ⇄ Coordinator` (HTTP).

Mỗi service là 1 container độc lập, mạng Docker bridge giả lập LAN datacenter
với RTT ~0.05–0.2 ms.

```
                    ┌────────────────────────────────────────────┐
                    │             Docker Compose Network         │
                    │                                            │
   ┌─────────┐      │   ┌──────────┐    ┌──────────────────┐    │
   │ Dataset │──────┼──▶│ Ingestor │───▶│  Nodes (4)        │    │
   │ (CSV /  │      │   │ scatter  │    │  ┌─────┐ ┌─────┐  │    │
   │ synth)  │      │   │ + RUN_ID │    │  │ N0  │ │ N1  │  │    │
   └─────────┘      │   └────┬─────┘    │  └──┬──┘ └──┬──┘  │    │
                    │        │          │  ┌─────┐ ┌─────┐  │    │
                    │        │          │  │ N2  │ │ N3  │  │    │
                    │        │          │  └──┬──┘ └──┬──┘  │    │
                    │        │ scatter_total ─┴───────┘     │    │
                    │        ▼                              │    │
                    │   ┌──────────────┐                    │    │
                    │   │ Coordinator  │◀── EOS report ─────┘    │
                    │   │ /api/wait    │◀── heartbeat 5s         │
                    │   │ run_id fence │                         │
                    │   └──────┬───────┘                         │
                    │          │                                 │
                    │          │ /metrics                        │
                    │          ▼                                 │
                    │   ┌────────────┐    ┌───────────┐          │
                    │   │ Prometheus │───▶│ Grafana   │ :3000    │
                    │   │   :9090    │    │ dashboard │          │
                    │   └────────────┘    └───────────┘          │
                    │                                            │
                    │   ┌──────────────┐                         │
                    │   │ DLQ (volume) │  per-node JSONL         │
                    │   │ dlq/node*/   │  survives restart       │
                    │   └──────────────┘                         │
                    └────────────────────────────────────────────┘
```

---

## 2. Service catalog

| Service | Image tag | Container | Port (host:cont) | Vai trò |
|---|---|---|---|---|
| `coordinator` | `csdlpt/coordinator:d1` | `csdlpt-coordinator` | `8000:8000` | Barrier + diagnosis + integrity check |
| `node0`–`node3` | `csdlpt/node:d1` | `csdlpt-node{0..3}` | `810{1..4}:8000` | Wrap `WatermarkEngine`, EOS protocol |
| `ingestor` | `csdlpt/ingestor:d1` | `csdlpt-ingestor` | (no port) | Scatter events + EOS markers, announce scatter_total |
| `prometheus` | `prom/prometheus:latest` | `csdlpt-prometheus` | `9090:9090` | Scrape `/metrics` mỗi 2s |
| `grafana` | `grafana/grafana:latest` | `csdlpt-grafana` | `3000:3000` | Visualize 5 panel dashboard |

Profile `manual` được gán cho `ingestor` → `docker compose up` không tự khởi
động nó. Gọi explicit qua `docker compose run --rm ingestor python ingest.py …`.

---

## 3. Source layout & file ownership

```
deploy/
├── node/                                  # OWNS: per-node lifecycle
│   ├── Dockerfile          Python 3.12-slim + iproute2 (cho tc netem)
│   ├── requirements.txt    fastapi, uvicorn, httpx, pydantic
│   └── main.py             [§8.13.6] /ingest, /health, /metrics
│
├── coordinator/                           # OWNS: cluster barrier
│   ├── Dockerfile          Python 3.12-slim + curl
│   ├── requirements.txt    fastapi, uvicorn, pydantic
│   └── main.py             [§8.13.6] /api/{run_started,heartbeat,completed,wait}
│
├── ingestor/                              # OWNS: scatter + run_id propagation
│   ├── Dockerfile          Python 3.12-slim + curl
│   ├── requirements.txt    httpx
│   └── ingest.py           Synthetic gen + CSV reader + scatter
│
├── docker-compose.yml                     # OWNS: topology + capabilities + volumes
├── prometheus.yml                         # OWNS: scrape config
└── grafana/
    ├── provisioning/datasources/          # OWNS: datasource auto-add
    ├── provisioning/dashboards/           # OWNS: dashboard auto-load
    └── dashboards/watermark.json          # OWNS: 5-panel dashboard

scripts/                                   # OWNS: chaos + acceptance
├── lib.sh                  dc(), wait_for_health(), assert_jq(), cleanup_cluster()
├── test_0{1..9}_*.sh       9 acceptance tests (smoke, happy, 7 chaos)
├── test_10_data_loss.sh    integrity check test
├── run_all.sh              Orchestrator + summary table
└── demo_live.sh            10-min interactive demo

Makefile                                   # OWNS: developer entry point
                            build, up, up-obs, down, smoke, happy,
                            test-all, chaos, demo, logs, clean
```

**Nguyên tắc ownership:** Mỗi file/dir có một concern duy nhất. Coordinator
không biết về DLQ. Node không biết về scatter_total. Ingestor không biết
về window logic. Tách bạch → bug ở đâu thì chỉ phải đọc một chỗ.

---

## 4. Data flow — Happy path

```
Time   Ingestor                    Node[i]                Coordinator
─────  ──────────                  ─────────              ──────────────
t=0    POST /api/run_started ──────────────────────────▶  scatter_total=-1
       (announce RUN_ID)                                  store RUN_ID

t=0+   for each event:
       hash(event_id) % 4 → target
       POST /ingest data ─────────▶ engine.process(ev)
                                    (windowing, dedup,
                                     backpressure)
       ...                          [emit /metrics every 2s]
                                                 ▲
                                                 │ scrape
                                              [Prometheus]

       POST hb every 5s ─────────────────────────────────▶ last_hb[i]=now

t=T    POST /ingest EOS ──────────▶ engine.flush() BLOCK
       (with RUN_ID)                build report (run_id, node_id,
                                    events_processed, watermark_final,
                                    completeness, dropped_late,
                                    flush_duration_ms, schema_version=1)
                                    POST /api/completed ─────▶  run_id match?
                                                                completed[i]=rpt
                                                                len==N? done.set()
                                    ◀──── 200 {ack:true} ────
                                    log_phase(ACKED)

       POST /api/run_started(N_sent) ──────────────────▶  scatter_total=N_sent

User   curl /api/wait?timeout=60 ────────────────────────▶  await done
                                                            check sum(events)
                                                              == scatter_total
                                    ◀── ALL_DONE + results ──
```

---

## 5. Control plane contracts (C1–C11)

Tham chiếu `pending/CONTAINER_COORDINATION.md` §8.13.3. Tóm tắt mapping vào code:

| # | Contract | Where enforced | Test |
|---|---|---|---|
| **C1** | Flush blocking trước report | `node/main.py:ingest()` — `engine.flush()` xong mới build report | test_02_happy |
| **C2** | run_id đi cùng EOS marker (in-band) | `ingestor/ingest.py` POST `{type:EOS, run_id}`; node lưu vào `state["run_id"]` | test_04, test_09 |
| **C3** | Coordinator reject stale run_id | `coordinator/main.py:node_completed()` check `r.run_id != RUN_ID` | test_09_stale_run |
| **C4** | Set semantics, không INCR | `coordinator/main.py:completed: dict[int, dict]` | test_08_double_report |
| **C5** | Wait-for-ACK qua body | `node/main.py:send_with_ack()` check `body.ack is True` | test_02_happy |
| **C6** | Bounded retry expo backoff | `node/main.py` `for attempt in range(5): sleep min(2**a,10)` | test_06, test_07 |
| **C7** | DLQ persist, không crash | `node/main.py:persist_dlq()` append JSONL, `replay_dlq()` on startup | test_06_partition |
| **C8** | Heartbeat kênh riêng | `node/main.py:heartbeat_loop()` async task, 5s interval | test_03, test_05 |
| **C9** | Hard timeout + diagnosis | `coordinator/main.py:wait()` `asyncio.wait_for` + DEAD/SLOW/NEVER_SEEN tag | test_03_kill_node |
| **C10** | Structured log mỗi phase | `node/main.py:log_phase()` JSON line tới stdout | (debug aid, không test gate) |
| **C11** | Integrity check | `coordinator/main.py:wait()` `sum(events_processed) == scatter_total` | test_10_data_loss |

---

## 6. State management

| State | Process | Persistence | Recovery |
|---|---|---|---|
| `WatermarkEngine.windows` / `closed_windows` | per-node in-memory | engine checkpoint file (`wm/engine.py:checkpoint()`) | `WatermarkEngine.restore()` đọc lại nếu container restart |
| `seen_ids` (dedup set) | per-node in-memory | snapshot trong checkpoint | restore từ snapshot |
| `metrics` (counters) | per-node in-memory | snapshot trong checkpoint | restore |
| `state["phase"]`, `state["run_id"]`, `state["eos_acked"]` | per-node in-memory | **không persist** — recovery dựa vào DLQ + heartbeat | re-emit phase = READY khi restart |
| DLQ pending reports | per-node | JSONL trên volume `dlq/node{N}/` | `replay_dlq()` ở startup gửi lại |
| `completed: dict[node_id, report]` | coordinator in-memory | **không persist** | nếu coordinator restart trước khi `done` → node sẽ retry và re-submit (C4 idempotent đảm bảo correctness) |
| `last_hb`, `scatter_total` | coordinator in-memory | không persist | tương tự — node sẽ tiếp tục heartbeat sau khi coordinator quay lại |

**Trade-off:** Coordinator stateless cố ý → đơn giản, idempotent ở client
side đảm bảo correctness. Nếu cần persist coordinator state cho re-attach
sau crash dài → có thể thêm Redis (theo §3 phương án B), nhưng đồ án không cần.

---

## 7. Port + network layout

| Service | Internal port | Host port | Protocol | Purpose |
|---|---|---|---|---|
| coordinator | 8000 | 8000 | HTTP/JSON | `/health`, `/api/*`, `/metrics` |
| node0 | 8000 | 8101 | HTTP/JSON | Same — namespaced bởi container |
| node1 | 8000 | 8102 | HTTP/JSON | |
| node2 | 8000 | 8103 | HTTP/JSON | |
| node3 | 8000 | 8104 | HTTP/JSON | |
| prometheus | 9090 | 9090 | HTTP | Scrape UI |
| grafana | 3000 | 3000 | HTTP | Dashboard UI |

**Service discovery**: Docker DNS — container gọi `http://node0:8000`,
`http://coordinator:8000`. Không dùng hard-coded IP, không cần Consul/etcd.

**Capabilities**: Mỗi node container có `cap_add: [NET_ADMIN]` → cho phép
`tc qdisc add ... netem` để inject network delay/loss trong chaos test (§9.6).

---

## 8. Observability stack

```
Node /metrics              Coordinator /metrics
(per-node text exposition) (cluster aggregates)
        │                          │
        │       Prometheus         │
        └──────▶ scrape 2s ◀───────┘
                  │
                  ▼
              [Time Series DB]
                  │
                  ▼
               Grafana
        ┌─────────┴─────────┐
        ▼                   ▼
  5-panel dashboard      Ad-hoc query
```

**Metrics exposed:**

| Metric | Type | Source | Use case |
|---|---|---|---|
| `wm_watermark{node}` | Gauge | node | Watermark advancement per node |
| `wm_events_total{node}` | Counter | node | Throughput via `rate()` |
| `wm_events_unique{node}` | Counter | node | After dedup |
| `wm_completeness{node}` | Gauge | node | % data đã đóng window |
| `wm_dropped_late{node}` | Counter | node | Late events trên watermark |
| `wm_duplicates{node}` | Counter | node | R4 dedup count |
| `wm_backpressure{node}` | Counter | node | R4 backpressure drops |
| `coord_received` | Gauge | coord | Số node đã EOS-acked |
| `coord_expected` | Gauge | coord | Tổng nodes (= N) |
| `coord_heartbeats_seen` | Gauge | coord | Số node có ít nhất 1 hb |
| `coord_done{run_id}` | Gauge | coord | 1 khi `done_event.set()` |

**Dashboard panels** (`grafana/dashboards/watermark.json`):

1. **Watermark per node** (timeseries) — thấy node nào tụt watermark.
2. **Throughput** (timeseries `rate`) — thấy node nào chết khi line tụt về 0.
3. **Completeness gauge** — % completeness realtime.
4. **Coordinator status** (stat) — received / expected.
5. **Late-dropped** (timeseries) — bao nhiêu event đến trễ.

---

## 9. Failure mode matrix

| Failure | Detection | Mitigation | Test coverage |
|---|---|---|---|
| Node chết (SIGKILL) | Heartbeat gap > 15s → DEAD diagnosis | Coordinator timeout returns missing list | test_03_kill_node |
| Node chậm (network delay) | Heartbeat vẫn đến, flush chậm | Tag SLOW thay vì DEAD; vẫn aggregate khi đến | test_05_slow_node |
| Network partition | POST `/completed` exception → retry | Exponential backoff 5 lần → DLQ persist → replay khi reconnect | test_06_partition |
| Coordinator chết | Node POST connection refused → retry | Khi coordinator restart trong < 60s, node retry tiếp tục → ACKED | test_07_coordinator_down |
| Duplicate report | Coordinator dedup qua `completed[node_id]` | Set semantics, `len(completed)` không tăng | test_08_double_report |
| Stale run | `run_id` mismatch | Reject với `ack:false, reason:stale_run_id` | test_09_stale_run |
| Data loss on the wire | `sum(events_processed) < scatter_total` | Status đổi từ ALL_DONE → DATA_LOSS với `missing_events > 0` | test_10_data_loss |
| Disk full (DLQ) | Append fails → exception propagates | Node tiếp tục chạy; mất 1 report cuối — chấp nhận | (chưa cover; future work) |
| Slow consumer (backpressure) | `queue_len > max_queue` | Engine drop có kiểm soát, đếm `wm_backpressure` | (cover by `wm/demos.py` single-process) |

---

## 10. Build & deploy workflow

```
make build             docker compose build (3 images)
  │
  ├─ deploy/node/Dockerfile       → csdlpt/node:d1        (~180MB)
  ├─ deploy/coordinator/Dockerfile → csdlpt/coordinator:d1 (~150MB)
  └─ deploy/ingestor/Dockerfile   → csdlpt/ingestor:d1     (~140MB)

make up-obs            docker compose up -d coord + 4 nodes + prom + grafana
  │
  ├─ healthcheck loop until coordinator green
  ├─ nodes start with depends_on: service_healthy
  └─ prometheus + grafana attach to network

make test-all          scripts/run_all.sh
  │
  ├─ test_01_smoke      D1 gate
  ├─ test_02_happy      D2 gate
  ├─ test_0{3..10}      D3-D5 chaos (8 tests)
  └─ summary table

make clean             down -v + rmi local + rm dlq/
```

---

## 11. Trade-offs ghi chú trong design

1. **HTTP vs gRPC**: Chọn HTTP vì `curl`-able demo. Trade-off: ~3 ms overhead/call thay vì 0.1 ms. Chấp nhận vì 4-node, batch-style — overhead < 0.5% pipeline. Đã giải thích trong §3 và §6 của CONTAINER_COORDINATION.md.

2. **In-memory coordinator state**: Đơn giản, không cần Redis/etcd. Trade-off: nếu coordinator crash quá lâu (>60s), node sẽ exhaust retry và rơi vào DLQ. Acceptable cho demo; production sẽ thêm Redis Sentinel.

3. **Per-node DLQ trên Docker volume**: Survives container restart. Trade-off: DLQ không được replicate — nếu host die thì DLQ mất. Acceptable cho single-host demo.

4. **Synthetic data generator trong ingestor**: Cho phép chạy không cần NASA dataset. Trade-off: kết quả demo dùng generator có thể không representative bằng dữ liệu thật. Mitigation: `ingest.py --csv path/to/nasa.csv` chấp nhận data thật.

5. **`profiles: ["manual"]` cho ingestor**: `docker compose up` không auto-start nó. Trade-off: thêm 1 lệnh khi demo (`make happy` đã wrap). Lợi: tránh ingest tự động lúc test chaos cần cluster idle.

6. **`cap_add: NET_ADMIN`**: Cho phép chaos test inject tc netem. Trade-off: privilege cao hơn cần thiết cho production. Mitigation: dùng profile khác cho production deploy (không thuộc scope đồ án).

---

## 12. Mapping về OBJECTIVES.md rubric

| Rubric | Hiện thực trong container architecture |
|---|---|
| **R1 — Event-time windowing** | `wm/engine.py` không sửa; wrapper `deploy/node/main.py` chỉ forward event vào `engine.process()` |
| **R2 — Distributed checkpoint** | DLQ JSONL trên Docker volume + engine checkpoint file; recovery qua `replay_dlq()` startup hook |
| **R3 — High-res latency** | `flush_duration_ms` trong report (chính xác đến ms); engine giữ `perf_counter_ns` cho per-event latency |
| **R4 — Robustness** | C1–C11 contract đầy đủ + 8 chaos test verify từng failure mode trên hệ thống thật (không phải single-process mô phỏng) |

→ Triển khai container hoàn thiện phần **R2 distributed** và **R4 node-death**
mà single-process `app.py` không demo được. Đây là điểm khác biệt then chốt
để đạt **Excellent** thay vì chỉ Good cho 2 tiêu chí đó.

---

## 13. Liên kết tới tài liệu khác

| Cần xem | File |
|---|---|
| Đề bài + rubric | `docs/OBJECTIVES.md` |
| Kiến trúc khái niệm (single-process) | `pending/ARCHITECTURE.md` |
| Lý thuyết coordination + protocol contract | `pending/CONTAINER_COORDINATION.md` §1–§8 |
| Implementation playbook D1–D7 | `pending/CONTAINER_COORDINATION.md` §9 |
| Hạ tầng — vì sao chọn HTTP, không Kafka | `docs/INFRASTRUCTURE.md` |
| Tối ưu hiệu năng | `docs/PERFORMANCE.md` |
| Báo cáo Strict vs Heuristic | `pending/REPORT.md` |
