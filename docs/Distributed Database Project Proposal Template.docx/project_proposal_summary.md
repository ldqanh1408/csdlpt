# Distributed Database Project Proposal

This document summarizes the Distributed Database Project Proposal compiled from the HTML template, representing **Topic #112: Distributed Watermark Tracker ("Log Delay Compensator")**.

---

* **Due Date (Hạn nộp):** Week 3 — 15/06/2026
* **Project ID & Category (Mã đề tài & Phân loại):** Topic #112: Distributed Watermark Tracker — Category 3

---

## 1. Project Identity (Thông tin Dự án)
* **Team Name (Tên nhóm):** StreamPioneers
* **Team Members (Thành viên nhóm):**
  * Lê Đăng Quỳnh Anh (MSSV: `[Insert ID 1]` | Email: `[Insert Email 1]`)
  * Nguyễn Văn An (MSSV: `[Insert ID 2]` | Email: `[Insert Email 2]`)
* **Project Title (Tên đề tài):**
  * *Hiện thực hóa Hệ thống định thời logic phân tán và so sánh hiệu quả giữa chiến lược Strict Watermark và Heuristic Watermark với Dead-Letter Queue (DLQ).*

---

## 2. Objective & Problem Statement (Mục tiêu & Đặc tả bài toán)
* **The "Why" (Lý do thực hiện đề tài):**
  Distributed stream processing engines face a fundamental tension between **result completeness** and **output latency** when handling out-of-order events. The watermark is the primary mechanism for bounding this trade-off. In windowed aggregations, the watermark determines when a window is deemed "closed" and its results emitted.
  We test and evaluate two main strategies:
  1. **Strict Watermark**: Guarantees 100% data completeness (0% loss) via Upstream Heartbeat Punctuation, but is vulnerable to stragglers.
  2. **Heuristic Watermark**: Adapts safety margins based on the observed lateness distribution using a DDSketch estimator and routes late events to a Dead-Letter Queue (DLQ) path for delayed reconciliation, optimizing for low latency.

* **Core Logic (Thuật toán cốt lõi):**
  * **Strict Path**: Uses punctuation tokens from the source to advance local watermarks. The coordinator computes the global minimum watermark:
    $$\large \boxed{W_{\text{global}}(t) = \min_{\forall P_k \in \text{Active}} LW_i(P_k)}$$
  * **Heuristic Path**: Tracks latency $\ell = T_{\text{arrival}} - T_{\text{event}}$ with log-scale buckets in a sliding DDSketch. Local watermarks advance as:
    $$\large \boxed{W_{\text{heur}}(i, p) = \max_{j < i} T_{\text{event\_max}} - Q_p(\{\ell\})}$$
    where $Q_p$ is the $p$-th percentile. Late events are routed to `late_logs_dlq` for historical window corrections.

---

## 3. Dataset Specification (Đặc tả Tập dữ liệu)
* **Source (Nguồn dữ liệu):**
  * NYC TLC Yellow Taxi Trip Records (`dataset/nyc_taxi_events_full.csv`)
* **Size (Dung lượng tập dữ liệu):**
  * NYC Taxi Events: $\approx 159$ MB, 2,961,423 events from 260 hosts.
* **Cơ chế nén thời gian phù hợp với Engine (Time Compression Design):**
  * Do tập dữ liệu gốc trải dài trọn vẹn 31 ngày thực tế (khoảng 2,678,368 giây), việc chạy mô phỏng trực tiếp theo thời gian thực tế sẽ không khả thi. Hệ thống áp dụng kỹ thuật **Nén thời gian (Time Compression)** với hệ số nén $\text{DIV} = 60$.
  * Mốc thời gian tham chiếu: $t_{\text{ref}} = \min(\text{pickup\_timestamps})$
  * Công thức ánh xạ:
    $$\large \boxed{T_{\text{event}} = \frac{\text{pickup\_timestamp} - t_{\text{ref}}}{60}}$$
    $$\large \boxed{T_{\text{arrival}} = \frac{\text{dropoff\_timestamp} - t_{\text{ref}}}{60}}$$
  * Đặc tính quan trọng:
    $$\large \boxed{\ell = T_{\text{arrival}} - T_{\text{event}} = \frac{\text{trip\_duration}}{60}}$$
  * Phép nén này thu nhỏ thời lượng mô phỏng của toàn bộ 31 ngày dữ liệu thực tế xuống còn **12.4 giờ** (giảm 60 lần), trong khi vẫn giữ nguyên các phân phối bất tuần tự thực tế:
    * Thời gian trễ trung vị (p50): 12 phút thực tế $\rightarrow$ **11.63 giây** mô phỏng.
    * Thời gian trễ phân vị p95: 38 phút thực tế $\rightarrow$ **37.78 giây** mô phỏng.
    * Thời gian trễ phân vị p99: 60 phút thực tế $\rightarrow$ **59.70 giây** mô phỏng.
    * Trễ cực đại (Max lateness): ~5 giờ thực tế $\rightarrow$ **359.73 giây** mô phỏng.
  * Cơ chế này giúp các Worker thực hiện benchmark đánh đổi hiệu năng (completeness vs wait time) cực kỳ nhanh chóng mà vẫn phản ánh chính xác hành vi thực tế.
* **Schema (Cấu trúc dữ liệu):**
  * `host` (string): Định danh máy chủ nguồn phát (được ánh xạ từ thông tin vùng đón/trả).
  * `event_time` (float): Timestamp logic sinh sự kiện ($T_{\text{event}}$, đơn vị: giây).
  * `arrival_time` (float): Timestamp vật lý Worker nhận được sự kiện ($T_{\text{arrival}}$, đơn vị: giây).
  * `response_bytes` (int): Dung lượng dữ liệu (giả lập trường thông tin phản hồi của web server).
  * `status` (int): Mã trạng thái giao dịch / phản hồi.
* **Fragmentation Strategy (Chiến lược phân mảnh):**
  * Horizontal Hash-based partitioning on `host`:
    $$\large \boxed{\text{PartitionID} = \text{hash}(host) \bmod 12}$$
  * 12 partitions are distributed across 4 worker nodes:
    * **Worker 0 (node0)**: partitions 0, 1, 2
    * **Worker 1 (node1)**: partitions 3, 4, 5
    * **Worker 2 (node2)**: partitions 6, 7, 8
    * **Worker 3 (node3)**: partitions 9, 10, 11

---

## 4. System Architecture (Kiến trúc Hệ thống)
Hệ thống hoạt động dựa trên cấu hình phân tán đồng bộ qua tệp cấu hình `deploy/docker-compose.yml`. Mỗi thực thể tiến trình được khởi chạy bằng lệnh `python3 -m refactor.run --role <role> --mode <mode>`. Hệ thống được phân rã thành các Domain thành phần và vai trò cụ thể như dưới đây.

### 4.1 Phân rã kiến trúc theo các Domain thành phần
* **Domain 1: Luồng nạp dữ liệu từ Ingestor đến các Worker Nodes (Data Ingestion Domain)**
  * *Ingestor*: Đọc hành trình taxi từ CSV, gán nhãn thời gian sự kiện logic $T_{\text{event}}$, gửi các bản ghi sự kiện kèm Punctuation Tokens vào Kafka topic `events`.
  * *Kafka Broker*: Tổ chức dữ liệu trên 12 partitions của topic `events`.
  * *Worker Layer*: 4 Worker Nodes poll dữ liệu từ partitions Kafka tương ứng, sắp xếp bằng Min-Heap trước khi tính toán.
  * *Output*: Khi cửa sổ chốt, Worker ghi kết quả cửa sổ (`WindowResult`) vào Kafka topic `strict_results` hoặc `audit_results`.
  
  ```mermaid
  flowchart TB
      classDef ingest fill:#2d6a4f,stroke:#1b4332,color:#fff,stroke-width:3px,font-size:16px,font-weight:bold
      classDef kafka fill:#e76f51,stroke:#c1440e,color:#fff,stroke-width:3px,font-size:16px,font-weight:bold
      classDef worker fill:#264653,stroke:#1d3557,color:#fff,stroke-width:3px,font-size:16px,font-weight:bold

      subgraph ingestLayer["🟢 Ingest Layer"]
          ING["Ingestor<br/>CSV -> T_event -> produce"]:::ingest
      end

      KAFKA["Kafka . 12 partitions<br/>topics: events . strict_results . audit_results"]:::kafka

      subgraph workers["🔵 Worker Layer -- 4 node . 12 partition"]
          N0["node0 . P0,1,2"]:::worker
          N1["node1 . P3,4,5"]:::worker
          N2["node2 . P6,7,8"]:::worker
          N3["node3 . P9,10,11"]:::worker
      end

      ING -->|"(1) Produce logs & punctuations"| KAFKA
      KAFKA -->|"(2) Poll events"| N0 & N1 & N2 & N3
      N0 & N1 & N2 & N3 -->|"(3) Emit window results"| KAFKA
  ```

  **Chi tiết các luồng giao tiếp trong Domain 1 (Data Ingestion):**

  | Luồng giao tiếp | Hướng | Giao thức / Cổng | Nội dung & Tần suất |
  |:---|:---|:---|:---|
  | Produce log + punctuation | Ingestor → Kafka `events` | Kafka Producer (`acks=all`) | Bản ghi `LogEvent` (đã gán `T_event`) kèm Punctuation Token (`T_commit`); phân mảnh theo `hash(host) % 12`. Khi partition nhàn rỗi, phát Empty Punctuation mỗi ~1s (chỉ chế độ Strict). |
  | Poll events | Kafka `events` → Worker (N0–N3) | Kafka Consumer (pull, consumer group) | Mỗi Worker poll các partition được gán, nạp vào Bounded Min-Heap (sắp theo `event_time`); cơ chế backpressure `pause()/resume()` chống tràn RAM. |
  | Emit window results | Worker → Kafka `strict_results` / `audit_results` | Kafka Producer (`acks=all`) | Phát `WindowResult` khi `window_end ≤ W`; idempotent theo `window_id`; nhân bản song song sang Critical Audit Sink. |

* **Domain 2: Luồng điều phối Strict Watermark (Strict Coordination Domain)**
  * *Local Watermark*: Các Worker Node báo cáo Watermark cục bộ $LW_i(P_k)$ thông qua heartbeat về Coordinator.
  * *HA Coordinator Plane*: Cụm 3 instance chạy đồng bộ. Hệ thống hỗ trợ hai chế độ bầu chọn và quản lý Leader: (1) Chế độ Raft nhúng (Embedded Raft) tự cử Leader và nhân bản trạng thái, hoặc (2) Chế độ ZooKeeper (nếu `ZK_ENSEMBLE` được cấu hình) thông qua việc tranh chấp khóa phân tán tại `/csdlpt/coordinator-lock` và ghi leader ID lên node tạm `/csdlpt/coordinator-leader`. Coordinator Leader tiếp nhận nhịp tim từ các Worker, tính toán Watermark toàn cục $W_{\text{global}} = \min(LW_i)$ trên toàn bộ partitions hoạt động và broadcast ngược lại cho các Worker Nodes để đóng cửa sổ.
  
  ```mermaid
  flowchart TB
      classDef worker fill:#264653,stroke:#1d3557,color:#fff,stroke-width:3px,font-size:16px,font-weight:bold
      classDef strict fill:#7b2cbf,stroke:#5a189a,color:#fff,stroke-width:3px,font-size:16px,font-weight:bold

      subgraph workers["🔵 Worker Layer -- 4 node . 12 partition"]
          N0["node0 . P0,1,2"]:::worker
          N1["node1 . P3,4,5"]:::worker
          N2["node2 . P6,7,8"]:::worker
          N3["node3 . P9,10,11"]:::worker
      end

      subgraph strictcp["🟣 Strict Control Plane -- 3-node HA"]
          C1["coordinator-1<br/>Leader"]:::strict
          C2["coordinator-2"]:::strict
          C3["coordinator-3"]:::strict
          C1 <-->|Raft / ZK Lock| C2
          C1 <-->|Raft / ZK Lock| C3
          C2 <-->|Raft / ZK Lock| C3
      end

      N0 & N1 & N2 & N3 <-->|"(1) Worker heartbeat & local LW_i"| C1
      C1 -.->|"(2) Broadcast global W_global"| N0 & N1 & N2 & N3
  ```

  **Chi tiết các luồng giao tiếp trong Domain 2 (Strict Coordination):**

  | Luồng giao tiếp | Hướng | Giao thức / Cổng | Nội dung & Tần suất |
  |:---|:---|:---|:---|
  | Worker Heartbeat | Worker → Coordinator Leader | gRPC `WorkerHeartbeat` (cổng HTTP+50), fallback HTTP `POST /punctuation` | `{partitions → LW_i, max_event_time, kafka_offsets, fencing_token, idle_partitions, backpressure}`; chu kỳ 1s. |
  | Get Global State | Worker → Coordinator Leader | gRPC `GetGlobalState`, fallback HTTP `GET /state` | Trả `{W_global, term, partition_types}`; Worker pull mỗi 500ms để chốt cửa sổ. |
  | Đồng thuận & Bầu chọn HA | Leader ↔ Followers / ZooKeeper | gRPC `RaftState`/`RaftVote` hoặc ZK Lock | Chế độ Raft: Sao chép `{W_global, partition_assignment, term}`, commit khi đa số (2/3) ack. Chế độ ZK: Leader sở hữu khóa tại `/csdlpt/coordinator-lock`, đồng bộ trạng thái qua gRPC/HTTP tới các follower. |
  | Failover / Failback command | Coordinator → Worker | gRPC/HTTP kèm `(term, command_id)` | Lệnh reassign / `PAUSE` / `RESUME` có fencing token; Worker từ chối lệnh có `term` cũ. |
  | Broadcast `W_global` | Coordinator (nội bộ) | Vòng lặp chủ động 200ms | Tính lại và đẩy `W_global` + trạng thái partition phục vụ đồng bộ và metrics. |

* **Domain 3: Luồng điều phối Heuristic Watermark (Heuristic Aggregation Domain)**
  * *Statistical Heartbeat*: Các Worker sử dụng DDSketch cục bộ để đo trễ thực tế, báo cáo mốc Watermark thích ứng $W_h$ và snapshot DDSketch tới Aggregator.
  * *Active-Standby Control Plane*: Cặp Aggregator dự phòng nóng (Primary-Standby). Việc tranh chấp Leader được thực hiện qua khóa phân tán ZooKeeper (`/csdlpt/aggregator-lock`) hoặc File Lock dùng chung (`FileLockLeader` hỗ trợ cross-platform `fcntl`/`msvcrt`). Standby liên tục theo dõi tệp nhịp tim của Active (`/tmp/aggregator-{port}-heartbeat` ghi mỗi 1s). Nếu nhịp tim mất hoặc quá hạn (> 1.5s), Standby tự động chiếm khóa và được nâng lên làm Active, tải lại trạng thái phân mảnh và watermark từ RocksDB/JSON (`load_state()`). Aggregator Active tổng hợp thông tin thu nhận để điều phối watermark thích ứng cục bộ, bản ghi muộn đi vào Dead-Letter Queue (DLQ).
  
  ```mermaid
  flowchart TB
      classDef worker fill:#264653,stroke:#1d3557,color:#fff,stroke-width:3px,font-size:16px,font-weight:bold
      classDef heur fill:#0077b6,stroke:#023e8a,color:#fff,stroke-width:3px,font-size:16px,font-weight:bold

      subgraph workers["🔵 Worker Layer -- 4 node . 12 partition"]
          N0["node0 . P0,1,2"]:::worker
          N1["node1 . P3,4,5"]:::worker
          N2["node2 . P6,7,8"]:::worker
          N3["node3 . P9,10,11"]:::worker
      end

      subgraph heurcp["🔷 Heuristic Control Plane -- HA pair"]
          AGG["aggregator<br/>primary (Active)"]:::heur
          AGGS["aggregator<br/>standby (Standby)"]:::heur
          AGG <-->|ZK Lock / File Lock| AGGS
      end

      N0 & N1 & N2 & N3 -.->|"(1) Heuristic heartbeat & local W_h"| AGG
  ```

  **Chi tiết các luồng giao tiếp trong Domain 3 (Heuristic Aggregation):**

  | Luồng giao tiếp | Hướng | Giao thức / Cổng | Nội dung & Tần suất |
  |:---|:---|:---|:---|
  | Heuristic Heartbeat | Worker → Aggregator Primary | gRPC `SendWorkerWatermark` (HTTP+50), fallback HTTP `POST /punctuation` | `{worker_id, partition_id, W_h}` kèm `L_eff` và snapshot DDSketch; chu kỳ ~200ms. |
  | Tính watermark toàn cục | Aggregator (nội bộ) | Vòng lặp 500ms | `W_global_h = min(W_h)` trên partition active; phân loại trạng thái `ACTIVE/STALE/IDLE/FAILED`. |
  | HA Active–Standby | Primary ↔ Standby | Khóa ZooKeeper / File lock + Shared State | Standby theo dõi file nhịp tim ghi mỗi 1s. Mất nhịp tim > 1.5s -> Standby chiếm khóa, khôi phục từ RocksDB/JSON và lên Active (RTO ≤ 2s). |
  | Broadcast `W_global_h` | Aggregator → Worker | HTTP `GET /state` (pull) | Worker nhận `W_global_h` thích ứng để chốt cửa sổ. |
  | Định tuyến dữ liệu muộn | Worker → Kafka `late_logs_dlq` | Kafka Producer | Sự kiện `T_event < W_h` được đẩy DLQ; hạch toán `per_window_loss` thay vì chặn dòng chính. |

* **Domain 4: Phối hợp Control & Metadata Plane (Control & Infrastructure Domain)**
  * *Ingestor Heartbeat*: Ingestor định kỳ gửi thông tin tiến độ đọc dữ liệu và độ lệch đồng hồ vật lý (clock skew) để Coordinator giám sát health và clock skew. Để tránh gRPC Fan-in Bottleneck, Ingestor đẩy heartbeat trực tiếp qua Kafka topic `ingestor-heartbeats`, sử dụng gRPC/HTTP làm luồng fallback.
  * *ZooKeeper*: Quản lý cấu hình Kafka cluster, thực hiện Service Discovery và hỗ trợ bầu chọn Leader cho các control plane.
  
  ```mermaid
  flowchart TB
      classDef ingest fill:#2d6a4f,stroke:#1b4332,color:#fff,stroke-width:3px,font-size:16px,font-weight:bold
      classDef strict fill:#7b2cbf,stroke:#5a189a,color:#fff,stroke-width:3px,font-size:16px,font-weight:bold
      classDef heur fill:#0077b6,stroke:#023e8a,color:#fff,stroke-width:3px,font-size:16px,font-weight:bold
      classDef zk fill:#6c757d,stroke:#495057,color:#fff,stroke-width:3px,font-size:16px,font-weight:bold

      subgraph controlGroup["🎛️ Control & Metadata Plane"]
          ING["Ingestor"]:::ingest
          ZK["ZooKeeper"]:::zk
          
          subgraph strictcp["🟣 Strict Control Plane"]
              C1["coordinator-1 (Leader)"]:::strict
          end

          subgraph heurcp["🔷 Heuristic Control Plane"]
              AGG["aggregator (primary)"]:::heur
          end
      end

      ING -->|"(1) Ingestor heartbeat (Kafka topic / gRPC)"| C1 & AGG
      ZK -.->|"(2) Discovery & configuration coordination"| C1 & AGG
  ```

  **Chi tiết các luồng giao tiếp trong Domain 4 (Control & Metadata):**

  | Luồng giao tiếp | Hướng | Giao thức / Cổng | Nội dung & Tần suất |
  |:---|:---|:---|:---|
  | Ingestor Heartbeat | Ingestor → Kafka / Coordinator | Hàng đợi Kafka (`ingestor-heartbeats`) hoặc gRPC `IngestorHeartbeat` (fallback) | `{ingestor_id, T_commit, timestamp, partitions_assigned, ingestor_clock, offsets}`; gửi mỗi 5s; Coordinator nhận RTT để giám sát health và clock skew. |
  | Truy vấn giám sát | Dashboard / Ops → Coordinator · Worker | HTTP `GET /health`, `/state`, `/api/metrics`, `/ingestor-health` | Bảng health/clock-skew, metrics completeness/latency, trạng thái `W_global`/term/failover. |
  | Discovery & cấu hình | ZooKeeper ↔ Kafka / Coordinator / Aggregator | ZooKeeper | Lưu cấu hình cluster Kafka, bầu chọn Leader và hỗ trợ Service Discovery. |

* **Domain 5: Luồng lưu trữ trạng thái và Checkpoint (State & Storage Domain)**
  * *Tier-1 Hot State*: RocksDB nhúng cục bộ tại mỗi Worker để ghi nhận trạng thái cửa sổ đang mở ($<1$ms write latency).
  * *Tier-2 Warm State*: Checkpoint định kỳ (mỗi 10 giây) sao lưu offset xử lý và RocksDB SST files sang Shared Volume chung phục vụ khôi phục nhanh.
  * *Tier-3 Cold State*: Các kết quả cửa sổ đã chốt được lưu trữ lâu dài trên Object Storage tương thích S3 (MinIO).
  
  ```mermaid
  flowchart TB
      classDef worker fill:#264653,stroke:#1d3557,color:#fff,stroke-width:3px,font-size:16px,font-weight:bold
      classDef store fill:#e9c46a,stroke:#c9a227,color:#333,stroke-width:3px,font-size:16px,font-weight:bold

      subgraph workers["🔵 Worker Layer -- 4 node . 12 partition"]
          N0["node0 . P0,1,2"]:::worker
          N1["node1 . P3,4,5"]:::worker
          N2["node2 . P6,7,8"]:::worker
          N3["node3 . P9,10,11"]:::worker
      end

      subgraph storage["🟡 State & Storage"]
          VOL["Shared Volume . Tier-2 Warm<br/>./checkpoint/shared -> /data"]:::store
          MINIO["MinIO . Tier-3 Cold"]:::store
      end

      N0 & N1 & N2 & N3 -->|"(1) Tier-2 checkpoint (metadata, RocksDB SST)"| VOL
      N0 & N1 & N2 & N3 -->|"(2) Tier-3 archive (closed windows)"| MINIO
  ```

  **Chi tiết các luồng giao tiếp trong Domain 5 (State & Storage):**

  | Luồng giao tiếp | Hướng | Giao thức / Cổng | Nội dung & Tần suất |
  |:---|:---|:---|:---|
  | Tier-1 Hot State | Worker ↔ RocksDB cục bộ | RocksDB embedded (cô lập theo partition) | Đọc/ghi trạng thái cửa sổ `< 1ms`; namespace key `ow:` (open), `cw:` (closed), `si:` (seen-id dedupe), `meta:`. |
  | Tier-2 Checkpoint | Worker → Shared Volume | I/O file (`/data/checkpoint/partition_{pid}/`) | Mỗi 10s ghi `checkpoint.json`, `emitted.json`, SST files + offset Kafka đã commit; phục vụ failover (node khác mount & `seek(offset+1)`). |
  | Tier-3 Archive / DR | Worker → MinIO (S3) | MinIO client | Upload closed window theo key tất định `{partition}/{window_id}.json`; máy trạng thái eviction `CLOSED → UPLOADING → UPLOADED → PURGED`; backup active-state định kỳ phục vụ Disaster Recovery. |

### 4.2 Vai trò các thực thể thành phần (Container View)
| Ký hiệu | Tên thành phần | Phân lớp | Mô tả vai trò & Chức năng chính |
|:---|:---|:---|:---|
| `ING` | Ingestor | Ingest Layer | Đọc CSV taxi, gán $T_{\text{event}}$, gửi log kèm Punctuation Token vào Kafka topic `events`. |
| `KAFKA` | Kafka Broker | Data Plane | Broker thông điệp phân tán quản lý 12 partitions cho các topic `events`, `strict_results`, `audit_results`. |
| `ZK` | ZooKeeper | Auxiliary | Duy trì cấu hình cluster Kafka, bầu chọn Leader và hỗ trợ Service Discovery. |
| `N0` - `N3` | Worker Nodes | Worker Layer | Các node tính toán chạy song song, tiêu thụ dòng sự kiện từ các partitions Kafka tương ứng, quản lý window logic và tính toán local watermark. |
| `C1` - `C3` | Coordinator Nodes | Strict Control Plane | Cụm 3 node điều phối hoạt động theo giải thuật Raft nhằm tính toán Watermark toàn cục $W_{\text{global}}$ trong chế độ Strict. |
| `AGG` / `AGGS` | Aggregator | Heuristic Control Plane | Cặp Aggregator dự phòng nóng (Active-Standby) tổng hợp DDSketch từ các Worker để tính Watermark thích ứng. |
| `VOL` | Shared Volume | Storage (Tier-2) | Shared Volume lưu tệp Checkpoint trạng thái của các worker định kỳ mỗi 10 giây. |
| `MINIO` | MinIO Storage | Storage (Tier-3) | Kho lưu trữ đối tượng tương thích S3 để lưu trữ lâu dài kết quả của các cửa sổ thời gian đã đóng. |
| `DASH` | Streamlit Dashboard | Management | Giao diện đồ họa giám sát hiệu năng hệ thống và điều phối container. |

### 4.3 Vai trò các tiến trình thực thi trong mã nguồn
| Vai trò (Role) | Đối số dòng lệnh | Mô tả chức năng mã nguồn |
|:---|:---|:---|
| `ingestor` | `--role ingestor` | Đọc CSV taxi từ `/dataset`, gán nhãn $T_{\text{event}}$, gửi log và punctuation vào Kafka. |
| `worker` | `--role worker` | Kéo dữ liệu từ Kafka, sắp xếp bằng Bounded Min-Heap, ghi RocksDB, gom window và emit kết quả. |
| `coordinator` | `--role coordinator` | Cụm Raft tính toán $W_{\text{global}} = \min(LW_i)$, điều phối failover và kiểm soát lệnh bằng term fencing. |
| `aggregator` | `--role aggregator` | Quản lý HA active-standby, tổng hợp $W_{\text{global\_h}}$ thích ứng từ các bản chụp DDSketch của các worker. |

### 4.4 Hợp đồng giao tiếp giữa các Domain và Giải pháp cho bài toán nút cổ chai (Stragglers)

Để hệ thống hoạt động đồng bộ và bền vững, các giao tiếp giữa các Domain được chuẩn hóa thành các hợp đồng dịch vụ (Service Contracts) chặt chẽ:
1. **Hợp đồng Ingestion (Data Ingestion Contract)**: Ingestor cam kết phân mảnh dữ liệu hành trình xe taxi ngang theo hàm băm và gán thời gian sự kiện logic $T_{\text{event}}$. Trong chế độ nghiêm ngặt (Strict), Ingestor phải phát thông điệp kiểm soát (Punctuation Token) chứa mốc cam kết $T_{\text{commit}}$. Khi một phân mảnh không có chuyến đi mới phát sinh (nhàn rỗi), Ingestor định kỳ gửi các *Empty Punctuation Token* với $T_{\text{commit}}$ tăng dần để dòng chảy thời gian của phân mảnh đó không bị dừng.
2. **Hợp đồng Heartbeat của Worker (Worker Heartbeat Contract)**: Định kỳ 1 giây, các Worker gửi báo cáo trạng thái qua gRPC `WorkerHeartbeat` tới Coordinator. Nội dung báo cáo bao gồm: mốc thời gian Watermark cục bộ của từng phân mảnh đang quản lý $LW_i(P_k)$, offset Kafka đã xử lý, và danh sách các phân mảnh bị nhàn rỗi (`idle_partitions`).
3. **Hợp đồng Trạng thái Toàn cục (Global State Contract)**: Các Worker định kỳ 500 ms truy vấn Coordinator qua gRPC `GetGlobalState` để nhận mốc thời gian Watermark toàn cục $W_{global}$ phục vụ cho việc chốt và giải phóng trạng thái cửa sổ thời gian logic.

#### Cơ chế giải quyết bài toán nút cổ chai (Stragglers)

Hiện tượng nút cổ chai xảy ra khi một hoặc một vài phân mảnh bị xử lý chậm trễ hoặc không có dữ liệu phát sinh (Idle Partition), kéo mốc Watermark toàn cục dừng lại và làm tràn bộ nhớ đệm trạng thái RAM (MemTable) tại tất cả các Worker khác. Hệ thống giải quyết bài toán này qua hai chiến lược chuyên biệt:

* **Giải pháp trong chế độ Strict (Empty Punctuation & Idleness Bypass):**
  * Khi Worker nhận được Empty Punctuation Token từ Kafka, nó sẽ tịnh tiến watermark cục bộ $LW_i(P_k)$ của phân mảnh đó lên.
  * Nếu một phân mảnh nhàn rỗi quá thời gian quy định (`IDLE_TIMEOUT_S`), Worker sẽ tự động đánh dấu phân mảnh đó là `TEMPORARY_IDLE` và gửi báo cáo danh sách này lên Coordinator.
  * Coordinator Leader khi tính toán watermark toàn cục sẽ loại trừ các phân mảnh nhàn rỗi *tường minh* ra khỏi hàm $\min$:
    $$\large \boxed{W_{\text{global}} = \max\Big(W_{\text{global}}^{prev},\ \min_{P_k \notin \text{Idle}} LW_i(P_k)\Big)}$$
    Điều này cho phép $W_{\text{global}}$ tiếp tục tiến lên, giải phóng trạng thái cửa sổ của các phân mảnh hoạt động khác. Khi phân mảnh nhàn rỗi có dữ liệu trở lại, nó sẽ tự động được đưa trở lại danh sách tính toán.
  * **Lưu ý đồng bộ mã hiện thực** (`strict/coordinator.py`): Empty Punctuation là cơ chế chính giữ $W_{\text{global}}$ tịnh tiến nên hệ thống **không** loại bỏ ngầm các phân mảnh chỉ vì im lặng. Các phân mảnh chậm (`STALE`) và nhàn rỗi ngầm (`IDLE`) vẫn nằm trong hàm $\min()$; chỉ phân mảnh `FAILED` hoặc được Worker đánh dấu `is_temporary_idle` mới bị loại trừ, bảo toàn cam kết 0% loss.

  ```mermaid
  sequenceDiagram
      autonumber
      participant I as Ingestor (Slow Source)
      participant K as Kafka Broker (P10)
      participant W3 as Worker 3 (Node chậm/nhàn rỗi)
      participant C as Coordinator (Raft Leader)
      participant W12 as Worker 1 & 2 (Node bình thường)

      Note over I,W3: Kịch bản 1: Phân mảnh P10 không có log mới (Idle Partition)
      I->>K: 1. Phát Empty Punctuation Token (T_commit tiến dần)
      K->>W3: 2. Poll Empty Punctuation
      W3->>W3: 3. Nhận thấy P10 nhàn rỗi quá timeout -> Đánh dấu TEMPORARY_IDLE
      W3->>C: 4. WorkerHeartbeat {idle_partitions: [10], local_watermarks} (gRPC)
      C->>C: 5. Loại bỏ P10 ra khỏi danh sách tính min: W_global = min(LW_0..LW_9, LW_11)
      C-->>W12: 6. GetGlobalState -> Broadcast W_global mới tiến lên
      Note over W12: Window chốt bình thường, không bị nghẽn bởi P10!

      Note over I,W3: Kịch bản 2: Khi P10 có dữ liệu mới trở lại (Active back)
      I->>K: 7. Ghi nhận log event mới (T_event)
      K->>W3: 8. Poll event log mới
      W3->>W3: 9. Hủy nhãn TEMPORARY_IDLE của P10
      W3->>C: 10. WorkerHeartbeat {idle_partitions: [], local_watermarks}
      C->>C: 11. Đưa P10 quay lại danh sách tính min toàn cục
  ```

* **Giải pháp trong chế độ Heuristic (DDSketch & DLQ routing):**
  * Không có sự phụ thuộc toàn cục. Mỗi Worker tự ước lượng phân phối độ trễ cục bộ của phân mảnh đang giữ bằng cấu trúc dữ liệu DDSketch và tự xác định mốc chốt cửa sổ thích ứng $W_h$ mà không cần chờ đợi các phân mảnh khác.
  * Nếu một phân mảnh bị chậm (Straggler), các sự kiện đến muộn sau khi cửa sổ đã chốt sẽ không làm tắc nghẽn luồng xử lý chính. Chúng sẽ được định tuyến tự động vào Hàng đợi xử lý muộn (Dead-Letter Queue - DLQ) để xử lý đền bù trạng thái lịch sử sau đó, đảm bảo độ trễ chốt cửa sổ luôn ở mức tối thiểu.

  ```mermaid
  sequenceDiagram
      autonumber
      participant K as Kafka Broker (P10 - Straggler)
      participant W3 as Worker 3 (Nhận log trễ)
      participant DLQ as late_logs_dlq (Kafka Topic)
      participant SINK as Window Output (Audit Results)

      Note over K,W3: Kịch bản: Log trễ bất tuần tự đến sau khi cửa sổ đóng
      K->>W3: 1. Poll log event trễ (T_event = 100)
      Note over W3: Watermark thích ứng hiện tại W_h = 120 (do DDSketch L_eff = 12)
      W3->>W3: 2. So sánh T_event (100) < W_h (120) -> Xác định log trễ (Late Event)
      W3->>DLQ: 3. Định tuyến tự động sang Hàng đợi xử lý muộn (DLQ)
      Note over W3: Log bình thường (T_event >= 120)
      W3->>SINK: 4. Xử lý cửa sổ nhanh chóng và emit kết quả tức thời
      Note over DLQ: Khôi phục/đền bù trạng thái lịch sử bất đồng bộ
  ```

---

## 5. Tech Stack & Implementation Plan (Công nghệ & Kế hoạch triển khai)
* **Programming Language:** Python 3.10+
* **Deployment:** Containerized deployment using Docker & Docker Compose with profiles (`strict` vs. `heuristic`).
* **Libraries/Frameworks:**
  * `grpcio` & `grpcio-tools` for high-performance communication.
  * RocksDB bindings for worker local state.
  * `ddsketch` for streaming quantile estimation ($\alpha=0.01$).
  * `pandas`, `numpy`, `matplotlib` for analytics and plotting Pareto curves.
  * `pytest` for testing suite.

---

## 6. Success Metrics & Analysis (Chỉ số đánh giá & Phân tích kịch bản sự cố)

### 6.1 Completeness-at-Latency Trade-off (Sự đánh đổi giữa tính hoàn thiện dữ liệu và độ trễ)

Để đánh giá hiệu quả của hai phương thức định thời logic, hệ thống thực hiện đo lường hai chỉ số định lượng cốt lõi:
1. **Tính hoàn thiện dữ liệu (Data Completeness %)**:
   * *Độ hoàn thiện tức thời (Immediate Completeness)*: Tỷ lệ dữ liệu xử lý đúng hạn trong cửa sổ logic trước khi mốc thời gian Watermark đóng cửa sổ đó.
   * *Độ hoàn thiện cuối cùng (Eventual Completeness)*: Tỷ lệ dữ liệu được tổng hợp đầy đủ sau khi đã tính cả các bản ghi đến muộn được xử lý đền bù qua Hàng đợi xử lý muộn (Dead-Letter Queue - DLQ).
2. **Độ trễ chờ Watermark (Watermark Latency / Wait Time)**: Khoảng thời gian hệ thống phải chờ (tính bằng giây mô phỏng) từ lúc cửa sổ thời gian logic kết thúc cho đến khi mốc thời gian Watermark toàn cục vượt qua biên đóng cửa sổ.

Chúng tôi đã thực hiện chạy quét thực nghiệm (benchmark sweeps) với tập dữ liệu mô phỏng nén thời gian hệ số $\text{DIV} = 60$. Kết quả so sánh trực tiếp giữa hai chiến lược Strict Watermark và Heuristic Watermark được trình bày trong bảng dưới đây:

| Wait Time (s) | Strict Comp. % | Heuristic ($p$) | Heur. $L_{eff}$ (s) | Heur. Imm. % | Heur. Eventual % |
|---:|---:|---:|---:|---:|---:|
| 0.0 | **8.75** | $p=0.500$ | 11.63 | 64.21 | **100.00** |
| 1.0 | **12.88** | $p=0.500$ | 11.63 | 64.21 | **100.00** |
| 2.0 | **17.86** | $p=0.500$ | 11.63 | 64.21 | **100.00** |
| 3.0 | **23.29** | $p=0.500$ | 11.63 | 64.21 | **100.00** |
| 5.0 | **34.66** | $p=0.500$ | 11.63 | 64.21 | **100.00** |
| 7.0 | **45.48** | $p=0.500$ | 11.63 | 64.21 | **100.00** |
| 10.0 | **59.17** | $p=0.500$ | 11.63 | 64.21 | **100.00** |
| 15.0 | **75.03** | $p=0.500$ | 11.63 | 64.21 | **100.00** |
| 20.0 | **84.44** | $p=0.750$ | 18.67 | 81.26 | **100.00** |
| 30.0 | **93.23** | $p=0.900$ | 28.80 | 91.31 | **100.00** |
| 40.0 | **96.72** | $p=0.950$ | 37.78 | 94.77 | **100.00** |
| 50.0 | **98.41** | $p=0.990$ | 59.70 | 97.70 | **100.00** |
| 60.0 | **99.27** | $p=0.990$ | 59.70 | 97.70 | **100.00** |
| 90.0 | **99.89** | $p=0.999$ | 95.03 | 98.38 | **100.00** |
| 120.0 | **99.97** | $p=0.999$ | 95.03 | 98.38 | **100.00** |

**Các phát hiện cốt lõi từ thực nghiệm:**
1. **Đánh đổi trực tiếp của Strict Watermark**: Với phương pháp Strict, mỗi giây cấu hình độ trễ biên an toàn $\delta$ (Wait Time) sẽ cộng trực tiếp 1 giây vào độ trễ xử lý của mọi cửa sổ. Để đạt độ hoàn thiện $\approx 100\%$, hệ thống bắt buộc phải đặt $\delta = 120$ giây. Tuy nhiên, vẫn có khoảng $0.028\%$ sự kiện ngoại lai cực đoan bị mất mát vĩnh viễn do có độ trễ lớn hơn 120 giây.
2. **Sự vượt trội của Heuristic kết hợp DLQ**: Phương pháp Heuristic thích ứng tự động (với phân vị $p=0.50$) giúp giảm thời gian chờ chốt cửa sổ xuống chỉ còn $11.63$ giây (nhanh hơn gấp 10 lần so với Strict để đạt cùng mức hoàn thiện cuối cùng), đồng thời cơ chế đối chiếu đền bù qua Hàng đợi xử lý muộn (DLQ) đảm bảo tính hoàn thiện cuối cùng đạt tuyệt đối $100\%$ không mất mát dữ liệu.
3. **Phân loại ứng dụng thực tế**: 
   * Chế độ *Strict Watermark* với $\delta \approx 38$ giây (độ hoàn thiện đạt $\approx 95\%$) phù hợp nhất cho các Dashboard trực quan thời gian thực yêu cầu độ trễ cố định và không chấp nhận xử lý đền bù phức tạp.
   * Chế độ *Heuristic + DLQ* với $p=0.50$ (thời gian trễ hiệu dụng $L_{eff} \approx 12$ giây) phù hợp cho các luồng xử lý phân tích (Analytics / Batch), tối ưu hóa thời gian chờ của hệ thống và giải phóng tài nguyên bộ nhớ nhanh chóng.

### 6.2 Failure Scenario & Recovery Path (Kịch bản Lỗi và Quy trình Phục hồi)
  1. **Worker Node Crash (Sập Worker Node)**:
     * **Sự cố (Failure Scenario)**: Khi một Worker (ví dụ: Worker 3 phụ trách các phân mảnh $P_9 - P_{11}$) bị dừng tiến trình đột ngột:
       * **Phát hiện lỗi (Detection)**: Coordinator không nhận được nhịp tim `WorkerPing` từ Worker 3 quá 10 giây (`HEARTBEAT_TIMEOUT_S`), lập tức xác nhận Worker 3 đã sập.
       * **Phong tỏa và Đổi Term (Fencing)**: Coordinator tăng giá trị `fencing term` logic lên để vô hiệu hóa tất cả các lệnh cũ từ Worker 3 nếu nó sống dậy bất ngờ.
       * **Tái phân bổ & Cân bằng tải (Rebalance)**: Coordinator đổi trạng thái phân mảnh $P_9, P_{10}, P_{11}$ sang `REASSIGNING` trong Raft log. Để đảm bảo cân bằng tải và tránh hiện tượng nút cổ chai, Coordinator không chỉ định một node duy nhất gánh toàn bộ, mà thực hiện chia đều các phân mảnh bị ảnh hưởng cho các Worker còn sống:
         * Phân mảnh $P_9$ được gánh hộ bởi **Worker 0 (node0)**.
         * Phân mảnh $P_{10}$ được gánh hộ bởi **Worker 1 (node1)**.
         * Phân mảnh $P_{11}$ được gánh hộ bởi **Worker 2 (node2)**.
       * **Khôi phục trạng thái ấm (Warm State Recovery)**: Các Worker còn sống (Worker 0, Worker 1, Worker 2) nhận chỉ thị, nạp checkpoint tương ứng từ Shared Volume (Tier-2) gồm offset đã xử lý gần nhất và RocksDB metadata, sau đó thực hiện `seek(checkpoint_offset + 1)` trên Kafka để tiếp tục xử lý dòng sự kiện, đảm bảo tính nhất quán Exactly-Once.
     * **Phục hồi (Recovery Path - Failback)**: Khi Worker 3 được khởi động lại và sẵn sàng nhận lại các phân mảnh cũ của mình ($P_9, P_{10}, P_{11}$):
       * **Bước 1 (Request)**: Worker 3 gửi yêu cầu đăng ký reassign các phân mảnh cũ lên Coordinator.
       * **Bước 2 (Lock)**: Coordinator ghi nhận trạng thái `REASSIGNING` cho $P_9 - P_{11}$ vào Raft log.
       * **Bước 3 (Pause Source)**: Coordinator gửi lệnh `PAUSE` đồng thời đến các Worker đang gánh hộ tạm thời (Worker 0, Worker 1, và Worker 2). Các Worker này dừng kéo dữ liệu từ Kafka cho phân mảnh bàn giao tương ứng, thực hiện flush toàn bộ window state của RocksDB xuống đĩa, ghi checkpoint hoàn chỉnh lên Shared Volume và gửi ack xác nhận kèm offset cuối cùng đã xử lý về Coordinator.
       * **Bước 4 (Reassign)**: Coordinator cập nhật trạng thái Kafka consumer group để Worker 3 làm owner mới của $P_9 - P_{11}$, thu hồi quyền xử lý từ các Worker đang gánh hộ, ghi nhận trạng thái `PAUSED` vào Raft log.
       * **Bước 5 (Resume Target)**: Coordinator gửi lệnh `RESUME` kèm `fencing term` cho Worker 3. Worker 3 nạp checkpoint từ Shared Volume, `seek(offset + 1)` trên Kafka cho cả 3 phân mảnh, và bắt đầu tiêu thụ dữ liệu bình thường. Trạng thái các phân mảnh đổi về `ASSIGNED` trong Raft log.
     
     **Sơ đồ Máy trạng thái Phân mảnh (Partition State Machine):**
     ```mermaid
     stateDiagram-v2
         [*] --> ASSIGNED
         ASSIGNED: Node X đang xử lý
         REASSIGNING: Đang chuyển giao từ X sang Y
         ORPHANED: Không có Node nào xử lý
         PAUSED: Tạm dừng (Backpressure/Handoff)

         ASSIGNED --> REASSIGNING: Yêu cầu chuyển giao (reassign)
         REASSIGNING --> ASSIGNED: Chuyển giao hoàn tất
         ASSIGNED --> PAUSED: Tạm dừng (Backpressure)
         PAUSED --> ASSIGNED: Tiếp tục (Resume)
         REASSIGNING --> ORPHANED: Node nguồn mất trước khi node đích sẵn sàng
         ORPHANED --> REASSIGNING: Coordinator chọn owner mới
     ```

     **Sơ đồ Trình tự Failback 5 bước sau phục hồi (5-Step Failback Sequence Diagram):**
     ```mermaid
     sequenceDiagram
         participant N3 as Worker 3 (Hồi phục)
         participant C as Coordinator (Raft Leader)
         participant W0 as Worker 0 (Gánh P9)
         participant W1 as Worker 1 (Gánh P10)
         participant W2 as Worker 2 (Gánh P11)
         participant K as Kafka Broker

         N3->>C: 1. Request reassign P9-P11 sau phục hồi
         C->>C: 2. Ghi trạng thái REASSIGNING cho P9-P11 vào Raft log
         C->>W0: 3a. Lệnh PAUSE P9 kèm fencing term
         C->>W1: 3b. Lệnh PAUSE P10 kèm fencing term
         C->>W2: 3c. Lệnh PAUSE P11 kèm fencing term
         W0->>W0: Dừng consume P9, flush RocksDB, checkpoint
         W1->>W1: Dừng consume P10, flush RocksDB, checkpoint
         W2->>W2: Dừng consume P11, flush RocksDB, checkpoint
         W0-->>C: Ack P9 & gửi offset cuối
         W1-->>C: Ack P10 & gửi offset cuối
         W2-->>C: Ack P11 & gửi offset cuối
         C->>C: Ghi trạng thái PAUSED & metadata checkpoint vào Raft log
         C->>K: 4. Cập nhật Reassign Partitions P9-P11 sang Worker 3
         C->>N3: Lệnh RESUME kèm fencing term
         N3->>N3: 5. Nạp checkpoint từ Shared Volume & seek(offset + 1)
         N3-->>C: Xác nhận hoàn tất ASSIGNED
         C->>C: Ghi trạng thái ASSIGNED(Worker 3) vào Raft log
     ```

  2. **Split-brain Fencing**: Các kịch bản đa Coordinator (Split-Brain) khi mạng phân mảnh được giải quyết triệt để nhờ cặp Fencing Token gồm nhiệm kỳ độc bản (`fencing term`) đi kèm trên mọi gói tin chỉ thị điều phối gửi tới các Worker.
  3. **Idle Partition Straggler (Idle Partition)**: Nhận biết phân mảnh nhàn rỗi thông qua cấu hình timeout và kích hoạt cơ chế `TEMPORARY_IDLE`, Coordinator tạm thời loại bỏ phân mảnh này ra khỏi hàm $\min()$ tính Watermark toàn cục để tránh làm nghẽn dòng chảy logic thời gian toàn cụm.

---

## 7. Project Milestones (Mốc thời gian Dự án)
* **Milestone 1 (Week 5):** Complete Docker environment, Ingestion pipeline, Hash Partitioning, and Worker Min-Heap prioritization.
* **Milestone 2 (Week 8):** Operational Strict Watermark logic, Punctuation heartbeat tracking, RocksDB window state, and Raft Coordinator metadata failover.
* **Milestone 3 (Week 12):** Operational Heuristic DDSketch tracking, DLQ historical reconciliation, automated failure simulation suite, and Pareto evaluation report.
