# TÀI LIỆU THIẾT KẾ KIẾN TRÚC HỆ THỐNG

## Stateful Stream Processing dựa trên Strict Watermark

**Bài toán #112 — Log Delay Compensator**
*Xử lý luồng `Web_Server_Logs` liên tục 24/7*

| Trường        | Giá trị               |
|---------------|-----------------------|
| Phiên bản     | 1.1                   |
| Trạng thái    | Design Specification  |
| Phạm vi       | Stateful Streaming    |
| Cam kết       | Exactly-Once + Strict Watermark (0% data loss) |

---

## Mục lục

1. [Tổng quan và mục tiêu](#1-tổng-quan-và-mục-tiêu)
2. [Bảng ký hiệu và thuật ngữ](#2-bảng-ký-hiệu-và-thuật-ngữ)
3. [Kiến trúc tổng thể](#3-kiến-trúc-tổng-thể)
4. [**Windowing Logic**](#4-windowing-logic)
   - 4.1. Event-Time vs Processing-Time
   - 4.2. Tumbling Window
   - 4.3. Strict Watermark (Offset-Based Punctuation)
   - 4.4. Partitioned Priority Queue
5. [**State Management**](#5-state-management)
   - 5.1. RocksDB LSM-Tree
   - 5.2. Partition-Level Checkpointing
6. [**Latency Analysis**](#6-latency-analysis)
   - 6.1. High-Resolution Profiling
   - 6.2. Node Skew — Lệch pha tiến độ logic
7. [**Robustness**](#7-robustness)
   - 7.1. Tầng 1 — PULL-Based Backpressure
   - 7.2. Tầng 2 — Even Redistribution
   - 7.3. Tầng 3 — Cascading Failure Protocol
   - 7.4. Tầng 4 — Idempotent Duplicate Filter
   - 7.5. Tầng 5 — Watermark Idleness
   - 7.6. Adaptive Watermark Delay (Elastic Time Horizon)
8. [Bảng tham số cấu hình mặc định](#8-bảng-tham-số-cấu-hình-mặc-định)
9. [Trade-offs và giới hạn](#9-trade-offs-và-giới-hạn)
10. [Kết luận](#10-kết-luận)

---

## 1. Tổng quan và mục tiêu

### 1.1. Bài toán

Hệ thống xử lý luồng (stream processing) log từ tầng Web Server hoạt động liên tục 24/7. Dữ liệu log có các đặc tính:

- **Vô hạn (unbounded)**: dòng dữ liệu không có điểm kết thúc.
- **Bất tuần tự (out-of-order)**: log đến muộn do độ trễ mạng, retry, buffering.
- **Khối lượng lớn**: throughput cao, đòi hỏi xử lý song song.
- **Tích lũy trạng thái (stateful)**: các cửa sổ thống kê cần lưu state dở dang.

### 1.2. Mục tiêu thiết kế

Thiết kế xoay quanh 4 trục yêu cầu chính, mỗi trục tương ứng với một mục trong tài liệu này:

| # | Trục yêu cầu          | Mục tiêu cốt lõi                                                          | Mục |
|---|-----------------------|---------------------------------------------------------------------------|-----|
| 1 | **Windowing Logic**   | Phân chia cửa sổ chính xác trên Event-Time, không phụ thuộc Processing-Time | §4  |
| 2 | **State Management**  | Lưu trữ trạng thái dở dang bền bỉ, dung lượng ổn định, recovery nhanh     | §5  |
| 3 | **Latency Analysis**  | Đo đạc & cảnh báo bottleneck cấp nano-giây                                | §6  |
| 4 | **Robustness**        | Tự khôi phục trước backpressure, cascading failures, replay không mất dữ liệu | §7  |

Các mục tiêu định lượng tổng hợp:

| Tiêu chí                | Mục tiêu định lượng                                                            |
|-------------------------|--------------------------------------------------------------------------------|
| **Strict Watermark**    | 0% mất dữ liệu do đến muộn *(bản gốc)*                                         |
| **Exactly-Once**        | Không trùng lặp khi replay, không bỏ sót khi sự cố *(bản gốc)*                 |
| **Latency**             | End-to-end ≈ `δ + W` = 15 giây tối thiểu ở chế độ vận hành thường *(suy ra)*   |
| **Availability**        | Tự khôi phục khi có ≥ 2 node sập đồng thời *(suy ra từ Cascading Protocol)*    |
| **Memory safety**       | Không bị OOM dù backpressure kéo dài *(bản gốc)*                               |
| **Disk footprint**      | Dung lượng RocksDB ổn định, không phình to vô hạn *(bản gốc)*                  |

> **Ghi chú**: bản đặc tả gốc không nêu SLA p99 cụ thể. Các giá trị "suy ra" ở trên là hệ quả logic của thiết kế, cần xác nhận lại trong pha benchmark.

### 1.3. Giả định và phạm vi

- **Hạ tầng**: Kafka làm message broker, các Worker Node chạy trong Docker container có Shared Volume gắn SSD.
- **Loại ngôn ngữ**: Python (asyncio) hoặc JVM-based, có thể truy cập RocksDB embedded.
- **Ngoài phạm vi**: thiết kế tầng ingest (Web Server → Kafka), thiết kế tầng tiêu thụ kết quả (downstream consumer).

---

## 2. Bảng ký hiệu và thuật ngữ

### 2.1. Ký hiệu toán học

| Ký hiệu                            | Ý nghĩa                                                                   |
|------------------------------------|---------------------------------------------------------------------------|
| `T_event`                          | Event-time của một bản ghi log                                            |
| `[window_start, window_end]`       | Cặp giá trị định danh một Tumbling Window                                 |
| `T_commit`                         | Mốc thời gian cam kết gắn vào Punctuation Token                           |
| `LW_i`                             | Local Watermark của Worker Node `i`                                       |
| `W_global`                         | Global Watermark — mốc watermark toàn cục, tính tại Coordinator           |
| `W_max(t)`                         | Mốc Local Watermark cao nhất của các Node Active tại thời điểm `t`        |
| `Node Skew_i(t)`                   | Độ lệch pha thời gian logic của Node `i` so với Node đi đầu               |
| `δ` (delta)                        | Độ trễ watermark (watermark delay / lateness tolerance) mặc định          |
| `δ_temp`                           | Độ trễ watermark tạm thời ở chế độ Greedy Inflation                       |
| `P_k`                              | Phân vùng Kafka thứ `k`                                                   |
| `Offset_{P_k}`                     | Offset Kafka cuối cùng đã commit của phân vùng `P_k`                      |

### 2.2. Thuật ngữ chính

- **Event-Time**: Mốc thời gian log được sinh ra tại Web Server (bất biến).
- **Processing-Time**: Mốc thời gian Worker Node xử lý log (biến thiên).
- **Tumbling Window**: Cửa sổ thời gian cố định, không chồng lấn.
- **Punctuation Token**: Gói tin điều khiển cam kết mốc watermark từ upstream.
- **Strict Watermark**: Cam kết 0% mất dữ liệu do đến muộn.
- **Backpressure**: Hiện tượng tải đẩy ngược từ downstream lên upstream khi tiêu thụ chậm.
- **Idempotent**: Tính chất một thao tác cho ra cùng kết quả khi thực hiện nhiều lần.
- **Checkpoint**: Bản chụp trạng thái nhất quán có thể khôi phục sau sự cố.

---

## 3. Kiến trúc tổng thể

```
┌───────────────────────────────────────────────────────────────────────┐
│                     Web Servers (Log Sources)                          │
└──────────────────────────────┬────────────────────────────────────────┘
                               │ log events + Punctuation Tokens
                               ▼
┌───────────────────────────────────────────────────────────────────────┐
│              Kafka Cluster (12 Partitions, Min-Heap per Partition)     │
│   ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ... ┌─────┐         │
│   │ P1  │ │ P2  │ │ P3  │ │ P4  │ │ P5  │ │ P6  │ ... │ P12 │         │
│   └─────┘ └─────┘ └─────┘ └─────┘ └─────┘ └─────┘     └─────┘         │
└────────┬──────────────┬──────────────┬──────────────┬─────────────────┘
         │              │              │              │
         ▼              ▼              ▼              ▼
   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
   │ Worker 1 │   │ Worker 2 │   │ Worker 3 │   │ Worker 4 │
   │ P1,P2,P3 │   │ P4,P5,P6 │   │ P7,P8,P9 │   │P10,11,12 │
   │ +RocksDB │   │ +RocksDB │   │ +RocksDB │   │ +RocksDB │
   └────┬─────┘   └────┬─────┘   └────┬─────┘   └────┬─────┘
        │              │              │              │
        │  Local_Watermark reports (TCP)             │
        └──────────────┴──────────────┴──────────────┘
                               │
                               ▼
                  ┌─────────────────────────┐
                  │      Coordinator         │
                  │  - W_global              │
                  │  - Failover Manager      │
                  │  - Idleness Detector     │
                  │  - Adaptive Delay Ctrl   │
                  └────────────┬─────────────┘
                               │ Broadcast W_global
                               ▼
                  ┌─────────────────────────┐
                  │  Docker Shared Volume    │
                  │  /data/checkpoint/       │
                  │   ├── partition_1/       │
                  │   ├── partition_2/       │
                  │   └── ...                │
                  └─────────────────────────┘
```

### Các thành phần chính

- **Ingestor Layer**: Đọc log từ Web Server, gắn `event_time`, định kỳ phát Punctuation Token.
- **Kafka Cluster**: 12 partitions, mỗi partition là một Min-Heap theo `event_time`.
- **Worker Nodes (4 nodes)**: Mỗi node quản lý 3 partitions, có RocksDB embedded làm State Backend.
- **Coordinator**: Service trung tâm hội tụ Global Watermark, điều phối failover, quản lý Adaptive Delay.
- **Shared Volume**: Lưu trữ checkpoint theo Partition ID để failover bất kỳ node nào cũng nạp được state.

---

## 4. Windowing Logic

> **Mục tiêu**: Phân chia cửa sổ chính xác trên trục Event-Time, đảm bảo không mất dữ liệu đến muộn (Strict Watermark), và xử lý out-of-order trong môi trường phân tán.

### 4.1. Event-Time vs Processing-Time

- **Event-Time** (`T_event`): Mốc thời gian thực tế bản ghi log được khởi tạo tại Web Server, ghi nhận trực tiếp vào trường `event_time` của cấu trúc log. Trục thời gian này bất biến, phản ánh đúng trình tự nghiệp vụ.
- **Processing-Time**: Mốc thời gian Worker Node nhận và thực thi tính toán. Trục này có độ trễ biến thiên liên tục do hiệu năng phần cứng, hàng đợi mạng và scheduling của hệ điều hành.

Hệ thống **loại bỏ hoàn toàn Processing-Time** khỏi các phép phân chia cửa sổ, sử dụng Event-Time làm thước đo duy nhất.

### 4.2. Tumbling Window

Dòng log vô hạn được phân chia thành các cửa sổ thời gian cố định kích thước **5 giây** (Tumbling Window). Mỗi bản ghi log có Event-Time `T_event` được gán vào duy nhất một cửa sổ định danh bằng cặp `[window_start, window_end]` thông qua phép làm tròn xuống:

$$window\_start = \lfloor T_{event} / 5 \rfloor \times 5$$

$$window\_end = window\_start + 5$$

Do hiện tượng out-of-order, các bản ghi thuộc nhiều Window khác nhau có thể đến xen kẽ tại cùng một thời điểm Processing-Time. Hệ thống gom nhóm chúng vào các phân vùng lưu trữ tương ứng trong State một cách độc lập với thứ tự nhận được.

### 4.3. Strict Watermark (Offset-Based Punctuation)

Để đáp ứng cam kết **0% data loss**, hệ thống thiết lập đồng bộ hóa qua **Punctuation Tokens**:

**Bước 1 — Phát sinh Punctuation từ Upstream**
Định kỳ hoặc theo dung lượng gói dữ liệu, Ingestor phát một gói tin đặc biệt chứa mốc `T_commit` vào luồng. Token mang ngữ nghĩa: *"Từ thời điểm này, không còn log nào có `T_event < T_commit` được sinh ra tại Upstream nữa."*

**Bước 2 — Sắp xếp tuần tự qua Priority Queue**
Token di chuyển dọc theo partition đã được sắp xếp ưu tiên của Kafka (đối với môi trường chạy thật qua Kafka: Punctuation Token được ghi trực tiếp vào cùng Kafka topic `"events"` - cơ chế In-band Signaling; đối với chế độ giả lập không có Kafka: gửi out-of-band qua cuộc gọi HTTP POST), đảm bảo nó chỉ đến Worker Node sau khi toàn bộ dữ liệu có `T_event < T_commit` của partition đó đã được tiêu thụ thành công.

**Bước 3 — Hội tụ tại Coordinator**
Khi Worker Node `i` đọc được Punctuation Token, nó cập nhật:

$$LW_i = T_{commit}$$

Đồng thời gửi báo cáo `LW_i` về Coordinator qua TCP thời gian thực.

**Bước 4 — Cập nhật Global Watermark**
Coordinator thu thập báo cáo từ toàn bộ partition đang Active. Chỉ khi tất cả partition Active đều đã xử lý qua `T_commit`:

$$W_{global} = \min_{i \in \text{Active Partitions}} (LW_i)$$

**Bước 5 — Chốt sổ vật lý (Physical Emit)**
`W_global` được broadcast xuống các Worker Node. Tại mỗi Node, Window nào có `window_end ≤ W_global` sẽ được:
1. Tính toán kết quả thống kê cuối cùng.
2. Ghi nhận báo cáo ra downstream.
3. Xóa dữ liệu thô khỏi RocksDB để dọn dẹp bộ nhớ.

### 4.4. Partitioned Priority Queue

Dòng log đổ về từ Web Server luôn gặp lỗi xáo trộn thứ tự do độ trễ mạng cục bộ giữa các luồng. Hệ thống giải quyết bằng cách tích hợp **Priority Queue** vào tầng lõi của từng partition.

#### 4.4.1. Natural Event-Time Sorting

Thay vì FIFO truyền thống, mỗi partition của Kafka hoạt động như một **Min-Heap**:

- Khi log mới được đẩy vào partition, nó được chèn vào Min-Heap với `event_time` làm Priority Key.
- Hàng đợi tự sắp xếp lại sao cho bản ghi có `event_time` nhỏ nhất luôn ở đỉnh cây.
- Khi Worker Node `poll()`, bản ghi có `event_time` cũ nhất luôn ra đầu tiên, tạo dòng chảy dữ liệu có thứ tự thời gian tăng dần tự nhiên.

#### 4.4.2. Vai trò khi Replay khôi phục trạng thái

Khi sự cố nghẽn mạng hoặc sập node xảy ra, Worker Node tiếp quản partition chỉ cần gọi `seek()` về offset an toàn. Nhờ Priority Queue, toàn bộ dòng dữ liệu lịch sử được kéo ra theo đúng trình tự thời gian tuyến tính tăng dần.

Điều này giúp bộ máy chia Window trong RocksDB tự động tái dựng các Window dở dang một cách tuần tự tuyệt đối, loại bỏ hoàn toàn các lỗi tính toán sai lệch trung bình hoặc tổng lũy kế.

---

## 5. State Management

> **Mục tiêu**: Lưu trữ trạng thái Active Windows bền bỉ trên đĩa, dung lượng ổn định bất chấp thời gian chạy vô hạn, và cho phép bất kỳ Node nào tiếp quản trạng thái của Partition khác khi failover.

Do hệ thống chạy liên tục vô hạn, trạng thái dở dang của các Active Windows tích lũy không giới hạn. Để tránh OOM, hệ thống sử dụng **RocksDB Embedded Key-Value Store** làm State Backend.

### 5.1. RocksDB LSM-Tree

**Write Path (ghi tốc độ cao)**
Khi có log mới, Worker Node ghi state Window vào:
1. Write-Ahead Log (WAL) trên đĩa cứng (append-only, không seek ngẫu nhiên).
2. MemTable trên RAM (bộ đệm siêu nhẹ).

**SST Flush (đẩy xuống đĩa)**
Khi MemTable đầy, RocksDB nén và đẩy dữ liệu xuống các file SST (Sorted String Table) trên SSD. RAM được giải phóng lập tức, chỉ giữ lại Index Cache nhẹ để tra cứu nhanh.

**Purge & Compaction (dọn rác nền)**
Khi nhận broadcast `W_global` xác nhận một Window đã chốt sổ, Node gọi delete key tương ứng. RocksDB chạy Compaction nền để thu hồi dung lượng SSD theo cách cuốn chiếu, đảm bảo dung lượng đĩa luôn phẳng và ổn định.

### 5.2. Partition-Level Checkpointing

Hệ thống lưu Checkpoint trên **Docker Shared Volume** (`/data/checkpoint`) theo **Partition ID** thay vì Node ID. Điều này cho phép bất kỳ Node nào cũng có thể tiếp quản partition khi failover.

```
/data/checkpoint/
├── partition_7/
│   ├── metadata.json      # Lưu Offset_P7 và danh sách file SST active
│   └── state.db/          # Các file SST chứa state Window của P7
├── partition_8/
│   ├── metadata.json      # Lưu Offset_P8 và danh sách file SST active
│   └── state.db/
└── partition_9/
    ├── metadata.json
    └── state.db/
```

Mỗi bản Checkpoint là một gói dữ liệu đóng nhất quán:

$$\text{Checkpoint Package} = \{ \text{Incremental SST Files}, \text{Metadata of Active Windows}, \text{Kafka Partition Offsets} \}$$

Cụ thể gồm:

- **Incremental Checkpoint**: RocksDB tận dụng tính bất biến (immutable) của các file SST. Mỗi chu kỳ checkpoint (10 giây), chỉ copy các file SST mới tạo trong chu kỳ đó sang Shared Volume, không copy lại file SST cũ. Quá trình diễn ra trong vài mili-giây, giảm thiểu IOPS.
- **Kafka Partition Offset**: Lưu chính xác mã số vị trí đọc cuối cùng mà Node đã xử lý thành công trước checkpoint (ví dụ: `Offset = 100` của phân vùng `P_7`).

---

## 6. Latency Analysis

> **Mục tiêu**: Đo đạc hiệu năng đến cấp nano-giây để định vị bottleneck trước khi sự cố xảy ra, và phát hiện sớm Node bị nghẽn làm chậm tiến độ chốt sổ Window của toàn cụm.

Hệ thống thiết lập mạng lưới telemetry giám sát hiệu năng đến cấp nano-giây để cảnh báo bottleneck trước khi sự cố xảy ra.

### 6.1. High-Resolution Profiling

Hệ thống sử dụng bộ định thời phần cứng truy cập trực tiếp thanh ghi đếm chu kỳ CPU thông qua API độ phân giải cao (ví dụ `time.perf_counter_ns()`).

Mọi toán tử chính tại Worker Node được bọc bằng Profiling Hooks:

```python
t_start = time.perf_counter_ns()
# ... operation ...
t_elapsed = time.perf_counter_ns() - t_start
```

Latency được chuẩn hóa về đơn vị mili-giây:

$$\text{Processing Latency (ms)} = \frac{\text{Cpu Cycle End} - \text{Cpu Cycle Start}}{1{,}000{,}000}$$

Hệ thống thu thập **3 chỉ số vàng**:

| Chỉ số              | Ý nghĩa                                              |
|---------------------|------------------------------------------------------|
| `T_network_ingest`  | Thời gian trích xuất dữ liệu từ hàng đợi mạng        |
| `T_deduplication`   | Thời gian thực hiện đối chiếu lọc trùng              |
| `T_state_write`     | Thời gian đồng bộ dữ liệu vào RocksDB State Backend  |

### 6.2. Node Skew — Lệch pha tiến độ logic

Để định vị Worker Node bị nghẽn, Coordinator liên tục tính chỉ số **Node Skew** của từng Node `i` tại mốc Processing-Time `t`:

$$\text{Node Skew}_i(t) = W_{max}(t) - LW_i(t)$$

Trong đó:

- `LW_i(t)` là mốc Local Watermark mới nhất báo cáo từ Node `i`.
- `W_max(t)` là mốc thời gian tiến trình đi đầu của toàn cụm:

$$W_{max}(t) = \max_{j \in \text{Active}} (LW_j(t))$$

> **Lưu ý**: Skew so sánh với Node *đi nhanh nhất* (`W_max`), không phải với `W_global` (vốn là `min`). Đây là chủ ý — `W_max` cho biết "lẽ ra Node này phải tới đâu", còn `W_global` chỉ thể hiện trạng thái chung của cụm.

Nếu `Node Skew_i` vượt ngưỡng (ví dụ **5000ms**), hệ thống ghi nhận **Red Alert** trên Telemetry Dashboard để kỹ sư hệ thống nhận diện ngay Node nghẽn phần cứng hoặc mạng.

---

## 7. Robustness

> **Mục tiêu**: Đối phó với backpressure, dữ liệu trùng lặp, lỗi sập đa nút đồng thời (Cascading Failures), và kịch bản node sập-hồi phục với khoảng trống dữ liệu lớn — tất cả mà không bao giờ bị dừng hoạt động, tràn tài nguyên, hoặc mất dữ liệu.

Hệ thống thiết kế một **ma trận phòng thủ 5 tầng** cho các sự cố tức thời, kèm theo **tầng phục hồi đỉnh cao Adaptive Watermark Delay** cho kịch bản node sập-hồi phục có khoảng trống lớn.

### 7.1. Tầng 1 — PULL-Based Backpressure tự vệ chủ động

Dòng log từ Web Server là vô hạn và không thể dừng tại nguồn. Hệ thống tự vệ bằng mô hình kéo (PULL-based throttling):

1. Mỗi Worker Node duy trì hàng đợi xử lý bất đồng bộ giới hạn: `asyncio.Queue(maxsize=500)`.
2. Khi Node quá tải khiến hàng đợi đầy kịch trần (500/500 phần tử):
   - Phát lệnh `consumer.pause(assigned_partitions)` tới Kafka Broker.
   - Tiến trình pull dữ liệu tạm ngừng hoàn toàn. Log mới vẫn được Kafka đệm an toàn trên đĩa Broker. RAM Worker Node được bảo vệ tuyệt đối khỏi OOM.
3. Khi hàng đợi nội bộ giảm xuống dưới mức an toàn (**20%** kích thước, tức 100 phần tử), Node phát `consumer.resume(assigned_partitions)` để tiếp tục pull.

### 7.2. Tầng 2 — Even Redistribution (Tái phân phối tải đều)

Hệ thống chia luồng log trong Kafka thành **12 Partitions**. Trạng thái bình thường, 4 Worker Nodes mỗi node quản lý 3 partitions:

| Node    | Partitions          |
|---------|---------------------|
| Node 1  | P1, P2, P3          |
| Node 2  | P4, P5, P6          |
| Node 3  | P7, P8, P9 *(sự cố)* |
| Node 4  | P10, P11, P12       |

Khi Node 3 quá tải và PAUSE 3 partitions của nó, **Coordinator chia đều 3 partitions này cho 3 Node còn lại** (thay vì dồn cho 1 Node duy nhất gây cascading):

| Node    | Phân vùng quản lý       | Tải tăng |
|---------|-------------------------|----------|
| Node 1  | P1, P2, P3, **P7**      | +33%     |
| Node 2  | P4, P5, P6, **P8**      | +33%     |
| Node 4  | P10, P11, P12, **P9**   | +33%     |

Các Node tiếp quản truy cập Shared Volume, nạp Checkpoint của partition mới, gọi `seek()` về offset an toàn và kéo dữ liệu từ Priority Queue để Replay state dở dang.

### 7.3. Tầng 3 — Cascading Failure Protocol

Nếu trong lúc Node 3 chưa hồi phục, Node 1 (đang gánh P1, P2, P3, P7) đột ngột sập (sự cố chồng sự cố):

**Bước 1 — Định vị partition mồ côi**
Coordinator phát hiện Node 1 mất Heartbeat, khoanh vùng các partition mồ côi: `{P1, P2, P3, P7}`.

**Bước 2 — Tái phân phối tải cấp 2**
Chia đều 4 partitions cho 2 Node sống sót:

| Node    | Partitions cũ          | Partitions mới        | Tổng |
|---------|------------------------|-----------------------|------|
| Node 2  | P4, P5, P6, P8         | + **P1, P3**          | 6    |
| Node 4  | P10, P11, P12, P9      | + **P2, P7**          | 6    |

**Bước 3 — Di trú trạng thái và Tua ngược**
- Node 2 nạp Checkpoint của `{P_1, P_3}` từ Shared Volume, gọi `seek(P_1, Offset_{P_1}+1)`, `seek(P_3, Offset_{P_3}+1)`.
- Node 4 nạp Checkpoint của `{P_2, P_7}` từ Shared Volume, gọi `seek(P_2, Offset_{P_2}+1)`, `seek(P_7, Offset_{P_7}+1)`.

**Bước 4 — Replay gánh tải**
Các Node sống sót kéo dữ liệu từ Priority Queue, Replay dựng lại state và tiếp tục xử lý, cô lập hoàn toàn cascading failure.

### 7.4. Tầng 4 — Idempotent Duplicate Filter

Khi tua ngược Offset để Replay, hệ thống tích hợp bộ lọc trùng bám sát Watermark để đảm bảo **Exactly-Once Semantics (EOS)**:

- **Vòng lọc ngoài (Watermark Filter)**: Nếu `T_event < W_global`, bản ghi bị loại bỏ ngay vì Window chứa nó đã chốt sổ và xóa khỏi RocksDB.
- **Vòng lọc trong (State Hash Filter)**: Nếu nằm trong Window đang mở, hệ thống đối chiếu nhanh qua bảng băm `log_id` đang Active trong RocksDB để loại các bản ghi trùng.

### 7.5. Tầng 5 — Watermark Idleness Detection

Khi một partition đứng im do backpressure hoặc không có traffic, Local Watermark `LW_i` của nó đứng im, làm đóng băng `W_global` theo công thức `min()`.

Coordinator áp dụng **Idleness Detection**: Nếu quá **2000ms** mà một partition không gửi báo cáo `LW_i` mới, Coordinator đánh dấu nó là `IDLE` và **loại nó khỏi phép tính `min()` của `W_global`**. Dòng thời gian chung tiếp tục tiến lên, giải phóng RAM cho các Node khỏe mạnh khác.

### 7.6. Adaptive Watermark Delay (Elastic Time Horizon)

> Tầng này xử lý kịch bản nâng cao mà 5 tầng trên không bao quát: **node sập rồi hồi phục với khoảng trống dữ liệu lớn** (ví dụ 10 phút).

Khi một Node sập rồi hồi phục (ví dụ Node 3 sập từ mốc `10:00` đến `10:10`), khoảng trống dữ liệu tích lũy trong Kafka là 10 phút. Trong khi đó, `W_global` toàn cụm đã chạy tới mốc `10:10`.

Nếu Node 3 nạp Checkpoint cũ tại `10:00` và Replay, hệ thống sẽ gặp xung đột thời gian vì **cửa sổ Global hiện tại đã đi xa hơn rất nhiều**. Để giải quyết, áp dụng cơ chế **Adaptive Watermark Delay (Elastic Time Horizon)**:

```
Trạng thái bình thường (chạy nhanh):
─── [W_global] ◄── δ = 10s ──► [Event-Time hiện tại] ──►

Khi Node 3 hồi phục (kéo giãn "tham lam"):
─── [W_global] ◄────── δ_temp = 20 phút ──────► [Event-Time hiện tại] ──►
                                                (Replay dữ liệu cũ an toàn)

Khi Node 3 đã Catch-up xong (co lại trạng thái thường):
─── [W_global] ◄── δ = 10s ──► [Event-Time hiện tại] ──►
```

#### 7.6.1. Giai đoạn 1 — Greedy Inflation

Ngay khi Node 3 sống lại và kết nối với cụm:

1. Coordinator nhận diện `Node Skew_3 = 10 phút` (lệch pha sâu).
2. Thay vì giữ `δ = 10s` mặc định, Coordinator **kéo giãn `δ_temp = 20 phút`**, nới rộng chân trời chờ đợi đến mốc `10:20`. Cấu hình này được broadcast toàn cụm.
3. **Hệ quả**: Toàn bộ Node tạm thời giữ nguyên trạng thái mở của các Window cũ từ `10:00 → 10:10`, không được phép chốt sổ, tạo không gian an toàn đón Replay.

#### 7.6.2. Giai đoạn 2 — Catch-Up Phase

- Node 3 nạp Checkpoint cũ của `P_7, P_8, P_9` từ Shared Volume.
- Gọi `seek()` tua ngược và pull dữ liệu cũ từ Kafka Priority Queue.
- Do `δ_temp = 20 phút` cực lớn, **không một bản ghi Replay nào bị coi là dữ liệu muộn và vứt bỏ**. Toàn bộ Window dở dang được tái dựng và chốt kết quả hoàn hảo.
- Vì chỉ đọc dữ liệu lịch sử có sẵn từ Kafka, throughput của Node 3 nhanh gấp nhiều lần tốc độ sinh log real-time.

#### 7.6.3. Giai đoạn 3 — Convergence Phase

Coordinator liên tục giám sát chỉ số `Node Skew_3`:

$$\text{Node Skew}_3 = W_{max\_active} - LW_3$$

Do tốc độ Replay cực nhanh, `Node Skew_3` giảm dần theo thời gian thực: **10 phút → 5 phút → 1 phút → dưới 5 giây**.

#### 7.6.4. Giai đoạn 4 — Elastic Shrinking

Khi `Node Skew_3` hạ xuống dưới ngưỡng an toàn (ví dụ **< 5 giây**), Coordinator xác nhận Node 3 đã hoàn toàn đuổi kịp tiến độ chung của toàn cụm.

Coordinator phát lệnh **thu hẹp `δ` về mặc định 10 giây**. Hệ thống quay lại chế độ vận hành real-time tiêu chuẩn với latency cực thấp, khép lại quy trình tự khôi phục không mất mát dữ liệu.

---

## 8. Bảng tham số cấu hình mặc định

| Tham số                       | Giá trị mặc định | Thuộc trục         | Ghi chú                                                      |
|-------------------------------|------------------|--------------------|--------------------------------------------------------------|
| Window size (Tumbling)        | **5 giây**       | Windowing          | Kích thước cửa sổ thống kê                                   |
| `δ` (watermark delay)         | **10 giây**      | Windowing          | Độ trễ watermark ở chế độ bình thường                        |
| Checkpoint interval           | **10 giây**      | State Management   | Chu kỳ flush checkpoint                                      |
| Skew red alert threshold      | **5000 ms**      | Latency            | Ngưỡng cảnh báo node bottleneck                              |
| Backpressure queue maxsize    | **500**          | Robustness         | `asyncio.Queue(maxsize=500)`                                 |
| Backpressure resume threshold | **20%** (= 100)  | Robustness         | Mức an toàn để resume consumer                               |
| `T_idle` (idleness timeout)   | **2000 ms**      | Robustness         | Ngưỡng đánh dấu partition IDLE                               |
| `δ_temp` (greedy inflation)   | **20 phút**      | Robustness         | Độ trễ watermark khi node đang phục hồi                      |
| Skew shrink threshold         | **< 5 giây**     | Robustness         | Ngưỡng `Node Skew` để co `δ_temp` về `δ`                     |
| Số Kafka Partitions           | **12**           | Robustness         | Phân mảnh mịn để cân tải                                     |
| Số Worker Nodes               | **4**            | Robustness         | Mỗi node mặc định quản lý 3 partitions                       |
| Heartbeat timeout             | *(chưa định)*    | Robustness         | Bản gốc không quy định cụ thể; cần xác định ở pha triển khai |

---

## 9. Trade-offs và giới hạn

### 9.1. Trade-offs đã chấp nhận

- **Latency vs Correctness** *(Windowing × Latency)*: Watermark Delay `δ = 10s` cộng với Window size 5s làm tăng end-to-end latency tối thiểu ~15 giây, nhưng đảm bảo gom đủ dữ liệu late-arrival. Chấp nhận đánh đổi để đạt Strict Watermark.
- **Disk I/O vs RAM** *(State Management)*: RocksDB Embedded ưu tiên đĩa hơn RAM, latency `T_state_write` cao hơn in-memory store nhưng tránh OOM tuyệt đối.
- **Failover complexity vs Availability** *(State Management × Robustness)*: Partition-level Checkpoint phức tạp hơn Node-level nhưng cho phép tái phân phối tải đều khi sự cố.

### 9.2. Giới hạn đã biết

- **Phụ thuộc Coordinator**: Coordinator là single point of failure (cần thiết kế HA riêng cho Coordinator — ngoài phạm vi tài liệu này).
- **Adaptive Delay tối đa**: `δ_temp = 20 phút` giới hạn thời gian một node có thể "chết" mà vẫn replay được không mất dữ liệu. Sự cố dài hơn cần chiến lược khác (offline backfill).
- **Idleness Detection**: Với `T_idle = 2000ms` rất ngắn, có khả năng đánh dấu sai partition là IDLE khi traffic bursty. Có thể cần tuning theo đặc thù workload.
- **Skew calculation**: Nếu Local Watermark chưa được report kịp (lag TCP), Skew có thể bị tính sai trong cửa sổ ngắn.

---

## 10. Kết luận

Hệ thống Strict Watermark được tổ chức xoay quanh 4 trục yêu cầu, mỗi trục giải quyết một lớp vấn đề riêng biệt:

| Trục               | Đóng góp chính                                                                              |
|--------------------|---------------------------------------------------------------------------------------------|
| Windowing Logic    | Event-Time + Tumbling Window + Punctuation Token + Priority Queue → 0% data loss            |
| State Management   | RocksDB LSM-Tree + Partition-level Incremental Checkpoint → bền bỉ, dung lượng ổn định      |
| Latency Analysis   | High-resolution Profiling + Node Skew → định vị bottleneck cấp nano-giây                    |
| Robustness         | 5-tier defense + Adaptive Watermark Delay → tự khôi phục trước mọi kịch bản sự cố           |

Bốn trục này phối hợp để đạt 3 cam kết cốt lõi:

1. **0% data loss** thông qua Punctuation Tokens + Priority Queue + Adaptive Watermark Delay.
2. **Exactly-Once Semantics** thông qua Incremental Checkpoint + Idempotent Duplicate Filter.
3. **Tự khôi phục từ cascading failures** thông qua Partition-Level Checkpoint + Even Redistribution.

Các tham số cấu hình được liệt kê ở [§8](#8-bảng-tham-số-cấu-hình-mặc-định) cho phép tinh chỉnh theo đặc thù workload thực tế.

**Bước tiếp theo**: triển khai PoC, đo benchmark, và xây dựng HA cho Coordinator.
