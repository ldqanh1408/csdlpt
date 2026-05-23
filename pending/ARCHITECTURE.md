# Kiến trúc Hệ thống (System Architecture Document) — Distributed Watermark Tracker

Tài liệu này trình bày toàn diện cấu trúc sơ đồ, luồng xử lý dữ liệu, thiết kế lưu trữ, cơ chế chịu lỗi và các khía cạnh lý thuyết phân tán của hệ thống **Distributed Watermark Tracker**.

---

## 1. Sơ đồ Kiến trúc Tổng quan (System Topology)

Hệ thống được thiết kế theo mô hình xử lý stream phân tán một chiều (Directed Data Pipeline), kết hợp cơ chế kiểm soát tập trung (Coordinator) và các nút tính toán song song độc lập (Watermark Engines).

```text
                               ┌───────────────────────────┐
                               │  Tập dữ liệu Log đầu vào  │
                               │  (NASA-HTTP / Synthetic)  │
                               └─────────────┬─────────────┘
                                             │
                                             ▼
                               ┌───────────────────────────┐
                               │      Bộ nạp dữ liệu       │
                               │     (Ingestion Pipeline)  │
                               └─────────────┬─────────────┘
                                             │
                                             ▼
                               ┌───────────────────────────┐
                               │       Partitioner         │
                               │   (Phân vùng theo Host)   │
                               └─────────────┬─────────────┘
                                             │ Định tuyến: hash(host) % N
                      ┌──────────────────────┼──────────────────────┐
                      ▼ (Phân mảnh 0)        ▼ (Phân mảnh 1)        ▼ (Phân mảnh N-1)
               ┌──────────────┐       ┌──────────────┐       ┌──────────────┐
               │    Node 0    │       │    Node 1    │       │   Node N-1   │
               │ (Engine 0)   │       │ (Engine 1)   │       │ (Engine N-1) │
               └──────┬───────┘       └──────┬───────┘       └──────┬───────┘
                      │                      │                      │
                      │ Checkpoint           │ Checkpoint           │ Checkpoint
                      ▼                      ▼                      ▼
               ┌──────────────┐       ┌──────────────┐       ┌──────────────┐
               │ Checkpoint 0 │       │ Checkpoint 1 │       │ Checkpoint N │
               │    (.json)   │       │    (.json)   │       │    (.json)   │
               └──────┬───────┘       └──────┬───────┘       └──────┬───────┘
                      │                      │                      │
                      └────────────────┬─────┴──────────────────────┘
                                       │ Báo cáo kết quả chốt cửa sổ
                                       ▼
                               ┌───────────────────────────┐
                               │    Bộ điều phối chính     │
                               │       (Coordinator)       │
                               └─────────────┬─────────────┘
                                             │
                                             ▼
                               ┌───────────────────────────┐
                               │   Dashboard Giám sát      │
                               │     (Streamlit UI)        │
                               └───────────────────────────┘
```

---

## 2. Các Thành phần Hệ thống (System Components)

### 2.1. Ingestion Pipeline (Bộ nạp dữ liệu)
*   **Nhiệm vụ:** Đọc dữ liệu log thô từ file CSV (NASA HTTP dataset) hoặc sinh tự động (Synthetic generator). 
*   **Giả lập thế giới thực:** Bộ nạp chủ động tạo ra độ trễ ngẫu nhiên (`arrival_time = event_time + delay`) và bản ghi trùng lặp (`duplicates`) để tạo ra môi trường kiểm thử out-of-order thực tế cho các node xử lý.

### 2.2. Partitioner (Bộ phân mảnh dữ liệu)
*   **Cơ chế:** Phân mảnh ngang dữ liệu (Horizontal Fragmentation) theo khóa phân vùng (Route Key) là tên miền server (`host` hoặc `endpoint`).
*   **Thuật toán:** Sử dụng mã băm MD5 để định tuyến bản ghi đến đúng Node xử lý:
    $$\text{Node ID} = \text{MD5}(host) \pmod{\text{Số Node}}$$
*   **Locality (Tính cục bộ):** Đảm bảo tất cả các log của cùng một host sẽ luôn đi về cùng một node tính toán, giúp việc gộp nhóm theo cửa sổ chính xác tuyệt đối.

### 2.3. Processing Nodes (Các nút xử lý - Watermark Engine)
*   Mỗi Node hoạt động như một tiến trình tính toán độc lập, sở hữu:
    *   **State Store (Bộ lưu trữ trạng thái RAM):** Lưu trữ kết quả đếm trung gian của các cửa sổ thời gian (Window) chưa đóng.
    *   **Deduplication Filter:** Lọc trùng lặp dựa trên tập hợp `seen_ids`.
    *   **Watermark Register:** Quản lý watermark cục bộ dựa trên thời gian sự kiện tối đa đã thấy: $WM = \max(EventTime) - AllowedLateness$.
    *   **Atomic Checkpointer:** Tự động ghi snapshot trạng thái ra đĩa cứng định kỳ.

### 2.4. Coordinator (Bộ điều phối & Chịu lỗi)
*   **Chức năng:** Giám sát trạng thái hoạt động (sống/chết) của các Node trong cụm.
*   **Cơ chế chịu lỗi (Dead-Letter Queue - DLQ):**
    *   Khi phát hiện một Node bị sập (`dead`), Coordinator chặn luồng gửi dữ liệu vào bộ nhớ của node đó và chuyển hướng ghi nối tiếp xuống tệp tin DLQ trên đĩa (`.simdata/dlq/dlq_node_[id].jsonl`).
    *   Khi Node được lệnh hồi sinh (`revive`), Coordinator điều khiển phục hồi trạng thái Engine từ Checkpoint gần nhất, sau đó tiến hành **Replay** toàn bộ dữ liệu đang lưu trong DLQ để khôi phục trạng thái Exactly-Once hoàn toàn.

---

## 3. Luồng dữ liệu chi tiết (Detailed Data Pipeline)

### 3.1. Quy trình xử lý một bản ghi (Log processing life-cycle)
```text
[ Log Record ] ──> [ Lọc Trùng (Dedup) ] ──> [ Kiểm tra Hàng đợi (Backpressure) ]
                          │                                   │
                          │ Chưa từng thấy                     │ Dưới ngưỡng max_queue
                          ▼                                   ▼
             [ Cập nhật Max Event Time ]            [ Định tuyến Cửa sổ ]
                          │                                   │
                          ▼                                   ▼
             [ Tính toán Watermark mới ] ──> [ Kiểm tra cửa sổ kết quả đã đóng? ]
                                                              │
                                                              ├─► Có ──► [ Late Drop ] (Bỏ qua)
                                                              │
                                                              └─► Không ─► [ Cập nhật State ]
```

### 3.2. Quy trình Đồng bộ Kết thúc (End-of-Stream Barrier Synchronization)
Để chốt kết quả cuối cùng mà không bị mất log trễ, hệ thống áp dụng cơ chế đồng bộ rào cản:
1.  **Client** gửi bản ghi đặc biệt `EOS_MARKER` xếp cuối hàng đợi dữ liệu FIFO.
2.  **Node** nhận được `EOS_MARKER` $\rightarrow$ gọi `flush()`, đóng toàn bộ các cửa sổ còn lại, gửi Webhook báo cáo hoàn thành cho Coordinator.
3.  **Coordinator** thu thập đủ báo cáo hoàn thành từ $N$ node $\rightarrow$ chính thức kết thúc chu kỳ xử lý.

---

## 4. Thiết kế Lưu trữ & Checkpoint (Storage & Checkpoint Design)

Trạng thái hệ thống phân tán được bảo vệ bằng mô hình lưu trữ phi quan hệ phân mảnh kết hợp Checkpoint nguyên tử.

### 4.1. Cấu trúc trạng thái trong bộ nhớ RAM mỗi Node:
```json
{
  "watermark": 1700000020.0,
  "max_event_time": 1700000022.0,
  "seen_ids": ["id_1", "id_2", "..."],
  "windows": {
    "1700000000.0": { "count": 142, "status_500": 3 },
    "1700000010.0": { "count": 98,  "status_500": 0 }
  }
}
```

### 4.2. Cơ chế Atomic Write (Ghi đè nguyên tử):
Để ngăn ngừa tệp tin trạng thái bị hỏng nếu hệ thống sập nguồn đột ngột khi đang ghi checkpoint:
$$\text{State Snapshot} \xrightarrow{\text{Ghi đĩa}} \text{file.json.tmp} \xrightarrow{\text{os.replace() (Atomic)}} \text{file.json}$$

---

## 5. Áp dụng Lý thuyết Hệ thống phân tán (Theoretical Model Mapping)

| Khái niệm trong đồ án | Nền tảng Lý thuyết Phân tán | Tài liệu tham khảo |
| :--- | :--- | :--- |
| **Băm `hash(host) % N`** | Phân mảnh ngang (Horizontal Fragmentation) | Özsu & Valduriez, Ch. 3 |
| **Atomic Checkpointing** | Phục hồi sự cố (Distributed Reliability & Logging) | Özsu & Valduriez, Ch. 12 |
| **Độ trễ chờ allowed_lateness** | Tính nhất quán thời gian (Commit-wait / Spanner) | Spanner: Google's Globally-Distributed DB |
| **Dead-Letter Queue (DLQ)** | Lưu trữ đệm tin nhắn chịu lỗi (Message Queueing) | Enterprise Integration Patterns |
| **Backpressure / Drop** | Kiểm soát dòng chảy dữ liệu (Flow Control / Load Shedding) | Data Stream Management Systems |
| **Wait Time Slider** | Đánh đổi Latency vs Consistency (Định lý PACELC) | Abadi, PACELC Theorem (2012) |
