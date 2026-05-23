# BÁO CÁO HỆ THỐNG: DISTRIBUTED WATERMARK TRACKER
## Xử Lý Log Bất Đồng Bộ Với Event-Time Watermarks & Khắc Phục Sự Cố Phân Tán

**Nhóm thực hiện:** Nhóm 112
**Môn học:** Cơ sở dữ liệu phân tán (CSDLPT)
**Tham chiếu lý thuyết:** M. Tamer Özsu & Patrick Valduriez, *Principles of Distributed Database Systems*, 4th Edition.

---

## 1. Đặt Vấn Đề & Mục Tiêu

Trong các hệ thống phân tán quy mô lớn, dữ liệu log (truy cập web, giao dịch, telemetry) được sinh ra liên tục tại nhiều server biên (edge servers) khác nhau. Do sự lệch múi giờ, độ trễ mạng bất định (network jitter) và lỗi truyền dẫn, các log này khi được gửi về hệ thống xử lý stream trung tâm thường bị **lệch thứ tự nghiêm trọng (out-of-order)**. 

Nếu hệ thống gom log và thống kê dựa trên thời gian nhận được log (**Processing-time**), kết quả thống kê (ví dụ: số lượng request/phút) sẽ bị sai lệch hoàn toàn so với thực tế xảy ra. Do đó, hệ thống bắt buộc phải xử lý theo thời gian thực tế xảy ra sự kiện (**Event-time**).

### Mục tiêu của dự án:
1. **Windowing chính xác theo Event-Time**: Gom nhóm log vào các cửa sổ thời gian (Tumbling Windows) dựa trên trường `event_time` của log.
2. **Quản lý Watermark thông minh**: Tự động ước lượng độ trễ tối đa của log (`allowed_lateness` hay **Wait Time**) để đưa ra biên thời gian Watermark, làm cơ sở chốt cửa sổ an toàn.
3. **Phân tán & Cân bằng tải**: Phân mảnh ngang luồng log đầu vào theo cơ chế băm `hash(host) % N` về các Processing Nodes độc lập, đo đạc độ lệch tải (hot-key skew).
4. **Khả năng chịu lỗi cao (Fault Tolerance)**: Thiết kế cơ chế checkpoint atomic đảm bảo tính bền vững của trạng thái in-memory, kết hợp mô hình hàng đợi thư chết (Dead-Letter Queue - DLQ) trên đĩa để khôi phục Exactly-Once khi node bị sập đột ngột.
5. **Định lượng đánh đổi PACELC**: Thực nghiệm quét (sweep) trên dữ liệu thực tế NASA-HTTP (200.000 events) để phân tích đường cong đánh đổi giữa **Độ đầy đủ dữ liệu (Data Completeness)** và **Độ trễ kết quả (Result Latency)**.

---

## 2. Kiến Trúc Hệ Thống & Phân Mảnh Ngang

Hệ thống được thiết kế theo mô hình luồng dữ liệu một chiều (dataflow pipeline) gồm 7 thành phần chính:

```
┌────────────────────────────────────────────────────────┐
│     Source: NASA-HTTP CSV / Synthetic Generator        │
│          (event_id, event_time, host, status)          │
└───────────────────────────┬────────────────────────────┘
                            │ sort by arrival_time
                            ▼
┌────────────────────────────────────────────────────────┐
│      Partitioner: node_id = hash(host) % N             │
└───────────┬───────────────┬───────────────┬────────────┘
            ▼               ▼               ▼
     ┌───────────┐   ┌───────────┐   ┌───────────┐
     │  Node 0   │   │  Node 1   │   │ Node N-1  │  <-- Processing Nodes
     │  Engine   │   │  Engine   │   │  Engine   │      (In-Memory Window State)
     │  ckpt.json│   │  ckpt.json│   │  ckpt.json│
     └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
           │               │               │
           └───────────────┼───────────────┘
                           ▼
┌────────────────────────────────────────────────────────┐
│      Coordinator: merge(metrics) & State Inspector     │
│        (Completeness / Latency / Drops / Skew)         │
└────────────────────────────────────────────────────────┘
```

### 2.1 Phân mảnh ngang (Horizontal Fragmentation)
Theo lý thuyết thiết kế CSDL phân tán `[Ö&V, Ch. 3]`, luồng dữ liệu được phân chia dựa trên trường khóa `host` (tên miền/IP của client gửi request).
* **Quy tắc định tuyến**: `node_id = hash(host) % N`.
* **Mục đích**: Đảm bảo tính nhất quán cục bộ (State Locality). Toàn bộ dữ liệu của một host cụ thể sẽ được đưa về duy nhất một node xử lý, giúp việc thống kê theo host không cần trao đổi dữ liệu chéo giữa các node xử lý (shuffle), tối ưu băng thông mạng.
* **Độ lệch tải (Hot-key Skew)**: Đo lường bằng sự chênh lệch số lượng bản ghi giữa node nhận tải lớn nhất và nhỏ nhất so với tổng tải:
  $$\text{Skew} = \frac{\max(P_i) - \min(P_i)}{\sum P_i} \times 100\%$$
  Trong thực tế dữ liệu NASA, một vài host (ví dụ các proxy lớn hoặc trang tìm kiếm) có tần suất request cực kỳ vượt trội, tạo ra hiện tượng lệch tải (skew) rõ rệt.

---

## 3. Cơ Chế Watermark & Xử Lý Cửa Sổ

### 3.1 Trục thời gian: Event-Time vs. Arrival-Time
* **Event-time ($t_e$)**: Thời điểm sự kiện thực sự xảy ra trên máy khách (client), được ghi nhận trong log.
* **Arrival-time ($t_a$)**: Thời điểm log đi tới hệ thống stream xử lý. Độ trễ truyền thông là $d = t_a - t_e$. Dữ liệu tới bị lệch thứ tự khi tồn tại hai sự kiện $e_1, e_2$ thỏa mãn $t_e(e_1) < t_e(e_2)$ nhưng $t_a(e_1) > t_a(e_2)$.

### 3.2 Định lý Watermark
Để biết khi nào có thể đóng một cửa sổ tumbling kích thước $W$ (ví dụ: $[0, 10s)$), hệ thống sử dụng khái niệm **Watermark ($WM$)**:
$$WM = \max(t_e) - \text{allowed\_lateness}$$

* Khi nhận được sự kiện mới có $t_e$, hệ thống cập nhật $\max(t_e) = \max(\max(t_e), t_e)$.
* Giá trị Watermark tịnh tiến tăng dần. Khi $WM \ge \text{window\_end}$, cửa sổ đó được coi là **đã đóng hoàn toàn**. Hệ thống chốt kết quả thống kê của cửa sổ và phát xạ (emit) kết quả ra ngoài.
* **Xử lý sự kiện trễ (Late Events)**: Nếu một sự kiện tới hệ thống sau khi cửa sổ chứa nó đã đóng ($ws < WM - W$), sự kiện này bị coi là quá trễ và sẽ bị loại bỏ hoàn toàn (Late Dropped) để đảm bảo tính hữu hạn của tài nguyên bộ nhớ.

---

## 4. Quản Lý Trạng Thái & Khả Năng Chịu Lỗi (Fault Tolerance)

Để đáp ứng tiêu chí **State Management** mức xuất sắc trong rubric `[Ö&V, Ch. 12 - Distributed Reliability]`, hệ thống cài đặt hai kỹ thuật then chốt:

### 4.1 Checkpoint Atomic (Nhất quán & Bền vững)
Mỗi node xử lý lưu trữ trạng thái in-memory gồm các cửa sổ đang mở (`open_windows`), các cửa sổ đã đóng (`closed_windows`) và tập hợp các ID sự kiện đã thấy (`seen_ids`). 
* Định kỳ sau mỗi $K$ bản ghi, node tiến hành ghi lại checkpoint xuống đĩa.
* **Ghi Atomic**: Tránh tình trạng file checkpoint bị lỗi hoặc mất dữ liệu khi tiến trình bị sập đúng lúc đang ghi file (partial write). Hệ thống ghi dữ liệu vào một file tạm `.tmp`, sau khi hoàn tất mới gọi hàm đổi tên hệ thống `os.replace` sang file checkpoint chính thức. Đây là thao tác nguyên tử (atomic write) được hỗ trợ bởi hệ điều hành.

### 4.2 Cơ chế Dead-Letter Queue (DLQ) & Khôi phục Exactly-Once
Khi một node trong cluster bị sập (trạng thái `dead` do coordinator giả lập hoặc lỗi phần cứng thực tế):
1. **Phát hiện & Chuyển hướng**: Partitioner tiếp tục băm log về node đó. Tuy nhiên, Coordinator nhận biết node đã chết và không drop bỏ dữ liệu. Thay vào đó, Coordinator ghi nối tiếp (append-only) các log này vào file **Dead-Letter Queue (DLQ)** trên đĩa tương ứng của node: `./.simdata/dlq/dlq_node_i.jsonl`.
2. **Khôi phục trạng thái (Revive)**: Khi node được kích hoạt trở lại:
   * Node đọc file checkpoint atomic cuối cùng để khôi phục lại toàn bộ trạng thái cửa sổ và tập hợp `seen_ids` tại thời điểm checkpoint gần nhất.
   * Node đọc tuần tự các sự kiện trong file DLQ trên đĩa để **replay** (xử lý lại).
   * **Exactly-Once Semantics**: Do checkpoint được lưu tại bản ghi thứ $X$ trước khi sập, và một phần log sau đó đã được ghi vào DLQ, việc replay có thể gây trùng lặp. Tập hợp `seen_ids` (Deduplication Store) sẽ lọc bỏ toàn bộ các bản ghi đã được xử lý trước đó, đảm bảo kết quả đếm cuối cùng không bị nhân đôi và cũng không bị mất mát dữ liệu. Sau khi replay xong, file DLQ được xóa để giải phóng dung lượng đĩa.

---

## 5. Kiểm Soát Lưu Lượng & Quá Tải (Backpressure)

Trong xử lý luồng phân tán, sự mất cân bằng giữa tốc độ sản xuất dữ liệu ($\lambda$) và tốc độ tiêu thụ của engine xử lý ($\mu$) sẽ dẫn tới tràn bộ nhớ.
Hệ thống tích hợp hàng đợi có giới hạn (`max_queue`):
* Khi kích thước hàng đợi vượt quá giới hạn, hệ thống kích hoạt cơ chế **Backpressure**.
* **Chính sách tải lỗi**: Hệ thống thực hiện chiến lược **Load Shedding** (chủ động loại bỏ các bản ghi mới nhất và tăng biến đếm `backpressure_drops`) để bảo vệ tài nguyên RAM của hệ thống, tránh lỗi OOM (Out Of Memory) crash. Khi tải giảm, hàng đợi co lại dưới ngưỡng, hệ thống tự động nhận dữ liệu bình thường trở lại.

---

## 6. Kết Quả Thực Nghiệm & Phân Tích Đánh Đổi (PACELC)

Kết quả sweep thực nghiệm trên 200.000 sự kiện NASA-HTTP với kích thước cửa sổ $W = 10s$:

| Wait Time (ms) | Data Completeness % | Late dropped | Result latency (ms) | Proc p99 (µs) |
|---:|---:|---:|---:|---:|
| 0    | 97.18  | 5.638 | 66.091 | 5.62 |
| 100  | 98.28  | 3.439 | 74.118 | 5.19 |
| 250  | 98.28  | 3.439 | 74.118 | 4.84 |
| 500  | 98.28  | 3.439 | 74.118 | 4.73 |
| 1000 | 98.28  | 3.439 | 74.118 | 4.40 |
| 2000 | 98.97  | 2.055 | 78.266 | 4.29 |
| 4000 | 99.72  |   571 | 84.932 | 4.09 |
| 6000 | 99.96  |    74 | 89.851 | 3.96 |
| 8000 | **100.00** | 0 | 94.298 | 4.18 |

### Phân tích biểu đồ:
1. **Định luật ELC (Eventual Consistency vs. Latency)**: 
   * Tại mức **Wait Time = 0ms** (Heuristic cao nhất), kết quả có ngay lập tức (Result Latency thấp nhất ~66s) nhưng độ đầy đủ dữ liệu chỉ đạt **97.18%** (mất 5.638 events trễ).
   * Tại mức **Wait Time = 8000ms** (Strict nhất), độ đầy đủ đạt **100%** nhưng Result Latency tăng lên **94.29s** (chậm hơn 28s).
2. **Hiện tượng đi ngang (Completeness Plateau)**:
   * Số liệu thực tế cho thấy tỷ lệ đầy đủ đứng yên ở mức **98.28%** trong suốt dải Wait Time từ 100ms đến 1000ms. Điều này phản ánh phân phối trễ của log NASA có tính chất **bimodal**: Log hoặc đến rất nhanh (dưới 200ms) hoặc bị trễ hẳn trên 1000ms (do bão truyền dẫn hoặc retry kết nối). Việc cấu hình Wait Time nằm trong khoảng trống bimodal (ví dụ: 500ms) chỉ làm tăng thêm độ trễ hệ thống vô ích mà không gom thêm được bản ghi nào.
3. **Phân tích điểm nghẽn (Bottlenecks)**:
   * Thời gian xử lý thực tế mỗi bản ghi của engine cực kỳ nhanh (p99 chỉ khoảng **4 - 6 micro-giây**), chứng tỏ năng lực tính toán của CPU không phải là bottleneck.
   * Bottleneck trễ kết quả (Result Latency ~66 - 94s) hoàn toàn đến từ đặc tính dữ liệu log: Cửa sổ event-time phải chờ đợi sự xuất hiện của các bản ghi thuộc cửa sổ tiếp theo để kéo watermark vượt qua mốc đóng cửa sổ.

---

## 7. Kết Luận

Hệ thống đã chứng minh tính đúng đắn của mô hình lý thuyết xử lý stream phân tán:
* Đảm bảo tính nhất quán dữ liệu dựa trên event-time độc lập với thời gian xử lý thực tế.
* Đảm bảo tính bền vững và khả năng phục hồi Exactly-Once sau sự cố sập node mạng nhờ sự kết hợp giữa Checkpoint Atomic và Dead-Letter Queue.
* Cung cấp số liệu thực nghiệm định lượng rõ ràng cho việc cấu hình hệ thống tối ưu theo nguyên lý PACELC tùy theo nhu cầu thực tế (Dashboard thời gian thực ưu tiên Latency hay Báo cáo tài chính ưu tiên Consistency).
