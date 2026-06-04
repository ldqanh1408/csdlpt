# Bộ đo thời gian độ phân giải cao và Phát hiện điểm nghẽn hiệu năng (Bottlenecks)

Tài liệu này đặc tả chi tiết về các số liệu (metrics) và cơ chế đo đạc được sử dụng để phân tích độ trễ xử lý (processing latency) và định vị các điểm nghẽn hiệu năng trong dự án **Distributed Watermark Tracker**.

---

## 1. Bộ đo thời gian độ phân giải cao (`HighResTimer`)

Để đo thời gian thực thi ở mức dưới mili-giây (sub-millisecond) mà không bị ảnh hưởng bởi độ trễ hoặc độ không chính xác của các bộ đo thời gian hệ thống thông thường, dự án cài đặt lớp `HighResTimer`:
* **Vị trí định nghĩa**: [metrics.py](file:///D:/dev/csdlpt/common/metrics.py#L11-L21)
* **Hàm nền tảng**: `time.perf_counter_ns()` (độ chính xác đến nanosecond, sử dụng clock đơn điệu - monotonic reference clock)

```python
class HighResTimer:
    @staticmethod
    def now_ns() -> int:
        return time.perf_counter_ns()

    @staticmethod
    def elapsed_ms(start_ns: int) -> float:
        return (time.perf_counter_ns() - start_ns) / 1_000_000.0
```

---

## 2. Các số liệu độ trễ thu thập (đơn vị Nanosecond)

Mỗi sự kiện đi qua đường ống (pipeline) xử lý đều được giám sát. Thời gian thực thi của từng giai đoạn quan trọng được ghi nhận dưới dạng nanosecond trong lớp [SystemMetrics](file:///D:/dev/csdlpt/common/metrics.py#L25):

| Tên số liệu (Metric Name) | Giai đoạn đo đạc | Mô tả |
| :--- | :--- | :--- |
| `T_network_ingest_ns` | Nhận dữ liệu & Kafka Inbound | Thời gian nhận/ingest các sự kiện từ network/Kafka stream. |
| `T_poll_decode_ns` | Poll từ Kafka & Giải mã JSON | Thời gian poll Kafka và giải mã cấu trúc JSON (schema). |
| `T_deduplication_ns` | Kiểm tra trùng lặp (Deduplication) | Thời gian kiểm tra ID filter và đối chiếu với nhật ký RocksDB. |
| `T_state_write_ns` | Ghi trạng thái cục bộ | Thời gian ghi cập nhật cửa sổ (window updates) vào RocksDB cục bộ. |
| `T_sketch_update_ns` | Cập nhật DDSketch | Thời gian đưa độ trễ quan sát được vào phân vị DDSketch. |
| `T_sketch_query_ns` | Truy vấn DDSketch | Thời gian ước lượng độ trễ watermark hiệu dụng ($L_{eff}$). |

---

## 3. Thống kê tóm tắt mức Microsecond ($p_{50}$, $p_{95}$, $p_{99}$)

Để tránh quá tải dữ liệu truyền tải (telemetry overhead) trong khi vẫn cung cấp thông tin gỡ lỗi hữu ích, các mảng dữ liệu nanosecond thô được tổng hợp động thành các phân vị (percentiles, đo bằng microsecond - $\mu$s) khi có yêu cầu truy vấn metrics:

### A. Độ trễ tổng thể (General Latency)
* **`proc_latency_p50_us`** / **`proc_latency_p95_us`** / **`proc_latency_p99_us`**: Độ trễ tổng thể của toàn bộ vòng lặp xử lý sự kiện bên trong hàm `process()`.

### B. Độ trễ phân đoạn (Segmented Latency)
Việc so sánh các giá trị này giúp nhà phát triển định vị lập tức thành phần nào trong pipeline đang gây ra nghẽn hiệu năng:
* **Giai đoạn Poll & Decode**: `poll_decode_latency_p50_us` · `poll_decode_latency_p95_us` · `poll_decode_latency_p99_us`
* **Giai đoạn lọc trùng (Deduplication)**: `dedup_latency_p50_us` · `dedup_latency_p95_us` · `dedup_latency_p99_us`
* **Disk I/O của RocksDB**: `state_write_latency_p50_us` · `state_write_latency_p95_us` · `state_write_latency_p99_us`
* **Xử lý Watermark Heuristic**:
  * *Độ trễ cập nhật:* `sketch_update_latency_p50_us` · `p95` · `p99`
  * *Độ trễ truy vấn:* `sketch_query_latency_p50_us` · `p95` · `p99`

---

## 4. Nghiên cứu thực tế về điểm nghẽn: Windows Host Mount so với `tmpfs`

Trong quá trình chạy thực nghiệm trên hệ điều hành Windows sử dụng Docker containers với các thư mục chia sẻ từ máy host (`/data/checkpoint` ánh xạ trực tiếp tới các thư mục Windows trên WSL2), các số liệu telemetry đã chỉ ra một điểm nghẽn chí mạng:

1. **Phát hiện**: Chỉ số `state_write_latency_p99_us` tăng vọt lên **hàng mili-giây** (milliseconds), trong khi độ trễ `dedup_latency` và `sketch_update_latency` vẫn duy trì ổn định dưới 5 microseconds.
2. **Chẩn đoán**: Việc ghi các file SST và commit WAL của RocksDB xuống ổ đĩa NTFS của Windows thông qua lớp dịch hệ thống tệp của WSL2 gây ra một chi phí I/O (overhead) cực kỳ lớn.
3. **Khắc phục**: Thư mục làm việc tạm thời của RocksDB (`CHECKPOINT_DIR`) đã được cấu hình lại để trỏ vào thư mục `/tmp` và được mount dưới dạng `tmpfs` (RAM-disk) bên trong các container profile tại file `docker-compose.yml`.
4. **Xác nhận**: Sau khi chuyển sang sử dụng `tmpfs`, chỉ số `state_write_latency_p99_us` đã giảm đi **10 lần**, chứng minh tính hiệu quả của việc phân tích độ trễ độ phân giải cao trong việc tìm kiếm và khắc phục các giới hạn về hiệu năng.
