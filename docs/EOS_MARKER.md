# Giải pháp Đồng bộ Kết thúc Luồng (End-of-Stream Barrier & Coordination)

Tài liệu này trình bày thiết kế kiến trúc và giải pháp kỹ thuật giải quyết bài toán chốt sổ cuối luồng dữ liệu (End of Stream - EOS) trong hệ thống xử lý phân tán, tránh tình trạng mất mát dữ liệu do tín hiệu kết thúc vượt mặt dữ liệu thực tế.

---

## 1. Vấn đề "Tín hiệu vượt mặt Dữ liệu" (Out-of-band Signaling Issue)

Khi nguồn phát log (Ingestor) muốn thông báo cho các Node xử lý rằng dữ liệu đã hết để chốt kết quả, phương án đơn giản nhất là gọi một Webhook HTTP độc lập (ví dụ: `POST /api/end-of-stream`). Tuy nhiên, phương án này gặp phải lỗi đồng thì nguy hiểm (**Race Condition**):

*   **Hiện tượng:** Tín hiệu Webhook (nhẹ, đi đường truyền ưu tiên) có thể đến Node nhận *trước* khi các log cuối cùng (nặng hơn hoặc đang bị kẹt trong hàng đợi mạng) kịp đến nơi.
*   **Hậu quả:** Node nhận được tín hiệu hết dữ liệu $\rightarrow$ lập tức gọi `flush()` để chốt toàn bộ các cửa sổ thời gian. Các log thực tế đến sau đó (thực ra thuộc stream cũ nhưng bị trễ) sẽ bị coi là sự kiện đến muộn (**Late Event**) và bị hệ thống loại bỏ hoàn toàn (**Late Drop**).

---

## 2. Giải pháp Đề xuất: End-of-Stream (EOS) Marker

Để khắc phục rủi ro trên, hệ thống áp dụng cơ chế **In-band Signaling (Tín hiệu lồng trong luồng dữ liệu)** dưới dạng các **EOS Marker** (còn gọi là *Barrier* trong các hệ thống như Apache Flink).

```text
[ Nguồn Phát ]
    │  1. Gửi Log 1, Log 2, ..., Log Cuối, rồi gửi EOS_MARKER
    ▼
[ Partitioner (Bộ định tuyến băm) ]
    │  2. Định tuyến theo hash(host)%N
    ▼
[ Hàng đợi của các Node (FIFO Queue) ]
   ┌────────────────────────────────────────────────────────┐
   │  ... ◄── [Log Cuối] ◄── ... ◄── [Log 2] ◄── [EOS_MARKER] │ (EOS đi cuối cùng)
   └────────────────────────┬───────────────────────────────┘
                            ▼
                  [ Node i (Engine i) ] 
                            │ 3. Nhận EOS -> Tự gọi flush() -> Chốt các window còn lại
                            ▼ 4. Gọi Webhook báo Coordinator
                    [ Coordinator ] (Nhận đủ N báo cáo từ N Node -> Hoàn tất!)
```

### Nguyên lý hoạt động:
*   Bản ghi báo hiệu kết thúc được gửi **sau cùng** và đi chung trên cùng một luồng dữ liệu với log bình thường.
*   Hàng đợi mạng vận chuyển theo nguyên tắc **FIFO (First-In, First-Out)**, đảm bảo bản ghi EOS chỉ được Node xử lý sau khi Node đã hoàn thành 100% các log thường trước đó.

---

## 3. Quy trình điều phối từng bước (Step-by-Step Coordination)

### Bước 1: Nguồn phát gửi bản ghi EOS
Khi gửi xong bản ghi dữ liệu thực tế cuối cùng, Nguồn phát gửi thêm các bản ghi đặc biệt có cấu trúc định dạng EOS. 
*   *Lưu ý:* Cụm có $N$ Node. Nguồn phát cần gửi các bản ghi EOS gắn với các khóa (key) khác nhau sao cho thuật toán băm phân mảnh `hash(key) % N` sẽ đưa ít nhất một bản ghi EOS đến từng Node trong cụm.

### Bước 2: Node nhận diện EOS và tự đóng sổ cục bộ (Local Flush)
Khi Node $i$ nhận được một bản ghi có trường dữ liệu đặc biệt (ví dụ: `status == "EOS"`):
1.  Node dừng nhận dữ liệu mới từ luồng chính.
2.  Node tự gọi hàm `flush()` để cưỡng bức watermark nhảy lên vô hạn ($+\infty$). Hành động này đóng và chốt kết quả của toàn bộ các cửa sổ thời gian đang mở dở dang trong RAM.
3.  Node thực hiện một cuộc gọi Webhook thông báo cho Coordinator: `POST /api/node-completed?node_id=i`.

### Bước 3: Coordinator đồng bộ rào cản (Barrier Synchronization)
Bộ điều phối trung tâm (Coordinator) duy trì danh sách trạng thái hoàn thành của các Node.
1.  Mỗi khi nhận được cuộc gọi từ Node báo hoàn thành, Coordinator ghi nhận Node đó vào danh sách.
2.  Khi danh sách nhận đủ báo cáo từ cả $N$ Node trong cụm $\rightarrow$ Coordinator chính thức công bố luồng stream kết thúc thành công và tiến hành gộp kết quả cuối cùng (Completeness, Latency, Skew).

---

## 4. Mã giả tham khảo (Pseudocode)

### Tại Nguồn phát (Client)
```python
# 1. Gửi toàn bộ dữ liệu thực tế
for log in raw_data:
    send_to_cluster(log)

# 2. Phát tán EOS Marker tới tất cả N node
# Tạo các key khác nhau để thuật toán băm phân phối đều đến từng node
for node_id in range(n_nodes):
    eos_log = {
        "event_id": f"EOS_LIMIT_{node_id}",
        "event_time": 9999999999, # Mốc thời gian rất lớn
        "status": "EOS",
        "route_key": f"key_for_node_{node_id}" # Băm trúng node_id
    }
    send_to_cluster(eos_log)
```

### Tại Node xử lý (Engine)
```python
def process_record(self, event):
    if event["status"] == "EOS":
        # Nhận diện tín hiệu kết thúc
        self.flush() # Đẩy watermark lên +inf, đóng toàn bộ window
        self.report_completed_to_coordinator()
        return
        
    # Xử lý log bình thường...
    self.windows[ws].count += 1
```

### Tại Coordinator
```python
completed_nodes = set()

def handle_node_completed_webhook(node_id):
    completed_nodes.add(node_id)
    if len(completed_nodes) == total_nodes:
        # Nhận đủ N tín hiệu hoàn thành
        trigger_final_aggregate_report()
```
