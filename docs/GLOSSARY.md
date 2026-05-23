# Thuật ngữ Hệ thống chi tiết (Comprehensive System Glossary) — Distributed Watermark Tracker

Tài liệu này là cẩm nang định nghĩa toàn bộ thuật ngữ kỹ thuật, biến số, cấu hình, lớp (class), hàm (function) và các khái niệm lý thuyết cốt lõi được sử dụng trong hệ thống xử lý stream phân tán xử lý log ngoài thứ tự.

---

## 1. Khái niệm cốt lõi về xử lý luồng (Core Stream Processing)

### 1.1. Trục thời gian (Time Semantics)
*   **Event-time (Thời gian sự kiện)**: 
    *   *Định nghĩa*: Thời điểm thực tế sự kiện (log) xảy ra tại nguồn phát (Client/Server). Mốc thời gian này được ghi thẳng vào dữ liệu của log (ví dụ: trường `event_time` trong tập dữ liệu log NASA HTTP).
    *   *Kỹ thuật*: Là trục tham chiếu chính được hệ thống dùng để gom nhóm dữ liệu vào các cửa sổ.
*   **Arrival-time (Thời gian đến)**:
    *   *Định nghĩa*: Thời điểm log đến được hệ thống xử lý stream (Engine). Trục thời gian này luôn tăng dần tuyến tính theo thời gian thực tại Engine.
    *   *Đặc điểm*: Do độ trễ mạng hoặc tắc nghẽn hàng đợi, các log có Event-time cũ hơn có thể có Arrival-time muộn hơn, gây ra hiện tượng lệch thứ tự (**Out-of-order**).
*   **Processing-time (Thời gian xử lý)**:
    *   *Định nghĩa*: Thời điểm CPU của máy chạy thực tế xử lý log đó.

### 1.2. Watermark (Thước đo thời gian sự kiện)
*   **Watermark**:
    *   *Định nghĩa*: Một mốc thời gian động (biên thời gian) đại diện cho tiến trình Event-time trong hệ thống. Watermark được tính dựa trên Event-time lớn nhất hệ thống đã thấy trừ đi khoảng trễ cho phép.
    *   *Ý nghĩa*: Khi Watermark đạt mốc $T$, hệ thống giả định rằng toàn bộ các sự kiện có $\text{Event-time} < T$ đều đã được nạp thành công và sẽ không còn sự kiện nào cũ hơn $T$ xuất hiện nữa.
    *   *Công thức*:
        $$\text{Watermark} = \max(\text{Event-Time đã thấy}) - \text{Allowed Lateness}$$
    *   *Cú pháp code*: Biến `self.watermark` trong lớp `WatermarkEngine` (tệp `wm/engine.py`).

### 1.3. Allowed Lateness / Wait Time (Độ trễ cho phép / Thời gian chờ)
*   **Allowed Lateness / Wait Time**:
    *   *Định nghĩa*: Khoảng thời gian đệm để đợi các log bị trễ mạng tới muộn trước khi chốt cửa sổ.
    *   *Trade-off (Đánh đổi)*:
        *   **Wait Time lớn (Strict)**: Bảo toàn dữ liệu, hầu như không bị loại bỏ log cũ, độ đầy đủ dữ liệu (**Completeness**) đạt gần 100%. Tuy nhiên, kết quả đầu ra bị trễ lâu hơn (**Result Latency** cao).
        *   **Wait Time nhỏ (Heuristic)**: Nhận kết quả nhanh hơn (**Result Latency** thấp), nhưng nếu có log bị tắc nghẽn mạng quá thời hạn này, chúng sẽ bị từ chối xử lý.
    *   *Cú pháp code*: Biến `allowed_lateness_s` trong cấu hình `EngineConfig` (tệp `wm/config.py`).

### 1.4. Windowing (Chia cửa sổ thời gian)
*   **Event-time Windowing**: Nhóm luồng log vô hạn thành các khối hữu hạn dựa trên Event-time.
*   **Tumbling Window (Cửa sổ dốc)**: Cửa sổ có kích thước cố định, không chồng lấn lên nhau (ví dụ: các khoảng 10 giây liên tiếp: [00:00 - 10:00], [10:00 - 20:00]).
    *   *Công thức tính mốc bắt đầu cửa sổ*:
        $$\text{Window Start} = \text{Event-Time} - (\text{Event-Time} \pmod{\text{Window Size}})$$
    *   *Cú pháp code*: Hàm `window_start_for(self, ts)` trong lớp `WatermarkEngine`.
*   **Window Size (`window_size_s`)**: Cấu hình độ rộng của cửa sổ, tính bằng giây. Biến `self.window_size` trong code.

### 1.5. Xử lý log trễ (Late Events)
*   **Late Event (Sự kiện đến muộn)**: Bản ghi có Event-time nhỏ hơn mốc cửa sổ đã bị chốt và đóng lại do Watermark đã vượt qua.
    *   *Điều kiện*: $\text{Event-Time của log} < \text{Watermark hiện tại}$.
*   **Late Drop (Loại bỏ log trễ)**: 
    *   *Định nghĩa*: Hành vi từ chối xử lý và bỏ qua Late Event. 
    *   *Lý do*: Vì cửa sổ cũ đã chốt kết quả và xóa trạng thái tích lũy khỏi RAM để giải phóng bộ nhớ. Nếu không drop, RAM sẽ bị tràn (OOM) khi phải giữ trạng thái của hàng triệu cửa sổ cũ trong quá khứ vô thời hạn.
    *   *Ví dụ chuyến tàu*: Tàu (Window) chạy lúc 10:00, ga mở cửa đợi muộn 5 phút (Wait Time). Đến 10:05 tàu chạy (Watermark chốt). Hành khách (Log) bị tắc đường đến ga lúc 10:15 sẽ bị từ chối cho lên tàu (**Late Drop**).
    *   *Cú pháp code*: Trường `self.metrics["late_dropped"]` đếm số lượng log bị loại bỏ do trễ.

---

## 2. Quản lý trạng thái và Độ tin cậy (State Management & Reliability)

### 2.1. State Store (Kho lưu trữ trạng thái)
*   **State Store**:
    *   *Định nghĩa*: Vùng bộ nhớ trung gian lưu giữ kết quả tính toán tạm thời của các cửa sổ thời gian chưa chốt (ví dụ: đếm số sự kiện, đếm lỗi 500).
    *   *Kỹ thuật*: Được triển khai dưới dạng `defaultdict(WindowState)` trong biến `self.windows` (tệp `wm/engine.py`).
*   **WindowState**: Lớp lưu giữ số sự kiện (`count`) và lỗi hệ thống (`status_500`) của từng cửa sổ.

### 2.2. Checkpoint & Atomic Checkpoint (Điểm kiểm tra nguyên tử)
*   **Checkpoint**: Bản sao lưu trạng thái hiện tại của Engine ra đĩa cứng tại một thời điểm nhất định để phục hồi nếu hệ thống bị sập.
*   **Checkpoint Interval (`checkpoint_interval`)**: Số lượng log được xử lý giữa mỗi lần tự động lưu checkpoint (mặc định là mỗi 200 hoặc 1000 logs).
*   **Atomic Checkpointing**: 
    *   *Định nghĩa*: Cơ chế lưu trạng thái an toàn trước lỗi nửa chừng. Hệ thống sẽ ghi dữ liệu ra một file tạm có đuôi `.tmp` trước, chỉ khi ghi thành công hoàn toàn mới thực hiện ghi đè nguyên tử thay thế file checkpoint chính thức bằng lệnh `os.replace`. 
    *   *Lợi ích*: Nếu node bị mất điện đột ngột hoặc crash khi đang ghi checkpoint, file checkpoint chính vẫn nguyên vẹn, tránh tình trạng file bị hỏng dữ liệu (corruption).
    *   *Cú pháp code*: Hàm `checkpoint()` trong lớp `WatermarkEngine` (tệp `wm/engine.py`).

### 2.3. Lọc trùng lặp & Phục hồi (De-duplication & Recovery)
*   **De-duplication / Dedup**: Bộ lọc loại bỏ các bản ghi trùng lặp (ví dụ do client gửi lại).
    *   *Kỹ thuật*: Sử dụng danh sách định danh sự kiện duy nhất `seen_ids` lưu trong checkpoint. Khi một log mới đến, hệ thống kiểm tra nếu `event_id` nằm trong `seen_ids` thì sẽ bỏ qua để tránh đếm lặp.
    *   *Cú pháp code*: Biến `self.seen_ids` và trường `self.metrics["duplicates"]` trong `WatermarkEngine`.
*   **Exactly-Once Semantics (Ngữ nghĩa xử lý chính xác một lần)**: Đảm bảo dữ liệu đầu ra chính xác tuyệt đối ngay cả khi xảy ra crash. Đạt được nhờ sự kết hợp giữa **Atomic Checkpointing** và **De-duplication**.
*   **Restore**: Quá trình khởi tạo lại Engine bằng cách nạp lại file checkpoint từ đĩa khi Node hồi sinh.
    *   *Cú pháp code*: Phương thức tĩnh `WatermarkEngine.restore(checkpoint_path, ...)` (tệp `wm/engine.py`).

### 2.4. Dead-Letter Queue (DLQ - Hàng đợi thư chết)
*   **Dead-Letter Queue (DLQ)**:
    *   *Định nghĩa*: Một tệp tin đệm lưu trữ dữ liệu tạm thời trên đĩa dưới định dạng append-only JSONL khi một Node trong cụm bị chết.
    *   *Cơ chế hoạt động*: Khi Node $i$ bị sập (`dead`), Coordinator nhận diện trạng thái và thay vì tiếp tục gửi log vào RAM của Node $i$ (gây mất dữ liệu hoặc crash cụm), nó sẽ ghi tuần tự các log được định tuyến cho Node $i$ vào file DLQ: `./.simdata/dlq/dlq_node_[id].jsonl`.
    *   *Replay (Phát lại)*: Khi Node $i$ được hồi sinh (`revive`), nó sẽ khôi phục trạng thái từ Checkpoint, sau đó đọc toàn bộ dữ liệu trong file DLQ để xử lý bù lại, bảo đảm Exactly-Once. Sau khi replay thành công, file DLQ được xóa.
    *   *Cú pháp code*: Hàm `revive_node()` trong tệp `wm/actions.py`.

---

## 3. Kiểm soát luồng & Phân tán (Flow Control & Distribution)

### 3.1. Phân hoạch ngang (Horizontal Partitioning)
*   **Horizontal Partitioning**: Kỹ thuật phân chia dữ liệu lớn thành các phần nhỏ để phân phối xử lý song song.
*   **Hash Partitioning**:
    *   *Định nghĩa*: Định tuyến dữ liệu đến các Node xử lý dựa trên thuật toán băm (MD5/MurmurHash) của một thuộc tính (khóa định tuyến - Route Key, trong dự án này là cột `host` hoặc `endpoint`).
    *   *Công thức*:
        $$\text{Node ID} = \text{MD5}(host) \pmod{\text{Số Node}}$$
    *   *Cú pháp code*: Hàm `partition_key(host, n_nodes)` (tệp `wm/partition.py`).

### 3.2. Cân bằng tải & Lệch tải (Load Balancing & Key Skew)
*   **Hot-key Skew (Lệch tải khóa nóng)**: 
    *   *Định nghĩa*: Hiện tượng mất cân đối tải trọng trong cụm. Khi một hoặc một vài client (ví dụ: một địa chỉ IP server lớn) tạo ra phần lớn log, thuật toán băm sẽ định tuyến toàn bộ log này về cùng một Node duy nhất, khiến Node này quá tải (Hot node) trong khi các Node khác nhàn rỗi.
    *   *Cách đo đạc trong dự án*:
        $$\text{Skew \%} = 100 \times \frac{\max(\text{Tải của Node}) - \min(\text{Tải của Node})}{\text{Tổng số log}}$$
    *   *Cú pháp code*: Biến `skew_pct` tính toán phân phối tải trọng trong tab `Distributed Cluster` (tệp `app.py`).

### 3.3. Áp lực ngược (Backpressure)
*   **Backpressure**:
    *   *Định nghĩa*: Cơ chế bảo vệ hệ thống trước tình trạng quá tải. Khi tốc độ ghi log đầu vào lớn hơn khả năng tính toán của Engine, hàng đợi buffer sẽ bị phình to.
    *   *Cơ chế trong code*: Engine giới hạn dung lượng hàng đợi `max_queue`. Khi số lượng bản ghi trong hàng đợi vượt quá ngưỡng, hệ thống sẽ thực hiện loại bỏ có kiểm soát (drop log tại hàng đợi, tăng biến đếm `backpressure_drops`) để bảo vệ RAM, tránh việc Engine bị OOM crash.
    *   *Trade-off*: Hy sinh một phần độ đầy đủ (Completeness) của luồng log hiện tại để giữ cho toàn bộ hệ thống phân tán được sống sót ổn định (Robustness).
    *   *Cú pháp code*: Tham số `max_queue` và biến đếm `backpressure_drops` trong `WatermarkEngine`.

---

## 4. Mô phỏng Kịch bản sự cố (Simulation DSL)

*   **Scenario (Kịch bản)**: Lớp đối tượng biểu diễn một kịch bản mô phỏng kiểm thử hệ thống, bao gồm số lượng Node và một danh sách các hành động lập lịch theo thời gian.
    *   *Cú pháp code*: Lớp `Scenario` (tệp `wm/scenario.py`).
*   **ScenarioAction (Hành động kịch bản)**: Một sự kiện xảy ra tại một thời điểm cursor (chỉ số log) xác định.
    *   *Các loại Action*:
        *   `kill`: Sập Node xử lý (gây mất kết nối, dữ liệu hướng đến Node này chuyển vào DLQ).
        *   `revive`: Hồi sinh Node (khôi phục checkpoint và replay DLQ).
        *   `ooo_spike` (Bão log lệch thứ tự): Giả lập độ trễ truyền thông tăng vọt bằng cách đảo lộn vị trí/thời gian của một nhóm log kế tiếp.
        *   `load_burst` (Tải đột biến): Đột ngột tăng số lượng log nạp vào trong mỗi giây để kích hoạt Backpressure.
        *   `delay_inject` (Trễ mạng truyền thông): Bơm thêm độ trễ tĩnh vào thời gian nhận của log để stress test thuật toán watermark.
*   **Scenario Presets (Kịch bản mẫu tích hợp sẵn - tệp `wm/presets.py`)**:
    *   **Chaos Cascade**: Lần lượt sập tuần tự các Node, sau đó phục hồi theo thứ tự ngược lại để kiểm thử độ ổn định khi mất mát dây chuyền.
    *   **Network Split**: Giả lập phân mảnh mạng bằng cách sập đồng thời một nửa số Node trong cụm, sau đó khôi phục đồng loạt.
    *   **Slow Recovery**: Sập một Node từ rất sớm và chỉ hồi sinh vào cuối luồng dữ liệu nhằm tích lũy một file DLQ khổng lồ trên đĩa để kiểm tra khả năng phục hồi tải nặng.
    *   **Full Apocalypse**: Toàn bộ Node trong cụm bị sập đồng loạt rồi cùng khôi phục lại.
    *   **OOO Storm**: Tạo bão dữ liệu lệch thứ tự nghiêm trọng (lên tới 90% log bị lệch vị trí) nhằm kiểm chứng khả năng tự thích ứng của thuật toán Watermark.

---

## 5. Thống kê và Phân tích hiệu năng (Performance Metrics)

*   **Processing Latency (Độ trễ xử lý)**: 
    *   *Định nghĩa*: Khoảng thời gian Engine tiêu tốn để xử lý một log đơn lẻ (tính toán cửa sổ, lọc trùng, watermark).
    *   *Cách đo*: Đo bằng đồng hồ CPU độ phân giải cao `time.perf_counter_ns()` tại đầu và cuối hàm `process()`.
    *   *Cú pháp code*: Mảng `self.proc_latencies_ns` lưu trữ dữ liệu thô, được tính phân vị p50 và p99 hiển thị ra giao diện (`proc_latency_p99_us` tính bằng micro-giây).
*   **Result Latency (Độ trễ kết quả)**:
    *   *Định nghĩa*: Khoảng Event-time chênh lệch từ khi một cửa sổ kết thúc cho đến khi nó thực sự được Watermark chốt phát ra kết quả.
    *   *Công thức*:
        $$\text{Result Latency} = \text{Event-Time lớn nhất đã nhận} - \text{Mốc kết thúc cửa sổ}$$
    *   *Ý nghĩa*: Phản ánh tốc độ cập nhật báo cáo. Độ trễ kết quả trung bình được hiển thị là `avg_result_latency_ms`.
*   **Completeness % (Độ đầy đủ)**:
    *   *Định nghĩa*: Tỷ lệ phần trăm log được phân bổ đúng vào cửa sổ thành công (không bị drop do trễ hay quá tải).
    *   *Công thức*:
        $$\text{Completeness} = 100 \times \frac{\text{Số log On-Time}}{\text{Số log duy nhất (Unique)}}$$
*   **PACELC Theorem (Định lý PACELC)**:
    *   *Lý thuyết áp dụng*: Hệ thống stream của dự án khi phân hoạch mạng xảy ra (**P**) (ví dụ Node chết), Coordinator hứng log ghi vào DLQ trên đĩa thay vì phản hồi ngay lập tức $\rightarrow$ Đánh đổi tính sẵn sàng (**A**vailability) lấy tính nhất quán tuyệt đối (**C**onsistency). 
    *   Khi mạng bình thường (**E**lse), thời gian chờ allowed lateness lớn sẽ tăng tính nhất quán (**C**onsistency) nhưng tăng độ trễ kết quả (**L**atency) $\rightarrow$ Chứng minh trực quan bằng núm xoay slider `Wait Time` điều khiển đường cong đánh đổi.
