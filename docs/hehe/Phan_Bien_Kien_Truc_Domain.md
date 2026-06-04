# Phản biện kiến trúc theo Domain — Vì sao thiết kế từng domain như vậy

**Dự án**: Distributed Watermark Tracker / Log Delay Compensator
**Phạm vi**: Biện giải (phản đề → bác bỏ → lý do) cho cách phân rã hệ thống thành 5 domain chức năng.

> **Quy ước về sơ đồ**: Toàn bộ mã nguồn sơ đồ được tách riêng ra thư mục [`mermaid/`](mermaid/) dưới dạng file `.mmd`. File `.md` này **chỉ trích dẫn** tới chúng, không nhúng trực tiếp.
> Cách render một sơ đồ: `mmdc -i mermaid/<ten_file>.mmd -o <ten_file>.svg` (mermaid-cli), hoặc dán nội dung `.mmd` vào trình xem mermaid bất kỳ.

---

## 1. Luận điểm gốc: vì sao chia theo *domain* (plane chức năng) thay vì theo *node*?

Hệ thống **không** mô tả theo máy vật lý (node0, node1…) mà theo **5 domain = các mặt phẳng (plane) chức năng**. Lý do then chốt: chia theo plane cho phép **hai chiến lược watermark dùng chung phần lớn hệ thống, chỉ hoán đổi đúng một plane điều khiển**.

| Domain | Plane | Strict & Heuristic dùng chung? |
| :--- | :--- | :--- |
| **D1 — Data Ingestion** | Data plane | ✅ Chung |
| **D2 — Strict Coordination** | Control plane | ⛔ Chỉ Strict |
| **D3 — Heuristic Aggregation** | Control plane | ⛔ Chỉ Heuristic |
| **D4 — Control & Metadata** | Metadata plane | ✅ Chung |
| **D5 — State & Storage** | State plane | ✅ Chung |

→ Đổi mode = **rút D2 ra, cắm D3 vào**, giữ nguyên D1/D4/D5. Đây chính là thứ làm "một hệ thống, hai chiến lược" khả thi mà không nhân đôi data/storage.

> 📊 Sơ đồ tổng quan: [`mermaid/domain_overview.mmd`](mermaid/domain_overview.mmd)

---

## 2. Phản biện từng domain

### D1 — Data Ingestion: vì sao phải qua Kafka, không đẩy thẳng vào Worker?

> 📊 Sơ đồ: [`mermaid/domain1_data_ingestion.mmd`](mermaid/domain1_data_ingestion.mmd)

**Phản đề:** Ingestor đọc CSV rồi gọi HTTP đẩy thẳng sự kiện tới Worker cho nhanh, cần gì Kafka ở giữa?

**Bác bỏ:** Đẩy thẳng thì khi Worker sập, **mọi sự kiện đang bay mất sạch** — không có nơi nào giữ lại để đọc lại. Cũng không có **offset** → không replay được → **không thể đạt exactly-once input**. Và khi Worker backpressure, Ingestor không có chỗ "đệm" nên phải chặn hoặc drop.

**Lý do:** Kafka là **data plane bền**: replication factor 3 (không mất input), **offset cho phép `seek(offset+1)` khi recovery** (nền tảng exactly-once), và **tách tốc độ** producer/consumer (Worker pause/resume mà Kafka vẫn giữ log trên đĩa broker). Việc **tách Ingestor thành tiến trình riêng** (không cho Worker tự đọc CSV) cũng có lý do: cô lập trách nhiệm gán `T_event` + phát Punctuation + băm `hash(host)%12` để cân tải — Worker chỉ lo tính toán.

---

### D2 — Strict Coordination: vì sao cần control plane riêng + Raft, không để Worker tự chốt?

> 📊 Sơ đồ: [`mermaid/domain2_strict_coordination.mmd`](mermaid/domain2_strict_coordination.mmd)

**Phản đề:** Mỗi Worker biết watermark partition của mình rồi, tự chốt cửa sổ là xong, cần gì Coordinator tập trung?

**Bác bỏ:** Worker chỉ thấy **partition của riêng nó**. Nếu nó tự chốt cửa sổ `[100,105]` khi watermark *cục bộ* qua 105, nhưng một partition **chậm ở node khác** mới tới mốc 102 → cửa sổ đó vẫn còn data chưa đến → **chốt sớm = mất data**. Phải có **một điểm hợp nhất** tính `W_global = min` trên *toàn bộ* partition.

**Lý do:** Tách **control plane (sự thật toàn cục) khỏi data plane (xử lý cục bộ)** — Worker = "công nhân", Coordinator = "nguồn chân lý watermark". Và phải là **Raft** (không phải lock thường) vì control plane này còn điều phối failback/reassign → cần `term` (fencing token) + replicate-trước-khi-thực-thi để giữ **exactly-once + single-owner**.

---

### D3 — Heuristic Aggregation: vì sao tách riêng D3, không gộp chung control plane với D2?

> 📊 Sơ đồ: [`mermaid/domain3_heuristic_aggregation.mmd`](mermaid/domain3_heuristic_aggregation.mmd)

**Phản đề:** D2 và D3 đều là "tính watermark toàn cục". Gộp làm một control plane dùng chung cho đỡ trùng code?

**Bác bỏ:** Hai mode có **cam kết đối nghịch** (Strict = PC/EC, Heuristic = PA/EL). Gộp lại buộc phải chọn **một** cường độ điều phối: nếu chọn Raft → ép Heuristic gánh chi phí đồng thuận, **giết mục tiêu latency <5s**; nếu chọn lock nhẹ → ép Strict mất fencing, **vỡ exactly-once**. Không có control plane "một cỡ vừa cả hai".

**Lý do:** Tách D2/D3 để **mỗi control plane đúng cường độ với cam kết của nó**, nhưng **dùng chung** D1 (data) và D5 (storage) nên không hề trùng phần lõi. **DLQ thuộc riêng D3** vì chỉ Heuristic mới chấp nhận loss → mới cần đường sửa lỗi; nhét DLQ vào D2 sẽ vô nghĩa (Strict không có data muộn).

---

### D4 — Control & Metadata: vì sao tách metadata/ingestor-heartbeat thành domain riêng?

> 📊 Sơ đồ: [`mermaid/domain4_control_metadata.mmd`](mermaid/domain4_control_metadata.mmd)

**Phản đề:** Ingestor-heartbeat và ZooKeeper cứ nhét vào D2/D3 cho gọn, sao phải lập domain thứ 4?

**Bác bỏ:** Hai lý do cụ thể. **(1)** Nếu ingestor-heartbeat đi **cùng kênh gRPC** với watermark vào Coordinator → tất cả ingestor fan-in một điểm gây **nghẽn hội tụ (fan-in bottleneck)**; nên nó được đẩy qua **một Kafka stream nhịp tim riêng** (gRPC chỉ là dự phòng). **(2)** ZooKeeper làm **election + service discovery cho CẢ hai mode** — nó là hạ tầng cross-cutting, không thuộc riêng strict hay heuristic.

**Lý do:** Tách **metadata plane** khỏi **watermark plane** để (a) tránh bottleneck, (b) gom các concern hạ tầng dùng chung (discovery, clock-skew monitor, health) vào một chỗ → cả D2 và D3 cùng tựa lên D4 mà không nhân bản.

---

### D5 — State & Storage: vì sao tách storage riêng + 3 tier?

> 📊 Sơ đồ: [`mermaid/domain5_state_storage.mmd`](mermaid/domain5_state_storage.mmd)

**Phản đề:** Lưu trữ cứ để mỗi Worker tự quản RocksDB, sao phải lập domain riêng với 3 tầng?

**Bác bỏ:** 1 tầng RocksDB **chết theo node** (không failover được — RocksDB giữ `LOCK` độc quyền, đĩa thuộc máy khác); 2 tầng + Shared Volume **không chống được mất volume** (single point of failure → không DR) và **phình đĩa** (dung lượng hữu hạn); dồn hết lên MinIO thì **latency chục–trăm ms giết hot-path <1ms**. Không công nghệ nào vừa nhanh, vừa failover-được, vừa bền-DR.

**Lý do:** Storage là **concern cross-cutting** — cả Strict lẫn Heuristic cùng cần recovery, nên tách thành domain để **dùng chung một cơ chế checkpoint/eviction** (chỉ khác Heuristic thêm `sketch.bin`). Ba tầng là phân rã theo 3 trục trực giao **tốc độ / failover / bền-DR**, tạo đường ống vòng đời `Tier1→Tier2→Tier3` giữ đĩa cục bộ phẳng lì.

---

## 3. Câu chốt

> Hệ thống chia theo **plane chức năng** chứ không theo máy, để **cô lập từng luồng giao tiếp** (data / control / metadata / state) cho dễ kiểm thử và chịu lỗi độc lập — và quan trọng nhất, để **hai chiến lược watermark dùng chung 3 plane (D1/D4/D5) và chỉ hoán đổi plane điều khiển (D2↔D3)**. Mỗi domain tồn tại vì nó giải đúng một bài toán mà domain khác không gánh thay được; loại bỏ hay gộp bất kỳ domain nào đều đánh mất một thuộc tính không thể thương lượng (durability, exactly-once, latency, hoặc khả năng chống thảm họa).

---

## Phụ lục — Danh mục file sơ đồ trong [`mermaid/`](mermaid/)

| File | Nội dung |
| :--- | :--- |
| [`domain_overview.mmd`](mermaid/domain_overview.mmd) | Tổng quan 5 domain theo 4 plane; D1/D4/D5 chung, D2↔D3 hoán đổi |
| [`domain1_data_ingestion.mmd`](mermaid/domain1_data_ingestion.mmd) | Luồng nạp: Ingestor → Kafka → Worker → results/audit |
| [`domain2_strict_coordination.mmd`](mermaid/domain2_strict_coordination.mmd) | Strict: heartbeat, GetGlobalState, Raft, fencing |
| [`domain3_heuristic_aggregation.mmd`](mermaid/domain3_heuristic_aggregation.mmd) | Heuristic: SendWorkerWatermark, Active-Standby, DLQ |
| [`domain4_control_metadata.mmd`](mermaid/domain4_control_metadata.mmd) | Ingestor heartbeat (Kafka), ZooKeeper discovery, dashboard |
| [`domain5_state_storage.mmd`](mermaid/domain5_state_storage.mmd) | Tiered storage Tier1/2/3 + eviction state machine |
