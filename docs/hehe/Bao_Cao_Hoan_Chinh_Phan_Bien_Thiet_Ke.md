# Báo cáo phản biện và bảo vệ thiết kế hệ thống Distributed Watermark Tracker

**Tên đề tài:** Distributed Watermark Tracker / Log Delay Compensator  
**Đối tượng báo cáo:** Thiết kế và hiện thực hệ thống xử lý stream có dữ liệu đến không đúng thứ tự event-time.  
**Mục tiêu báo cáo:** Trình bày đầy đủ thiết kế đã implement, giải thích vì sao từng quyết định thiết kế là cần thiết, phản biện các phương án đơn giản hơn, và đánh giá bằng số liệu thực nghiệm.  
**Tài liệu tham khảo nội bộ:** Báo cáo này có tham khảo ở mức khung lý thuyết từ [`REPORT_OZSU_VALDURIEZ_DESIGN_JUSTIFICATION.md`](REPORT_OZSU_VALDURIEZ_DESIGN_JUSTIFICATION.md), nhưng nội dung chính được viết lại theo code hiện tại của repository.

Các sơ đồ Mermaid được tách riêng trong thư mục [`mermaid/`](mermaid/). Báo cáo này chỉ trích dẫn sơ đồ, không nhúng code Mermaid vào nội dung chính.

---

## Tóm tắt

Trong xử lý dữ liệu dòng theo cửa sổ thời gian, kết quả không thể chốt chỉ dựa trên thời điểm hệ thống nhận bản ghi. Mỗi bản ghi có `event_time`, là thời điểm sự kiện thực sự xảy ra, và có `arrival_time`, là thời điểm hệ thống quan sát được bản ghi. Khi `arrival_time > event_time`, dữ liệu đến muộn. Nếu hệ thống đóng cửa sổ quá sớm, kết quả bị thiếu; nếu hệ thống chờ quá lâu, độ trễ đầu ra tăng. Vì vậy, watermark là cơ chế quyết định khi nào một cửa sổ event-time đủ an toàn để phát kết quả.

Dự án hiện thực hai chiến lược watermark có mục tiêu khác nhau:

| Chiến lược | Mục tiêu chính | Cơ chế đã implement | Đánh đổi |
| :--- | :--- | :--- | :--- |
| Strict Watermark | Ưu tiên tính đúng ngay tại thời điểm phát kết quả | Punctuation Token, local watermark theo partition, Coordinator tính `W_global = min(LW_i)`, failover/failback có fencing | Độ trễ cao hơn, phụ thuộc partition chậm, cần control plane mạnh |
| Heuristic Watermark | Ưu tiên giảm độ trễ watermark | DDSketch ước lượng lateness, Aggregator tính `W_global_h = min(W_h)`, late event vào DLQ và correction | Có sai số tức thời, cần pipeline sửa sai sau |

Kết quả thực nghiệm trên dataset 2,961,423 sự kiện cho thấy trade-off là thực: với Strict, completeness tăng từ 8.753% tại `δ = 0s` lên 99.972% tại `δ = 120s`; với Heuristic, `L_eff` bám theo phân vị lateness và eventual completeness đạt 100% khi DLQ/correction được xử lý đầy đủ. Kết luận chính là không có một cơ chế watermark đơn giản duy nhất vừa đảm bảo latency thấp, vừa đảm bảo completeness tức thời tuyệt đối, vừa chịu lỗi phân tán. Thiết kế hai mode là cần thiết vì mỗi mode bảo vệ một loại SLA khác nhau.

**Từ khóa:** distributed stream processing, watermark, event-time window, out-of-order data, Kafka partition, DDSketch, DLQ, failover, tiered storage, PACELC.

**Nguyên tắc trình bày của báo cáo:** Với từng thành phần, báo cáo trình bày **thiết kế chi tiết đã implement trước**, sau đó mới giải thích **vì sao thiết kế như vậy** và phản biện phương án đơn giản hơn. Cách viết này giúp người đọc thấy rõ hệ thống đang làm gì trước khi nghe phần bảo vệ quyết định thiết kế.

---

## 1. Giới thiệu và phạm vi

### 1.1 Bài toán

Hệ thống nhận luồng log/event có dạng tổng quát:

```text
event = {
  event_id,
  event_time,
  arrival_time,
  partition_id,
  status,
  payload
}
```

Kết quả cần tính theo tumbling window trên `event_time`, không phải theo `arrival_time`. Ví dụ với window 5 giây, bản ghi có `event_time = 100.2s` thuộc cửa sổ `[100, 105)`, kể cả khi nó đến hệ thống ở `arrival_time = 120s`. Câu hỏi trung tâm là: tại thời điểm nào hệ thống được phép coi cửa sổ `[100, 105)` là đã hoàn tất?

Nếu đóng cửa sổ theo wall-clock, hệ thống sẽ sai khi dữ liệu replay, dữ liệu bị nghẽn mạng, hoặc dữ liệu bị đảo thứ tự. Nếu giữ mọi cửa sổ mở vô hạn, hệ thống không phát kết quả kịp thời và state phình lên. Watermark là lời giải trung gian: nó đặt một ranh giới logic cho biết hệ thống tin rằng các event nhỏ hơn ranh giới đó đã đủ an toàn để xử lý.

### 1.2 Phạm vi báo cáo

Báo cáo tập trung vào phần đã implement trong repository:

- Data plane: ingestor, Kafka, worker, partition, bounded priority queue.
- Strict mode: `StrictWatermarkEngine`, `StrictWorker`, `StrictCoordinator`, `RaftCoordinator`, `FailoverManager`, `OutputManager`.
- Heuristic mode: `HeuristicWatermarkEngine`, `HeuristicAggregator`, `AggregatorHA`, `DLQPipeline`, `CorrectionProtocol`, DDSketch nội bộ.
- State plane: RocksDB wrapper, checkpoint, tiered storage MinIO, eviction state.
- Control and observability: gRPC/HTTP API, monitoring, alerting, config.
- Evaluation: offline analysis và sweep CSV/Markdown trong `reports/results/` và `docs/`.

Báo cáo không giả định hệ thống đã là bản production-grade nhiều máy vật lý. Các điểm còn là mô phỏng hoặc giới hạn thực nghiệm được nêu riêng tại mục 15.

---

## 2. Ký hiệu và định nghĩa

| Ký hiệu / thuật ngữ | Định nghĩa trong báo cáo |
| :--- | :--- |
| `T_event` | Thời điểm sự kiện xảy ra, lấy từ `event_time`. |
| `T_arrival` | Thời điểm hệ thống nhận/quan sát sự kiện, lấy từ `arrival_time`. |
| `lateness` | `T_arrival - T_event`. Nếu lớn hơn 0 thì event đến muộn. |
| `window_end` | Biên phải của tumbling window chứa event. |
| `LW_i` | Local watermark của partition hoặc engine cục bộ trong Strict mode. |
| `W_global` | Watermark toàn cục trong Strict mode, tính bằng min trên các `LW_i` hợp lệ. |
| `L_eff` | Độ trễ hiệu dụng trong Heuristic, lấy từ phân vị DDSketch. |
| `W_h` | Watermark heuristic cục bộ, thường là `max_event_time - L_eff`. |
| `W_global_h` | Watermark toàn cục trong Heuristic, tính bằng min trên các `W_h` active/stale. |
| Immediate completeness | Tỷ lệ event được xử lý đúng hạn trước khi window đóng lần đầu. |
| Eventual completeness | Tỷ lệ event đúng sau khi tính cả DLQ/correction. |
| Fencing token | Term/command id dùng để vô hiệu hóa lệnh cũ sau failover hoặc split-brain. |
| Single-owner invariant | Tại một thời điểm, một partition chỉ được một worker sở hữu. |

Điểm cần nói rõ để tránh mơ hồ: "Strict" trong báo cáo này có hai ngữ cảnh. Trong ngữ cảnh giao thức, Strict nghĩa là chỉ đóng window khi có bằng chứng tiến độ an toàn từ punctuation/control plane. Trong ngữ cảnh sweep với `δ` hữu hạn, Strict biểu diễn trade-off completeness-vs-wait; event trễ hơn biên `δ` vẫn có thể bị late/drop. Muốn đạt 100% trên dataset hữu hạn, hệ thống phải dùng EOF/data-driven punctuation hoặc chọn `δ` ít nhất bằng max lateness.

---

## 3. Cách đọc báo cáo và đối chiếu code

### 3.1 Cách đối chiếu

Báo cáo được xây từ ba loại nguồn:

1. **Mã nguồn hiện thực:** các class/function trong `strict/`, `heuristic/`, `common/`, `local_ddsketch/`, `run.py`.
2. **Kết quả thực nghiệm:** `reports/results/analysis_report.md`, `docs/strict_sweep_20260602_203750.csv`, `docs/heuristic_sweep_20260602_203750.csv`, `reports/results/completeness_vs_wait_strict_20260603_003718.csv`.
3. **Khung lý thuyết:** các khái niệm phân mảnh, allocation, replication, coordination, recovery, transparency trong hệ dữ liệu phân tán, tham khảo từ tài liệu Özsu-Valduriez nội bộ.

### 3.2 Đối chiếu code đã implement

| Thành phần | Bằng chứng implementation | Ý nghĩa thiết kế |
| :--- | :--- | :--- |
| Ingestor và partitioning | [`run.py`](../../run.py), `run_ingestor`, `_next_csv_event`, `send_event_kafka`, `send_punctuation` | Chuẩn hóa CSV, sinh `event_time/arrival_time`, hash host sang 12 partition, phát event và punctuation qua Kafka/HTTP. |
| Strict local engine | [`strict/engine.py`](../../strict/engine.py), `StrictWatermarkEngine.on_punctuation`, `_advance_watermark`, `process`, `checkpoint` | Nhận punctuation, cập nhật local watermark, lọc late event, dedup, đóng window, upload/purge, checkpoint state và offset. |
| Strict worker | [`strict/worker.py`](../../strict/worker.py), `BoundedPriorityQueue`, `StrictWorker.heartbeat`, `update_global_watermark` | Sắp event theo event-time trước engine, gửi heartbeat per-partition, nhận global watermark và term/fencing. |
| Strict coordinator | [`strict/coordinator.py`](../../strict/coordinator.py), `receive_heartbeat`, `_compute_global` | Enforce fencing token, cập nhật partition status, tính `W_global` đơn điệu bằng min trên partition hợp lệ. |
| Failover/failback | [`strict/failover.py`](../../strict/failover.py), `detect_failures`, `reassign_failed_partitions`, `start_failback`, `advance_failback` | Phát hiện worker timeout, reassign partition, thực hiện failback 5 bước và persist trạng thái bàn giao. |
| Exactly-once output | [`strict/output_manager.py`](../../strict/output_manager.py), `OutputManager.emit`, `save_emitted`, `load_emitted` | Chống phát trùng theo `window_id`, có transactional/mock sink và Kafka output topic. |
| Heuristic engine | [`heuristic/engine.py`](../../heuristic/engine.py), `_compute_watermark`, `_update_L_eff`, `process` | DDSketch quantile, cold start, hysteresis, rate limit, negative lag handling, DLQ late event. |
| Heuristic aggregator | [`heuristic/aggregator.py`](../../heuristic/aggregator.py), `receive_worker_watermark`, `_compute_global` | Tổng hợp `W_h` theo partition, lấy min trên active/stale và giữ watermark đơn điệu. |
| DLQ và correction | [`heuristic/dlq.py`](../../heuristic/dlq.py), `enqueue`, `compute_corrections`, `CorrectionProtocol.apply_correction` | Late event không mất thầm lặng; được lưu, gom theo window, phát correction có dedup. |
| Aggregator HA | [`heuristic/aggregator_ha.py`](../../heuristic/aggregator_ha.py), `FileLockLeader`, `AggregatorHA` | Active-standby nhẹ cho Heuristic, phù hợp vì Aggregator không điều phối partition ownership. |
| Tiered storage | [`common/tiered_storage.py`](../../common/tiered_storage.py), `upload_window`, `download_window`, `purge_window` | Đẩy closed window lên MinIO, tải lại khi cần, quản lý eviction state. |
| Config | [`common/config.py`](../../common/config.py), `Config` | Tất cả tham số quan trọng đều cấu hình được: window, partition, heartbeat, DDSketch, DLQ, TLS, metrics. |

### 3.3 Tiêu chí phản biện

Một quyết định thiết kế được xem là hợp lý nếu thỏa ba điều kiện:

1. **Có mục tiêu rõ:** nó bảo vệ completeness, latency, recovery, availability hoặc observability cụ thể.
2. **Có bằng chứng implementation:** repo có thành phần thực thi, không chỉ là mô tả giấy.
3. **Có phản biện phương án đơn giản hơn:** nếu bỏ thành phần đó, phải chỉ ra invariant nào bị phá hoặc SLA nào không còn giữ được.

---

## 4. Bối cảnh dữ liệu đầu vào của thiết kế

### 4.1 Dataset

Trước khi trình bày thiết kế chi tiết, cần xác định dạng dữ liệu đầu vào mà hệ thống phải xử lý. Dataset dùng trong báo cáo là NYC TLC Yellow Taxi tháng 01/2024, được chuẩn hóa thành CSV engine-compatible.

| Chỉ số | Giá trị |
| :--- | ---: |
| Tổng số event | 2,961,423 |
| Số host/partition key | 260 |
| File chuẩn hóa | Khoảng 152 MB |
| Event-time span sau nén | 44,639.5s, khoảng 12.4 giờ mô phỏng |
| Tổng số window với `WINDOW_SIZE_S = 5` | 8,927 |
| Out-of-order adjacent steps | 1,463,199, tương đương 49.41% |

Lateness được tạo từ duration sau nén thời gian. Cách này giữ hình dạng phân phối độ trễ nhưng rút thời gian benchmark xuống mức có thể chạy được trong Docker/local.

### 4.2 Phân phối lateness

| Percentile | Lateness |
| ---: | ---: |
| P50 | 11.633s |
| P75 | 18.667s |
| P90 | 28.800s |
| P95 | 37.783s |
| P99 | 59.700s |
| P99.9 | 95.034s |
| P99.99 | 160.028s |
| Max | 359.733s |

Ý nghĩa trực tiếp: nếu một watermark dùng wait time cố định `δ`, completeness xấp xỉ CDF của lateness tại `δ`, có thêm sai lệch do biên window và hành vi runtime. Vì tail lateness dài tới 359.733s, việc ép 100% completeness bằng một `δ` cố định sẽ kéo latency lên rất cao.

### 4.3 Metric sẽ dùng ở phần đánh giá sau thiết kế

| Metric | Công thức / cách hiểu | Dùng để đánh giá |
| :--- | :--- | :--- |
| Immediate completeness | `on_time / total` trước correction | Chất lượng kết quả phát lần đầu |
| Late rate | `late / total` | Tỷ lệ event bị xử lý muộn |
| Eventual completeness | `(on_time + corrected) / total` | Chất lượng sau DLQ/correction |
| Wait time | `δ` hoặc `L_eff` | Độ trễ watermark đưa vào thiết kế |
| DLQ backlog | Số late event đi đường sửa sai | Chi phí của Heuristic |
| Processing latency p99 | P99 thời gian xử lý ở runtime | Chi phí runtime của implementation |

---

## 5. Kiến trúc tổng quan đã implement

Sơ đồ liên quan: [`mermaid/domain_overview.mmd`](mermaid/domain_overview.mmd)

Hệ thống được chia theo plane chức năng, không chỉ theo node vật lý.

| Plane | Thành phần | Vai trò | Vì sao tách riêng |
| :--- | :--- | :--- | :--- |
| Data plane | Ingestor, Kafka, Worker | Đưa event vào, lưu theo partition, xử lý window | Data traffic lớn, cần bền, cần replay, không nên trộn với control message |
| Strict control plane | Coordinator, Raft/fencing | Tính `W_global`, điều phối partition ownership, failover/failback | Strict cần quyết định toàn cục có tính đúng đắn mạnh |
| Heuristic control plane | Aggregator, Active-Standby | Tổng hợp `W_h`, phát `W_global_h` | Heuristic cần control nhẹ để giảm latency |
| Metadata plane | ZooKeeper/File lock, health, schema, monitoring | Election, discovery, health, schema registry, metrics | Dùng chung cho hai mode và phục vụ vận hành |
| State plane | RocksDB, checkpoint, Shared Volume, MinIO | Lưu state nóng, state failover, archive/DR | Recovery không thể phụ thuộc hoàn toàn vào RAM |

### 5.1 Luồng Strict

```text
CSV/Events -> Ingestor -> Kafka partitions -> StrictWorker -> StrictWatermarkEngine
             -> WorkerHeartbeat -> StrictCoordinator -> W_global -> Worker closes windows
             -> OutputManager -> results/audit
```

### 5.2 Luồng Heuristic

```text
CSV/Events -> Ingestor -> Kafka partitions -> HeuristicWorker -> HeuristicWatermarkEngine
             -> WorkerWatermark -> HeuristicAggregator -> W_global_h
             -> speculative windows + DLQ -> CorrectionProtocol
```

### 5.3 Luận điểm kiến trúc

Hai mode dùng chung data plane và state plane, nhưng không dùng chung control plane. Đây là quyết định cốt lõi. Nếu ép Strict và Heuristic dùng một control plane duy nhất, ta phải chọn một trong hai bất lợi:

- Dùng control plane mạnh kiểu Strict cho cả Heuristic: Heuristic trả chi phí đồng thuận/fencing không cần thiết, làm hỏng mục tiêu latency thấp.
- Dùng control plane nhẹ kiểu Heuristic cho cả Strict: Strict thiếu fencing/commit đủ mạnh cho failover/failback, làm yếu exactly-once và single-owner.

Vì vậy, tách control plane không phải là trùng lặp thừa; nó là cách cô lập hai loại cam kết khác nhau.

---

## 6. Thiết kế chi tiết Strict Watermark và lý do lựa chọn

Sơ đồ liên quan: [`mermaid/strict_watermark_seq.mmd`](mermaid/strict_watermark_seq.mmd)

### 6.1 Thiết kế chi tiết đã implement

Strict dùng Punctuation Token để biến tiến độ của nguồn dữ liệu thành tiến độ logic. Mỗi token chứa `T_commit`. Khi engine nhận token hợp lệ, local watermark được cập nhật:

```text
LW_i = T_commit - δ
```

Coordinator nhận heartbeat chứa `LW_i` theo từng partition và tính:

```text
W_global = max(W_global_prev, min(LW_i))
```

Window chỉ đóng khi:

```text
window_end <= W_global
```

Trong code, logic này nằm ở:

- `StrictWatermarkEngine.on_punctuation`: cập nhật `last_T_commit` và `local_watermark`.
- `StrictCoordinator._compute_global`: lấy min trên partition active/stale/idle hợp lệ và giữ watermark đơn điệu.
- `StrictWatermarkEngine._advance_watermark`: đóng window, upload, emit, purge theo trạng thái.

**Nội dung thiết kế trong mục Strict.**

| Nội dung | Mô tả |
| :--- | :--- |
| Thành phần tham gia | Ingestor, Kafka, StrictWorker, StrictWatermarkEngine, StrictCoordinator, OutputManager. |
| Dữ liệu trao đổi | Event nghiệp vụ, punctuation token, local watermark của từng partition, global watermark, kết quả window đã đóng. |
| Cách xử lý chính | Ingestor phát event và punctuation theo từng partition; worker cập nhật watermark cục bộ; coordinator lấy min để tạo watermark toàn cục; worker chỉ đóng window khi global watermark vượt qua cuối window. |
| Trạng thái cần lưu | Offset Kafka đã xử lý, watermark cục bộ, watermark toàn cục cuối cùng, trạng thái open window, kết quả đã emit để tránh lặp khi recovery. |
| Đầu ra của thiết kế | Kết quả window có tính đầy đủ mạnh trong phạm vi dữ liệu đã có punctuation, kèm metadata cho output và checkpoint. |

### 6.2 Vì sao thiết kế như vậy: cần Punctuation Token

**Phản đề:** Có thể không cần token; chỉ cần Worker nhìn event mới nhất rồi tự tính watermark.

**Bác bỏ:** Worker chỉ nhìn thấy partition cục bộ. Nếu partition P0 đã đi tới event-time 200s nhưng P7 vẫn còn event-time 150s chưa đến, P0 tự đóng window `[150,155)` sẽ gây thiếu dữ liệu toàn cục. Token là bằng chứng từ nguồn rằng dữ liệu nhỏ hơn một mốc đã được phát qua partition tương ứng. Không có token, Strict sẽ phải đoán. Mà nếu đoán, nó không còn là Strict.

**Kết luận thiết kế:** Punctuation Token là metadata bắt buộc cho correctness trong mode Strict. Nó không thay thế Kafka; nó đi cùng data stream để giữ thứ tự với event trong cùng partition.

### 6.3 Vì sao thiết kế như vậy: cần Coordinator

**Phản đề:** Mỗi Worker tự đóng window theo local watermark để giảm điều phối.

**Bác bỏ:** Watermark đúng là thuộc tính toàn cục, không phải thuộc tính node. Nếu node tự chốt, hệ thống sẽ mất thông tin về partition mà node đó không quản lý. Việc lấy min toàn cục là cách bảo thủ nhưng chính xác: chỉ cần một partition chưa đi qua mốc thời gian, cửa sổ toàn cục chưa an toàn.

**Bằng chứng code:** `StrictWorker.heartbeat` gửi map `{partition_id -> local_watermark}`; `StrictCoordinator.receive_heartbeat` cập nhật từng partition; `_compute_global` tính min trên partition. Code không gộp watermark ở cấp node trước khi gửi.

### 6.4 Vì sao thiết kế như vậy: không bỏ qua partition idle ngầm định

**Phản đề:** Partition lâu không có event làm `min()` đứng yên. Bỏ nó ra khỏi min sẽ giúp watermark chạy nhanh.

**Bác bỏ:** Im lặng không đồng nghĩa với an toàn. Partition có thể tạm thời không có log, bị chậm, hoặc bị nghẽn mạng. Nếu Coordinator tự loại partition im lặng rồi nó hoạt động trở lại với event cũ, các event đó sẽ rơi sau `W_global` và bị late. Điều này trực tiếp phá cam kết của Strict.

**Điều code đang làm:** `StrictCoordinator._compute_global` vẫn tính các partition `ACTIVE`, `STALE`, `IDLE`; chỉ loại `FAILED` hoặc `is_temporary_idle` được Worker đánh dấu tường minh. Đây là điểm quan trọng: hệ thống không tự tiện bỏ partition active khỏi min; nó chỉ loại trường hợp đã có phân loại rõ.

**Kết luận thiết kế:** Giải pháp đúng không phải là bỏ idle tùy tiện, mà là có signal rõ: punctuation rỗng, temporary-idle tường minh, hoặc failover khi heartbeat timeout.

### 6.5 Vì sao thiết kế như vậy: Strict cần Raft/fencing thay vì chỉ cần lock

**Phản đề:** Dùng ZooKeeper lock hoặc file lock chọn một Coordinator active là đủ, cần gì Raft/fencing.

**Bác bỏ:** Coordinator Strict không chỉ tính watermark; nó điều phối quyền sở hữu partition, reassign, failback và output correctness. Nếu leader cũ bị partition mạng rồi sống lại, nó có thể phát lệnh cũ cho Worker trong khi leader mới đã reassign partition. Không có term/fencing, hai worker có thể cùng xử lý một partition, tạo duplicate output hoặc lệch offset. Lock chỉ cho biết ai đang giữ khóa hiện tại; nó không đủ để mọi lệnh cũ tự vô hiệu khi topology đổi.

**Bằng chứng code:** `StrictCoordinator.receive_heartbeat` reject heartbeat có fencing token cũ; `StrictWorker.validate_command` bỏ lệnh có term cũ hoặc duplicate command id; `RaftCoordinator` cung cấp role/term/state replication; `FailoverManager` persist failback state.

**Kết luận thiết kế:** Với Strict, fencing là điều kiện an toàn, không phải tối ưu phụ. Raft/fencing bảo vệ single-owner và exactly-once trong các tình huống failover.

### 6.6 Vì sao vẫn có late/drop trong Strict khi chạy sweep hữu hạn

**Phản đề:** Strict nói không mất dữ liệu, sao code vẫn có `late_dropped`?

**Bác bỏ:** Có hai chế độ vận hành khác nhau. Nếu punctuation là bằng chứng đúng và đủ, window chỉ đóng khi an toàn, nên late event sau đóng là vi phạm thứ tự đầu vào hoặc nằm ngoài biên `δ` đã chọn. Trong sweep `δ` hữu hạn, hệ thống cố ý đo completeness-vs-wait; event trễ hơn `δ` bị xem là late để vẽ đường trade-off. Trong replay cần 100%, `run.py` có chế độ data-driven punctuation giữ watermark ở `-inf` trong lúc ingest và nhảy tới `max_event_time + δ + window` tại EOF để flush.

**Kết luận thiết kế:** `late_dropped` không mâu thuẫn với Strict; nó là metric cho trường hợp watermark tiến trước event do cấu hình `δ` hoặc punctuation mode. Báo cáo phải nói rõ điều kiện đảm bảo của Strict thay vì khẳng định mơ hồ "luôn 0% loss".

---

## 7. Thiết kế chi tiết Heuristic Watermark và lý do lựa chọn

Sơ đồ liên quan: [`mermaid/heuristic_watermark_seq.mmd`](mermaid/heuristic_watermark_seq.mmd)

### 7.1 Thiết kế chi tiết đã implement

Heuristic không chờ Punctuation Token. Mỗi engine quan sát lateness:

```text
lateness = arrival_time - event_time
```

Các mẫu lateness hợp lệ đi vào Sliding DDSketch. Engine lấy phân vị `p` để tính:

```text
L_eff = DDSketch.quantile(p)
W_h = max_event_time - L_eff
```

Aggregator nhận `W_h` theo partition và tính:

```text
W_global_h = max(W_global_h_prev, min(W_h))
```

Window đóng theo `W_global_h` hoặc theo local `W_h` nếu bật `HEURISTIC_LOCAL_WATERMARK_CLOSE` cho sweep thực nghiệm.

**Nội dung thiết kế trong mục Heuristic.**

| Nội dung | Mô tả |
| :--- | :--- |
| Thành phần tham gia | HeuristicIngestor, HeuristicWorker, HeuristicWatermarkEngine, DDSketch, Aggregator, DLQ/output sink. |
| Dữ liệu quan sát | Event time, processing time, lateness, phân phối lateness theo partition, local heuristic watermark, global heuristic watermark. |
| Cách xử lý chính | Worker đo lateness của event đến, cập nhật sketch, lấy quantile theo cấu hình, trừ quantile khỏi processing time để sinh watermark heuristic, rồi gửi watermark lên Aggregator. |
| Cơ chế ổn định | Cold start dùng ngưỡng khởi động, hysteresis tránh watermark dao động, rate limit giới hạn tốc độ tăng watermark, DLQ giữ event quá muộn. |
| Đầu ra của thiết kế | Kết quả window có độ trễ thấp hơn Strict nhưng chấp nhận late/drop theo ngưỡng quantile đã chọn, kèm thông tin để đo trade-off giữa completeness và latency. |

### 7.2 Vì sao thiết kế như vậy: cần DDSketch

**Phản đề:** Dùng EWMA, trung bình trượt hoặc histogram cố định đơn giản hơn.

**Bác bỏ:**

| Phương án đơn giản | Lỗi kỹ thuật |
| :--- | :--- |
| EWMA / mean | Watermark cần kiểm soát tail lateness như p95/p99, trong khi mean bị che bởi phân phối lệch phải. |
| Histogram cố định | Nếu bin rộng thì tail p99 sai; nếu bin hẹp thì tốn bộ nhớ và khó chọn range khi lateness biến động. |
| Tính percentile trên toàn bộ mẫu thô | Không bounded memory, không phù hợp stream dài. |
| T-digest hoặc estimator khác | Có thể dùng, nhưng implementation hiện tại đã có DDSketch nội bộ, log-scale và merge/quantile phù hợp với độ trễ dương heavy-tail. |

**Bằng chứng code:** `HeuristicWatermarkEngine._update_L_eff` truy vấn `self.sketch.quantile(self.p_current)`, có caching, hysteresis 10%, burst detection, adaptive alpha và cold start. DDSketch nội bộ nằm ở `local_ddsketch/`.

**Kết luận thiết kế:** DDSketch không phải trang trí học thuật. Nó giải đúng bài toán cần phân vị tail trong stream có bộ nhớ giới hạn.

### 7.3 Vì sao thiết kế như vậy: cần cold start, hysteresis và rate limit

**Phản đề:** Cứ nhận event là cập nhật `L_eff`, rồi chốt window ngay.

**Bác bỏ:** Khi số mẫu nhỏ, percentile ước lượng chưa ổn định. Nếu `L_eff` quá thấp, watermark tiến quá nhanh và đẩy nhiều event hợp lệ vào DLQ. Nếu `L_eff` nhảy mạnh theo burst, downstream sẽ nhận kết quả dao động. Nếu watermark nhảy quá nhanh so với wall-clock, các window có thể đóng hàng loạt không đại diện cho streaming thật.

**Bằng chứng code:** `HeuristicWatermarkEngine._compute_watermark` giữ watermark đơn điệu, có rate limit theo elapsed time và window size; `_update_L_eff` dùng cold start prior khi chưa warm, chỉ cập nhật khi drift vượt threshold; `_check_burst` chuyển sang `p_safe` khi phát hiện burst.

**Kết luận thiết kế:** Heuristic không chỉ là "đoán". Nó là speculative execution có biên an toàn thống kê và điều tiết tốc độ.

### 7.4 Vì sao thiết kế như vậy: late event phải đi DLQ

**Phản đề:** Nếu chọn p99 đủ cao, có thể bỏ DLQ để đơn giản.

**Bác bỏ:** Không percentile hữu hạn nào bao phủ mọi outlier. Dataset có max lateness 359.733s trong khi p99 là 59.700s. Dùng p99 vẫn bỏ sót tail. Nếu không có DLQ, event tail bị mất thầm lặng và hệ thống không thể tuyên bố eventual completeness.

**Bằng chứng code:** `HeuristicWatermarkEngine.process` thêm event vào `late_events` khi window đã đóng hoặc lag quá cực đoan; `DLQPipeline.enqueue` persist entry và có thể produce sang Kafka topic; `compute_corrections` gom late event theo window; `CorrectionProtocol.apply_correction` hỗ trợ incremental, replace, append-versioning và dedup correction id.

**Kết luận thiết kế:** DLQ là điều kiện để Heuristic được phép đóng cửa sổ sớm. Không có DLQ, Heuristic chỉ là mất dữ liệu có hệ thống.

### 7.5 Vì sao thiết kế như vậy: Heuristic không dùng Raft như Strict

**Phản đề:** Raft đã có, dùng cho Aggregator luôn để chắc chắn hơn.

**Bác bỏ:** Aggregator không điều phối quyền sở hữu partition và không quyết định failback. Nếu Aggregator chậm hoặc failover, hậu quả chính là watermark heuristic cập nhật chậm hoặc late event tăng, nhưng DLQ/correction vẫn bảo vệ eventual completeness. Dùng Raft cho Aggregator sẽ thêm quorum/election/replication vào một đường vốn cần nhẹ, làm trái mục tiêu latency thấp.

**Bằng chứng code:** `HeuristicAggregator.receive_worker_watermark` chỉ nhận `W_h` và `_compute_global` lấy min; `AggregatorHA` dùng active-standby/file lock heartbeat. Không có logic partition ownership trong Aggregator.

**Kết luận thiết kế:** Heuristic dùng control plane nhẹ vì bản chất cam kết của nó là eventual consistency, không phải immediate exactly-once control.

---

## 8. Thiết kế chi tiết Worker Node và Partition

Sơ đồ liên quan: [`mermaid/node_partition_engine.mmd`](mermaid/node_partition_engine.mmd)

### 8.1 Thiết kế chi tiết đã implement

Mỗi Worker quản lý nhiều partition, nhưng state và engine được cô lập theo partition. Trong Strict, `StrictWorker.engines` và `StrictWorker.buffers` là map theo `partition_id`; mỗi partition có lock riêng. Heartbeat gửi đầy đủ map local watermark theo partition. Heuristic cũng duy trì engine/sketch/window state theo partition.

**Nội dung thiết kế trong mục Worker/Partition.**

| Nội dung | Mô tả |
| :--- | :--- |
| Đơn vị quản lý | Partition là đơn vị sở hữu, checkpoint, watermark và recovery; worker chỉ là nơi đang tạm thời chạy một hoặc nhiều partition. |
| Thành phần trong worker | Consumer theo partition, BoundedPriorityQueue, engine xử lý window, state store, heartbeat client, output client. |
| Trạng thái theo partition | Buffer event, open window, watermark cục bộ, offset Kafka, lock xử lý, metadata owner/epoch. |
| Cách heartbeat hoạt động | Worker gửi trạng thái từng partition thay vì chỉ gửi trạng thái tổng của node, để Coordinator/Aggregator ra quyết định đúng theo partition. |
| Đầu ra của thiết kế | Partition có thể được di chuyển giữa worker khi failover/failback mà không làm lẫn state hoặc làm sai watermark của partition khác. |

### 8.2 Vì sao thiết kế như vậy: đơn vị đúng là partition, không phải node

**Phản đề:** Một node có thể gộp ba partition của nó thành một watermark rồi gửi lên Coordinator/Aggregator để giảm message size.

**Bác bỏ:** Partition có phân phối lateness, traffic và trạng thái failover khác nhau. Gộp sớm làm mất thông tin. Một partition chậm sẽ kéo lùi toàn node, còn control plane không biết partition nào là nguyên nhân. Khi failover một partition, hệ thống cũng cần bàn giao riêng partition đó, không phải cả node.

**Bằng chứng code:** `StrictWorker.heartbeat` gửi `partitions=partitions`; `StrictCoordinator.partitions` lưu `PartitionInfo` theo `partition_id`; `FailoverManager` reassign từng partition.

**Kết luận thiết kế:** Partition là đơn vị đúng đắn nhỏ nhất cho watermark, state, failover và accountability.

### 8.3 Vì sao thiết kế như vậy: mỗi partition cần state riêng

**Phản đề:** Dùng một RocksDB chung cho cả Worker sẽ đơn giản hơn.

**Bác bỏ:** Failover xảy ra theo partition. Nếu state của nhiều partition nằm chung một DB, node mới khó mở riêng state của partition được giao. RocksDB còn có lock độc quyền trên thư mục DB. Cô lập DB/state theo partition làm cho ownership và checkpoint khớp với đơn vị failover.

**Kết luận thiết kế:** Per-partition state là chi phí chấp nhận được để bảo vệ single-owner và failover từng partition.

---

## 9. Thiết kế chi tiết Kafka và Bounded Priority Queue

Sơ đồ liên quan: [`mermaid/kafka_bounded_pq.mmd`](mermaid/kafka_bounded_pq.mmd)

### 9.1 Thiết kế chi tiết đã implement

Kafka là data plane bền vững. Ingestor produce event vào topic `events` theo `partition_id`; Worker poll theo partition, giữ offset để replay khi recovery. Trong Strict, punctuation cũng đi qua Kafka để giữ thứ tự với event cùng partition. Ở Worker, event không được đưa thẳng vào window engine mà đi qua `BoundedPriorityQueue`, một min-heap theo `event_time`, có `maxsize` và `max_wait_ms`.

**Nội dung thiết kế trong mục Kafka và Bounded Priority Queue.**

| Nội dung | Mô tả |
| :--- | :--- |
| Vai trò Kafka | Là log bền vững cho event, punctuation, result/audit/DLQ; giữ thứ tự trong partition và cho phép replay bằng offset. |
| Vai trò BoundedPriorityQueue | Sắp xếp cục bộ theo event time trước khi đưa vào window engine, nhưng bị giới hạn bởi `maxsize` và `max_wait_ms` để không giữ RAM vô hạn. |
| Luồng xử lý | Ingestor ghi Kafka; worker poll theo partition; event vào hàng đợi ưu tiên; queue xả event đủ điều kiện sang engine; offset và state được checkpoint. |
| Trạng thái cần theo dõi | Kafka offset, queue size, thời gian event nằm trong queue, event_time nhỏ nhất, giới hạn bộ nhớ và timeout xả queue. |
| Đầu ra của thiết kế | Vừa có durability/replay từ Kafka, vừa giảm sai lệch do event lệch thứ tự khi xử lý tại worker. |

### 9.2 Vì sao thiết kế như vậy: cần Kafka

**Phản đề:** Ingestor có thể gửi HTTP trực tiếp vào Worker, bỏ Kafka để giảm độ phức tạp.

**Bác bỏ:** HTTP trực tiếp không cung cấp offset bền vững. Nếu Worker crash sau khi nhận event nhưng trước khi checkpoint, Ingestor không có log bền để replay chính xác. Kafka cung cấp partition, offset, broker persistence và replay. Nó cũng tách tốc độ producer khỏi consumer, cho phép backpressure không biến thành drop dữ liệu.

**Bằng chứng code:** `run.py::send_event_kafka` produce event vào topic `events`; `common/kafka_real.py` cung cấp Kafka producer/consumer; các experiment Docker dùng Kafka 12 partitions.

**Kết luận thiết kế:** Kafka là write-ahead log phân tán của data plane. Nó không chỉ là message queue tiện dụng.

### 9.3 Vì sao thiết kế như vậy: Kafka không thay Bounded Priority Queue

**Phản đề:** Kafka đã có partition order, cần gì min-heap ở Worker.

**Bác bỏ:** Kafka giữ thứ tự offset, không giữ thứ tự event-time. Event đến sau có thể có `event_time` nhỏ hơn event trước. Worker cần buffer theo event-time để giảm out-of-order trước khi cập nhật window. Nhưng buffer không thể vô hạn, nên phải bounded.

**Bằng chứng code:** `BoundedPriorityQueue` dùng `heapq` theo `event.event_time`; nếu đạt `maxsize` hoặc vượt `max_wait_ms`, queue pop event nhỏ nhất để pipeline không kẹt.

**Kết luận thiết kế:** Kafka bảo vệ độ bền và replay; Bounded Priority Queue bảo vệ thứ tự event-time cục bộ và giới hạn RAM. Hai thành phần giải hai bài toán khác nhau.

---

## 10. Thiết kế chi tiết Tiered Storage và lý do lựa chọn

Sơ đồ liên quan: [`mermaid/tiered_storage_cycles.mmd`](mermaid/tiered_storage_cycles.mmd), [`mermaid/eviction_state_machine.mmd`](mermaid/eviction_state_machine.mmd)

### 10.1 Thiết kế chi tiết đã implement

State được chia thành ba tầng:

| Tầng | Nội dung | Vai trò |
| :--- | :--- | :--- |
| Tier-1 RocksDB local | Open window, dedup ids, offsets gần nhất, local engine state | Hot path, đọc/ghi nhanh theo event |
| Tier-2 checkpoint/shared volume | Checkpoint định kỳ, SST manifest, failback state | Recovery nhanh khi worker crash hoặc coordinator đổi leader |
| Tier-3 MinIO/Object storage | Closed windows, archive, DR backup | Lưu dài hạn và phục hồi sau mất node/volume |

**Nội dung thiết kế trong mục Tiered Storage.**

| Nội dung | Mô tả |
| :--- | :--- |
| Phân loại dữ liệu | Hot state đang xử lý, checkpoint phục hồi nhanh, closed window/result/archive cần lưu dài hạn. |
| Vòng đời dữ liệu | Event vào open window ở RocksDB; checkpoint định kỳ đưa state sang tầng phục hồi; window đóng được upload sang MinIO và có thể purge khỏi tầng nóng. |
| Metadata cần quản lý | Window id, partition id, offset range, watermark lúc đóng window, checkpoint version, object key, trạng thái evict/purge. |
| Cơ chế phục hồi | Worker mới đọc checkpoint gần nhất, khôi phục state cần thiết, seek Kafka từ offset an toàn, rồi tiếp tục xử lý phần còn thiếu. |
| Đầu ra của thiết kế | Hệ thống không phụ thuộc vào RAM hoặc disk cục bộ duy nhất; dữ liệu vừa xử lý nhanh, vừa có đường recovery và archive rõ ràng. |

### 10.2 Vì sao thiết kế như vậy: một tầng RocksDB là không đủ

**Phản đề:** RocksDB local vừa nhanh vừa có WAL, chỉ cần nó là đủ.

**Bác bỏ:** RocksDB local chết theo node. Nếu node sập vật lý, node khác không đọc được state local để takeover. WAL chỉ giúp process trên cùng máy phục hồi, không giúp failover sang worker khác. Ngoài ra closed window giữ mãi local sẽ làm phình đĩa.

### 10.3 Vì sao thiết kế như vậy: một tầng MinIO là không đủ

**Phản đề:** MinIO/S3 bền và dùng chung được, sao không đặt toàn bộ state lên MinIO.

**Bác bỏ:** Hot path cập nhật window theo từng event cần latency rất thấp. Object storage không phù hợp với ghi ngẫu nhiên nhỏ và cập nhật thường xuyên. Nếu mọi update window đi qua MinIO, cả Strict lẫn Heuristic mất thông lượng và tăng latency.

### 10.4 Vì sao thiết kế như vậy: hai tầng vẫn chưa đủ

**Phản đề:** RocksDB + Shared Volume đã failover được, không cần Tier-3.

**Bác bỏ:** Shared Volume giúp node khác đọc checkpoint nhanh, nhưng không giải quyết disaster recovery nếu volume hỏng hoặc cần lưu closed window lâu dài. Tier-3 tách archive/DR khỏi checkpoint nóng.

**Bằng chứng code:** `StrictWatermarkEngine._advance_watermark` chuyển closed window sang trạng thái upload, gọi tiered storage, emit, rồi purge state local; `TieredStorageManager.upload_window/download_window/purge_window` hiện thực object storage.

**Kết luận thiết kế:** Ba tầng tương ứng ba mục tiêu không thay thế nhau: tốc độ, failover, bền vững/DR.

---

## 11. Thiết kế chi tiết Coordinator và Aggregator

Sơ đồ liên quan: [`mermaid/coordinator_vs_aggregator.mmd`](mermaid/coordinator_vs_aggregator.mmd)

### 11.1 Thiết kế chi tiết đã implement

| Thành phần | Mode | Trách nhiệm | Cường độ nhất quán cần có |
| :--- | :--- | :--- | :--- |
| Coordinator | Strict | Watermark toàn cục, partition ownership, failover/failback, fencing | Mạnh |
| Aggregator | Heuristic | Tổng hợp watermark heuristic, active/standby availability | Nhẹ |

**Nội dung thiết kế trong mục Coordinator và Aggregator.**

| Nội dung | Mô tả |
| :--- | :--- |
| Coordinator | Control plane mạnh cho Strict: tính global watermark, quản lý owner partition, phát hiện failover/failback, áp dụng fencing/epoch. |
| Aggregator | Control plane nhẹ cho Heuristic: gom watermark heuristic, chọn active/standby, phục vụ availability và quan sát trạng thái. |
| Dữ liệu đầu vào | Heartbeat worker, local watermark theo partition, trạng thái partition, epoch/owner, tín hiệu worker sống/chết. |
| Quyết định đầu ra | Global watermark, danh sách partition reassignment, trạng thái active/standby, thông tin điều phối recovery. |
| Ranh giới trách nhiệm | Strict cần tính đúng và nhất quán mạnh; Heuristic ưu tiên latency, availability và tính quan sát nên không kéo toàn bộ chi phí Raft vào đường nhẹ. |

### 11.2 Vì sao thiết kế như vậy: không dùng một service chung

**Phản đề:** Coordinator và Aggregator đều lấy min watermark, có thể gộp lại.

**Bác bỏ:** Cùng phép min không đồng nghĩa cùng trách nhiệm. Coordinator thay đổi topology xử lý và ownership partition; Aggregator chỉ tổng hợp tín hiệu thống kê. Nếu gộp, service chung sẽ hoặc quá nặng cho Heuristic, hoặc quá yếu cho Strict.

### 11.3 Vì sao phép `min` vẫn xuất hiện ở cả hai mode

Trong cả hai mode, kết quả window toàn cụm phải tôn trọng partition chậm nhất trong tập đang được xét. Khác biệt là nguồn watermark:

- Strict: `LW_i` đến từ Punctuation Token, có ý nghĩa bằng chứng tiến độ.
- Heuristic: `W_h` đến từ phân vị lateness, có ý nghĩa ước lượng.

Vì nguồn dữ liệu khác nhau, hậu quả khi sai cũng khác nhau. Strict sai là có thể mất correctness tức thời; Heuristic sai thì late event đi DLQ và sửa sau.

---

## 12. Giao tiếp giữa các layer

Sơ đồ liên quan: [`mermaid/layer_communication.mmd`](mermaid/layer_communication.mmd)

Hệ thống không dùng một kênh cho mọi loại dữ liệu. Mỗi kênh tương ứng một yêu cầu kỹ thuật.

| Kênh | Dùng cho | Lý do |
| :--- | :--- | :--- |
| Kafka | Events, punctuation, results, audit, DLQ | Cần bền, partitioned, replay bằng offset |
| gRPC | Heartbeat, watermark, Raft vote/state, Aggregator watermark | Cần schema rõ, latency thấp, RPC nội bộ |
| HTTP | `/health`, `/state`, `/metrics`, dashboard/control fallback | Dễ quan sát và thao tác vận hành |
| RocksDB/MinIO clients | State, checkpoint, archive | Cần persistence ngoài RAM |

**Nội dung thiết kế trong mục giao tiếp giữa các layer.**

| Nội dung | Mô tả |
| :--- | :--- |
| Data plane | Kafka vận chuyển event, punctuation, result/audit/DLQ theo partition để đảm bảo replay và thứ tự cục bộ. |
| Control plane | gRPC truyền heartbeat, watermark, vote/state của Raft và tín hiệu Aggregator vì cần schema chặt và latency thấp. |
| Observability/control phụ | HTTP dùng cho health, state, metrics, dashboard hoặc thao tác vận hành không nằm trên hot path xử lý event. |
| Persistence plane | RocksDB và MinIO client ghi/đọc state, checkpoint, closed window và archive. |
| Ranh giới layer | Mỗi layer chỉ dùng kênh phù hợp với yêu cầu của dữ liệu nó xử lý, tránh ép toàn hệ thống vào một giao thức duy nhất. |

**Phản đề:** Dùng HTTP toàn bộ để dễ debug.

**Bác bỏ:** HTTP không thay được log bền có offset của Kafka; cũng không phù hợp cho stream event lớn cần replay. Ngược lại, Kafka không thay được HTTP health/control vì operator cần truy vấn trạng thái nhanh. Tách kênh giúp data traffic không làm nghẽn control/observability.

---

## 13. Thiết kế chi tiết Failover và Failback 5 bước

Sơ đồ liên quan: [`mermaid/failback_5step_seq.mmd`](mermaid/failback_5step_seq.mmd), [`mermaid/partition_state_machine.mmd`](mermaid/partition_state_machine.mmd)

### 13.1 Thiết kế chi tiết failover

Khi worker không gửi heartbeat quá ngưỡng, `FailoverManager.detect_failures` đánh dấu worker `FAILED`. Các partition orphaned được phân bổ lại vòng tròn sang worker còn sống qua `reassign_failed_partitions` hoặc `cascading_failover`.

**Nội dung thiết kế trong mục failover.**

| Nội dung | Mô tả |
| :--- | :--- |
| Tín hiệu phát hiện lỗi | Heartbeat timeout, trạng thái worker, danh sách partition đang thuộc worker lỗi. |
| Quyết định điều phối | Coordinator đánh dấu worker `FAILED`, xác định partition orphaned và chọn worker sống để nhận lại partition. |
| Dữ liệu cần bảo toàn | Owner/epoch của partition, checkpoint/offset cuối an toàn, trạng thái failover đã persist để coordinator restart không quên quyết định. |
| Hành động phục hồi | Worker mới nhận partition, đọc checkpoint, seek Kafka, tiếp tục xử lý từ điểm an toàn. |
| Đầu ra của thiết kế | Partition có owner mới rõ ràng và không để nhiều worker cùng xử lý một partition sau lỗi. |

**Phản đề:** Khi worker chết, cứ để một worker bất kỳ consume partition đó.

**Bác bỏ:** Nếu không có authority trung tâm, nhiều worker có thể cùng consume. Nếu không biết offset/checkpoint cuối, worker mới có thể bỏ sót hoặc xử lý lại quá nhiều. Nếu không ghi trạng thái reassign, coordinator restart sẽ quên quá trình failover.

### 13.2 Thiết kế chi tiết failback 5 bước

Khi worker cũ phục hồi, nó không tự lấy partition lại. Failback đi qua các bước:

1. **PAUSE:** dừng partition ở worker đang gánh hộ.
2. **FLUSH_ACK:** worker gánh hộ flush state và offset.
3. **KAFKA_REASSIGN:** control plane đổi owner.
4. **SEEK_RESUME:** worker phục hồi đọc checkpoint và seek Kafka.
5. **COMPLETE:** partition trở lại trạng thái assigned bình thường.

**Bằng chứng code:** `FailoverManager.start_failback` tạo `FailbackState` ở bước `PAUSE`; `advance_failback` chuyển tuần tự qua `FLUSH_ACK`, `KAFKA_REASSIGN`, `SEEK_RESUME`, `COMPLETE`, cập nhật owner và xóa persisted failback state khi hoàn tất.

**Nội dung thiết kế trong mục failback.**

| Nội dung | Mô tả |
| :--- | :--- |
| Mục tiêu | Đưa partition từ worker đang gánh hộ về worker cũ đã phục hồi mà không làm mất offset continuity. |
| Năm bước bắt buộc | PAUSE, FLUSH_ACK, KAFKA_REASSIGN, SEEK_RESUME, COMPLETE. |
| Trạng thái cần persist | Partition id, source worker, target worker, step hiện tại, checkpoint/offset liên quan, thời điểm bắt đầu và lỗi nếu có. |
| Điều kiện chuyển bước | Mỗi bước chỉ chuyển tiếp khi bước trước đã hoàn tất hoặc có ack rõ ràng, tránh vừa chuyển owner vừa còn state chưa flush. |
| Đầu ra của thiết kế | Partition trở lại owner mong muốn, worker mới seek đúng offset, failback state được xóa khi hoàn tất. |

**Kết luận thiết kế:** Năm bước không phải nghi thức thừa. Chúng bảo vệ hai invariant: chỉ một owner tại một thời điểm và offset continuity khi chuyển giao.

---

## 14. Invariant đúng đắn

| Invariant | Vì sao bắt buộc | Thành phần bảo vệ |
| :--- | :--- | :--- |
| Một partition chỉ có một owner | Tránh duplicate consume/output | Coordinator, FailoverManager, fencing token, partition state machine |
| Watermark không giảm | Tránh mở lại window đã chốt | `max(prev, candidate)` trong Coordinator/Aggregator/Engine |
| Window chỉ đóng khi watermark vượt `window_end` | Tránh phát thiếu dữ liệu | Strict/Heuristic engine `_advance_watermark` và `_close_windows` |
| Strict không tự bỏ partition im lặng | Tránh coi dữ liệu hợp lệ là late | Punctuation, status classification, temporary-idle tường minh |
| Late event Heuristic không mất thầm lặng | Bảo vệ eventual completeness | DLQPipeline, CorrectionProtocol |
| Checkpoint đi kèm offset | Recovery không bỏ sót/không lặp vô hạn | RocksDB checkpoint, Kafka seek/resume |
| Output không phát trùng theo window | Exactly-once/idempotent emission | OutputManager `_emitted`, transactional sink, persisted emitted set |
| Correction không áp dụng trùng | Không cộng double late event | CorrectionProtocol `correction_id` dedup |

Một phương án đơn giản hơn chỉ được chấp nhận nếu không phá các invariant trên. Phần lớn phương án "gọn hơn" bị loại vì phá ít nhất một invariant.

---

## 15. Kết quả thực nghiệm

### 15.1 Strict offline sweep

Nguồn: `docs/strict_sweep_20260602_203750.csv` và `reports/results/analysis_report.md`.

| `δ` | Wait time | Immediate completeness | Late rate | Late events |
| ---: | ---: | ---: | ---: | ---: |
| 0s | 0ms | 8.753% | 91.247% | 2,702,216 |
| 10s | 10,000ms | 59.175% | 40.825% | 1,208,999 |
| 30s | 30,000ms | 93.228% | 6.772% | 200,549 |
| 40s | 40,000ms | 96.724% | 3.276% | 97,013 |
| 60s | 60,000ms | 99.271% | 0.729% | 21,591 |
| 120s | 120,000ms | 99.972% | 0.028% | 833 |

Nhận xét:

- Đường completeness tăng theo `δ`, đúng với lý thuyết `completeness(δ) ≈ CDF(lateness <= δ)`.
- Lợi ích biên giảm mạnh sau khoảng p95/p99. Từ 60s lên 120s chỉ tăng 0.701 điểm phần trăm nhưng thêm 60s wait.
- Nếu cần 100% strict trên dataset này bằng wait cố định, `δ` phải tiệm cận max lateness 359.733s, không thực tế cho low-latency.

### 15.2 Strict Docker/runtime sweep

Nguồn: `reports/results/completeness_vs_wait_strict_20260603_003718.csv`.

| `δ` | Completeness final | Proc p99 |
| ---: | ---: | ---: |
| 0s | 83.085% | 7752.8 us |
| 5s | 88.153% | 4666.4 us |
| 10s | 90.145% | 5610.5 us |
| 40s | 97.565% | 5884.8 us |
| 90s | 99.301% | 7816.0 us |
| 120s | 98.739% | 5678.0 us |

Runtime sweep vẫn cho cùng xu hướng tổng quát: tăng wait giúp tăng completeness. Một số điểm không đơn điệu tuyệt đối do runtime drain, pacing, backpressure, EOF settle và cách lấy mẫu sau xử lý. Vì vậy offline sweep dùng để đọc quan hệ lý thuyết sạch hơn; Docker sweep dùng để chứng minh pipeline thực thi được qua Kafka/worker/coordinator.

### 15.3 Heuristic sweep

Nguồn: `docs/heuristic_sweep_20260602_203750.csv`.

| `p_normal` | `L_eff` | Immediate completeness | DLQ routed | Eventual completeness |
| ---: | ---: | ---: | ---: | ---: |
| 0.500 | 11.633s | 64.206% | 1,060,021 | 100.000% |
| 0.750 | 18.667s | 81.263% | 554,876 | 100.000% |
| 0.900 | 28.800s | 91.305% | 257,506 | 100.000% |
| 0.950 | 37.783s | 94.767% | 154,984 | 100.000% |
| 0.990 | 59.700s | 97.699% | 68,148 | 100.000% |
| 0.999 | 95.034s | 98.376% | 48,099 | 100.000% |

Nhận xét:

- `L_eff` khớp trực tiếp với percentile lateness, chứng minh DDSketch/percentile là đúng biến điều khiển.
- Tăng `p_normal` giảm DLQ nhưng tăng wait.
- Heuristic tách hai mục tiêu: kết quả phát lần đầu có thể thiếu một phần, nhưng eventual correctness được phục hồi qua DLQ/correction.

### 15.4 So sánh kết luận

| Câu hỏi | Strict | Heuristic |
| :--- | :--- | :--- |
| Muốn đúng ngay khi phát? | Phù hợp hơn | Không phải mục tiêu chính |
| Muốn latency watermark thấp? | Khó nếu tail lateness dài | Phù hợp hơn |
| Chấp nhận correction sau? | Không cần | Bắt buộc |
| Cần vận hành đơn giản? | Đơn giản hơn về downstream, phức tạp hơn control plane | Phức tạp hơn downstream do DLQ/correction |
| Cần chịu lỗi partition ownership? | Có thiết kế mạnh hơn | Không điều phối ownership ở Aggregator |

---

## 16. Phản biện tổng hợp các phương án đơn giản hơn

| Phương án đơn giản hơn | Tưởng như lợi ích | Lỗi thiết kế cụ thể | Invariant/SLA bị phá |
| :--- | :--- | :--- | :--- |
| Ingestor gửi HTTP trực tiếp vào Worker | Bỏ Kafka, dễ code | Không có offset bền, replay khó, worker crash có thể mất event | Recovery, input durability |
| Kafka tự sort event-time | Bỏ heap ở Worker | Kafka sort theo offset, không sort theo event-time | Event-time ordering |
| Worker tự chốt window | Giảm Coordinator/Aggregator | Không thấy partition khác, có thể đóng sớm | Global completeness |
| Gộp watermark cấp node | Message nhỏ hơn | Mất thông tin partition skew và failover | Per-partition accountability |
| Bỏ partition idle khỏi min tự động | Watermark nhanh hơn | Partition sống lại với event cũ sẽ bị late/drop | Strict correctness |
| ZK/file lock thay Raft cho Strict | Control plane nhẹ hơn | Không đủ term/fencing cho lệnh cũ và failback | Single-owner, exactly-once |
| EWMA thay DDSketch | Dễ implement | Không đo p95/p99 tail | Heuristic loss budget |
| Heuristic không có DLQ | Downstream đơn giản hơn | Late event mất thầm lặng | Eventual completeness |
| Một RocksDB chung cho node | Ít DB hơn | Failover từng partition khó, lock/state không tách bạch | Partition-level recovery |
| Chỉ dùng MinIO cho state | Bền và share được | Latency hot path quá cao | Throughput/latency |
| Bỏ failback 5 bước | Recovery nhanh hơn | Hai owner hoặc mất offset cuối | Single-owner, offset continuity |

---

## 17. Giới hạn và giả định

| Giới hạn / giả định | Ảnh hưởng | Cách trình bày khi phản biện |
| :--- | :--- | :--- |
| Chạy chủ yếu trong Docker/local | Chưa phải benchmark production nhiều máy vật lý | Kết quả chứng minh giao thức và trade-off; production cần benchmark network thật |
| Raft là implementation mô phỏng/thực nghiệm | Chưa tương đương etcd/Raft library production | Dùng để thể hiện term, role, replication, fencing; có thể thay bằng thư viện chuẩn |
| Strict sweep với `δ` hữu hạn có late/drop | Không nên nói "Strict luôn 100%" trong mọi cấu hình | Nói rõ điều kiện: EOF/data-driven punctuation hoặc `δ >= max lateness` mới đạt đầy đủ |
| Heuristic eventual completeness phụ thuộc DLQ chạy thật | Nếu DLQ retention/reconciliation lỗi thì eventual completeness không tự xảy ra | DLQ là thành phần bắt buộc của mode, cần monitor backlog và correction latency |
| Dataset từ taxi được ánh xạ sang log | Không phải production web log thật | Dữ liệu có phân phối lateness thực và out-of-order đủ để nghiên cứu watermark |
| MinIO/Shared Volume local | Chưa chứng minh DR nhiều vùng | Chứng minh mô hình tiered storage; production cần HA storage thật |
| Clock skew live chưa đo trên nhiều máy | Negative lag handler có nhưng môi trường local ít skew thật | Cần test bổ sung khi deploy phân tán vật lý |

---

## 18. Câu hỏi phản biện dự kiến và câu trả lời ngắn

### 18.1 Vì sao không chỉ dùng Flink/Kafka Streams?

Mục tiêu đề tài là hiện thực và đánh giá cơ chế watermark phân tán, không chỉ dùng framework có sẵn. Hệ thống tự implement partition ownership, punctuation, DDSketch, DLQ, checkpoint và failback để làm rõ trade-off. Framework như Flink có thể là baseline tương lai, nhưng không thay phần nghiên cứu thiết kế.

### 18.2 Strict có thật sự 0% loss không?

Strict không nên được phát biểu mơ hồ. Trong mode có punctuation an toàn và EOF/data-driven flush, nó không đóng window trước khi có bằng chứng nên không mất dữ liệu do late arrival. Trong experiment `δ` hữu hạn, Strict được dùng để vẽ đường completeness-vs-wait, nên late event vượt `δ` vẫn bị tính là late/drop. Báo cáo vì vậy tách "cam kết giao thức" và "kết quả sweep".

### 18.3 Heuristic có phải chỉ là chấp nhận sai không?

Không. Heuristic chấp nhận sai số tức thời có kiểm soát để giảm latency, nhưng không drop thầm lặng. Sai số được đưa vào DLQ và correction. Đó là mô hình eventual consistency, không phải bỏ qua correctness.

### 18.4 Vì sao p=0.50 của Heuristic lại immediate completeness 64.206%, không phải 50%?

`p` điều khiển `L_eff` theo percentile lateness, nhưng immediate completeness thực tế còn chịu ảnh hưởng window boundary, local/global close, warm-up, rate limit, burst/hysteresis và cách event phân bố theo partition. Vì vậy nó không nhất thiết bằng đúng `p * 100%`, dù xu hướng tăng theo `p` là rõ.

### 18.5 Vì sao phải có cả Coordinator và Aggregator?

Vì trách nhiệm khác nhau. Coordinator trong Strict là authority điều phối ownership và failback, cần fencing. Aggregator trong Heuristic chỉ gom watermark thống kê, có thể nhẹ hơn. Gộp chúng làm hoặc Strict yếu đi, hoặc Heuristic nặng lên.

### 18.6 Vì sao ba tầng lưu trữ không phải over-engineering?

RocksDB local phục vụ hot path, Shared Volume/checkpoint phục vụ failover nhanh, MinIO phục vụ archive/DR. Một tầng không thể đồng thời nhanh, failover được và bền lâu. Ba tầng là tách trách nhiệm, không phải thêm thành phần tùy tiện.

---

## 19. Kết luận

Thiết kế của Distributed Watermark Tracker xuất phát từ một mâu thuẫn cơ bản trong stream processing: completeness và latency không thể tối ưu tuyệt đối bằng một nút chỉnh đơn giản. Strict Watermark chọn correctness tức thời, vì vậy cần Punctuation Token, Coordinator, per-partition watermark, fencing và failback có kiểm soát. Heuristic Watermark chọn latency thấp, vì vậy cần DDSketch để ước lượng tail lateness, cold start/hysteresis để ổn định watermark, và DLQ/correction để bảo vệ eventual completeness.

Các quyết định như Kafka, Bounded Priority Queue, per-partition engine, tiered storage, Raft/fencing và failback 5 bước đều có lý do gắn với invariant cụ thể. Khi phản biện từng phương án đơn giản hơn, có thể thấy đa số phương án đó chỉ "gọn" vì bỏ qua một ràng buộc phân tán: offset replay, single-owner, global completeness, state recovery hoặc late-event reconciliation.

Kết quả thực nghiệm củng cố lập luận thiết kế. Strict cho đường tăng completeness theo wait time, nhưng chịu tail lateness dài. Heuristic cho phép giảm wait bằng percentile và đạt eventual completeness nhờ DLQ, nhưng phải trả chi phí correction. Do đó, hệ thống không chỉ trình bày hai thuật toán, mà hiện thực hai chiến lược nhất quán khác nhau cho hai kiểu SLA khác nhau.

---

## Tài liệu tham khảo

1. M. Tamer Özsu, Patrick Valduriez, *Principles of Distributed Database Systems*, Springer.
2. Akidau et al., *MillWheel: Fault-Tolerant Stream Processing at Internet Scale*.
3. Carbone et al., *Apache Flink: Stream and Batch Processing in a Single Engine*.
4. Li et al., *Out-of-Order Processing: A New Architecture for High-Performance Stream Systems*.
5. Nội bộ dự án: [`REPORT_OZSU_VALDURIEZ_DESIGN_JUSTIFICATION.md`](REPORT_OZSU_VALDURIEZ_DESIGN_JUSTIFICATION.md).
6. Nội bộ dự án: [`reports/results/analysis_report.md`](../../reports/results/analysis_report.md).
7. Nội bộ dự án: [`docs/strict_sweep_20260602_203750.csv`](../strict_sweep_20260602_203750.csv), [`docs/heuristic_sweep_20260602_203750.csv`](../heuristic_sweep_20260602_203750.csv).

---

## Phụ lục A: Danh mục sơ đồ

| Nội dung | File |
| :--- | :--- |
| Tổng quan domain | [`mermaid/domain_overview.mmd`](mermaid/domain_overview.mmd) |
| Data ingestion domain | [`mermaid/domain1_data_ingestion.mmd`](mermaid/domain1_data_ingestion.mmd) |
| Strict coordination domain | [`mermaid/domain2_strict_coordination.mmd`](mermaid/domain2_strict_coordination.mmd) |
| Heuristic aggregation domain | [`mermaid/domain3_heuristic_aggregation.mmd`](mermaid/domain3_heuristic_aggregation.mmd) |
| Metadata/control domain | [`mermaid/domain4_control_metadata.mmd`](mermaid/domain4_control_metadata.mmd) |
| State/storage domain | [`mermaid/domain5_state_storage.mmd`](mermaid/domain5_state_storage.mmd) |
| Strict Watermark sequence | [`mermaid/strict_watermark_seq.mmd`](mermaid/strict_watermark_seq.mmd) |
| Heuristic Watermark sequence | [`mermaid/heuristic_watermark_seq.mmd`](mermaid/heuristic_watermark_seq.mmd) |
| Node quản lý partition | [`mermaid/node_partition_engine.mmd`](mermaid/node_partition_engine.mmd) |
| Kafka + Bounded PQ | [`mermaid/kafka_bounded_pq.mmd`](mermaid/kafka_bounded_pq.mmd) |
| Tiered Storage | [`mermaid/tiered_storage_cycles.mmd`](mermaid/tiered_storage_cycles.mmd) |
| Eviction state machine | [`mermaid/eviction_state_machine.mmd`](mermaid/eviction_state_machine.mmd) |
| Coordinator vs Aggregator | [`mermaid/coordinator_vs_aggregator.mmd`](mermaid/coordinator_vs_aggregator.mmd) |
| Giao tiếp layer | [`mermaid/layer_communication.mmd`](mermaid/layer_communication.mmd) |
| Failback 5 bước | [`mermaid/failback_5step_seq.mmd`](mermaid/failback_5step_seq.mmd) |
| Partition state machine | [`mermaid/partition_state_machine.mmd`](mermaid/partition_state_machine.mmd) |

## Phụ lục B: Checklist bảo vệ trước hội đồng

| Nội dung cần nói | Đã có trong báo cáo | Bằng chứng |
| :--- | :--- | :--- |
| Bài toán event-time và out-of-order | Có | Mục 1, 2 |
| Kiến trúc tổng quan theo plane/domain | Có | Mục 5, sơ đồ domain |
| Strict Watermark và điều kiện correctness | Có | Mục 6 |
| Heuristic Watermark và DLQ/correction | Có | Mục 7 |
| Kafka, partition, bounded queue | Có | Mục 8, 9 |
| Tiered storage và recovery | Có | Mục 10 |
| Coordinator vs Aggregator | Có | Mục 11 |
| Failover/failback 5 bước | Có | Mục 13 |
| Invariant đúng đắn | Có | Mục 14 |
| Kết quả thực nghiệm | Có | Mục 15 |
| Giới hạn, giả định, threat to validity | Có | Mục 17 |
| Câu hỏi phản biện dự kiến | Có | Mục 18 |
