# Giải pháp Tối ưu hóa Hiệu năng (Performance Optimization Strategies)

Tài liệu này đề xuất các phương án và kiến trúc tối ưu nâng cao giúp tăng thông lượng xử lý (throughput), giảm độ trễ (latency) và tiết kiệm bộ nhớ RAM cho hệ thống **Distributed Watermark Tracker**.

---

## 1. Tối ưu hóa Quản lý Trạng thái & I/O (State & Storage)

### 1.1. Thay thế Python Set bằng Bloom Filter (Khử trùng lặp `seen_ids`)
*   **Vấn đề hiện tại:** Danh sách `seen_ids` lưu giữ tất cả các ID sự kiện đã xử lý bằng cấu trúc `set` trong RAM. Khi luồng dữ liệu chạy lâu dài (hàng triệu logs), tập hợp này sẽ phình to, tiêu tốn rất nhiều bộ nhớ RAM và làm chậm quá trình tuần tự hóa (serialization) file checkpoint.
*   **Giải pháp:** Sử dụng bộ lọc **Bloom Filter** (mảng bit kết hợp với nhiều hàm băm).
    *   *Ưu điểm:* Tiết kiệm đến 95% bộ nhớ RAM. Kích thước file checkpoint lưu trữ luôn cố định và nhỏ gọn.
    *   *Đánh đổi:* Chấp nhận một tỷ lệ cực nhỏ lỗi dương tính giả (false positive - nhận nhầm log chưa từng thấy là trùng lặp), nhưng có thể kiểm soát được bằng cấu hình số hàm băm.
*   **Kỹ thuật code:** `config.py` đã chuẩn bị sẵn trường `dedup_mode: str = "set"` làm cơ sở để mở rộng sang cấu hình `"bloom"` trong tương lai.

### 1.2. Tuần tự hóa nhị phân (MessagePack / BSON) thay thế cho JSON
*   **Vấn đề hiện tại:** Ghi checkpoint bằng tệp `JSON` tốn thời gian parse chuỗi văn bản và tạo dung lượng file lớn.
*   **Giải pháp:** Sử dụng thư viện `msgpack` hoặc định dạng nhị phân `BSON` để tuần tự hóa trạng thái Engine.
    *   *Hiệu quả:* Tốc độ đọc/ghi checkpoint nhanh gấp 3 đến 5 lần. Kích thước file nén nhị phân giảm khoảng 60% so với JSON thông thường.

### 1.3. Áp dụng RocksDB làm State Store (Giống Apache Flink)
*   **Vấn đề hiện tại:** Toàn bộ trạng thái tích lũy của cửa sổ (`self.windows`) nằm hoàn toàn trên RAM. Nếu lượng cửa sổ lớn, RAM sẽ bị quá tải.
*   **Giải pháp:** Sử dụng **RocksDB** (một embedded key-value store hiệu năng cao ghi đĩa tuần tự) thông qua thư viện `rocksdb-python`.
    *   *Ưu điểm:* Cho phép ghi checkpoint gia tăng (**Incremental Checkpointing**) bằng cách chỉ đồng bộ các file SSTable mới thay đổi, thay vì phải ghi đè toàn bộ trạng thái như hiện tại.

---

## 2. Tối ưu hóa Xử lý Song song (Concurrency & CPU)

### 2.1. Kiến trúc Đa tiến trình (Multiprocessing Cluster)
*   **Vấn đề hiện tại:** Do GIL (Global Interpreter Lock) của Python, toàn bộ cụm $N$ node chạy tuần tự trên một luồng CPU duy nhất của tiến trình Streamlit.
*   **Giải pháp:** Đưa mỗi Node Engine vào một tiến trình độc lập (`multiprocessing.Process` hoặc `ProcessPoolExecutor`).
    *   *Hạ tầng:* Kết nối các Node và Coordinator qua hàng đợi liên tiến trình (`multiprocessing.Queue`) hoặc giao thức mạng siêu nhẹ **ZeroMQ (ZMQ)**.
    *   *Hiệu quả:* Tận dụng tối đa kiến trúc đa nhân (Multi-core CPU) của máy tính, tăng thông lượng xử lý thô song song lên gấp $N$ lần.

---

## 3. Tối ưu hóa Ngôn ngữ & Trình thông dịch (Runtime Execution)

### 3.1. Chạy trên trình thông dịch PyPy
*   **Cách thực hiện:** Chạy ứng dụng bằng **PyPy** thay cho trình thông dịch CPython mặc định.
*   **Cơ chế:** PyPy tích hợp trình biên dịch JIT (Just-In-Time) tự động tối ưu hóa các vòng lặp số học, giúp tăng tốc độ xử lý logic thô của Watermark Engine từ **2 đến 7 lần** mà không cần sửa bất kỳ dòng mã nguồn nào.

### 3.2. Viết lại lõi tính toán bằng Cython / Rust (PyO3)
*   **Giải pháp:** Chuyển đổi module tính toán chính của lớp `WatermarkEngine` và hàm băm phân hoạch `partition_key` sang **Cython** hoặc **Rust** (sử dụng thư viện kết nối `PyO3`).
*   **Kết quả:** Giải phóng GIL của Python, tốc độ thực thi xử lý sự kiện đạt mức tối đa cận biên của phần cứng vật lý.

---

## Bảng so sánh các phương án tối ưu

| Phương pháp | Độ phức tạp triển khai | Tài nguyên RAM | Tốc độ CPU | Tác động mã nguồn |
| :--- | :--- | :--- | :--- | :--- |
| **Trình thông dịch PyPy** | Rất thấp (chỉ cần đổi run command) | Giữ nguyên | Tăng **2x - 5x** | Không thay đổi |
| **Bloom Filter (Dedup)** | Trung bình (viết lớp Bloom Filter) | Giảm **90%** | Tăng nhẹ | Sửa đổi nhỏ ở `process()` |
| **MessagePack Checkpoint** | Thấp (thay `json.dump` bằng `msgpack`) | Giữ nguyên | Tăng **3x (I/O)**| Sửa đổi nhỏ ở `checkpoint()` |
| **Multiprocessing Cluster**| Cao (sử dụng ZMQ hoặc IPC Queue) | Tăng nhẹ | Tăng **gấp N lần** | Cấu trúc lại luồng Cluster |
| **Rust / Cython Core** | Rất cao (đòi hỏi compiler) | Giảm nhẹ | Tăng **10x - 50x**| Viết lại module `WatermarkEngine` |
