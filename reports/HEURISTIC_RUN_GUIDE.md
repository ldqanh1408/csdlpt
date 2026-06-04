# Hướng dẫn Chạy thực nghiệm Heuristic Watermark Sweep

Tài liệu này hướng dẫn cách thiết lập các cờ môi trường (environment flags) để chạy đúng thực nghiệm quét Heuristic Watermark Sweep trên các tập dữ liệu rút gọn (ví dụ: `nyc_taxi_events_half.csv` hoặc `nyc_taxi_events_sliced.csv`).

---

## 1. Danh sách các cờ (Environment Flags) quan trọng

Để thực nghiệm Heuristic chạy đúng bản chất thống kê và không bị kẹt ở mức Completeness 100% (hoặc rơi vào trạng thái BOO Fallback do trễ âm), bạn **bắt buộc** phải cấu hình các cờ sau:

| Tên biến môi trường | Giá trị khuyến nghị | Mô tả |
| :--- | :--- | :--- |
| `INGESTOR_REPLAY` | `arrival` | Bật chế độ phát lại theo nhịp độ thời gian thực của dữ liệu (paced replay). Giúp tính toán trễ luôn dương và thực tế. |
| `REPLAY_SPEED` | `150` (hoặc `50` - `200`) | Hệ số tăng tốc mô phỏng. Ví dụ: `150` tương đương phát nhanh gấp 150 lần thời gian thực (giúp sweep hoàn thành nhanh hơn). |
| `HEURISTIC_LOCAL_WATERMARK_CLOSE` | `true` | Cho phép các partition chốt cửa sổ độc lập dựa trên watermark cục bộ ($W_h = max\_event\_time - L\_eff$). Nếu đặt là `false`, hệ thống sẽ chờ watermark của cả cụm (min), dẫn đến việc bị straggler kéo tụt và completeness luôn là 100%. |
| `HEURISTIC_WARMUP_SAMPLES` | `2000` | Số lượng mẫu tối thiểu để thoát khỏi giai đoạn khởi động (Cold Start). Trên các tập dữ liệu nhỏ, nếu giữ nguyên mặc định (50,000), worker sẽ không bao giờ ấm lên và giữ nguyên độ trễ an toàn tối đa. |
| `HEURISTIC_WARMUP_S` | `5.0` | Số giây tối thiểu để thoát khỏi giai đoạn khởi động. |
| `PYTHONUNBUFFERED` | `1` | Buộc Python phải ghi trực tiếp log ra stdout mà không đệm, giúp bạn theo dõi log real-time trong file `.log`. |

---

## 2. Cách khởi chạy thực nghiệm

### Bước 1: Chuẩn bị dữ liệu 1/2 Dataset (nếu chưa có)
Trong PowerShell (Windows):
```powershell
python -c "source_path='dataset/nyc_taxi_events_full.csv'; dest_path='dataset/nyc_taxi_events_half.csv'; f_in=open(source_path,'r',encoding='utf-8'); header=f_in.readline(); rows=[f_in.readline() for _ in range(1480711)]; f_out=open(dest_path,'w',encoding='utf-8',newline=''); f_out.write(header); f_out.writelines(rows); print('Sliced half dataset!')"
```

### Bước 2: Thiết lập biến môi trường và chạy thực nghiệm

#### Trên Windows (PowerShell):
```powershell
# 1. Thiết lập các cờ môi trường
$env:INGESTOR_REPLAY = "arrival"
$env:REPLAY_SPEED = "150"
$env:HEURISTIC_LOCAL_WATERMARK_CLOSE = "true"
$env:HEURISTIC_WARMUP_SAMPLES = "2000"
$env:HEURISTIC_WARMUP_S = "5.0"
$env:PYTHONUNBUFFERED = "1"

# 2. Chạy quét thực nghiệm Heuristic
python reports/run_experiment.py --mode heuristic --punctuation max-event-time --dataset nyc_taxi_events_half.csv --ps 0.1,0.2,0.3,0.4,0.5,0.75,0.9,0.95,0.99,0.999,0.9999 --max-wait 2000 --settle 30
```

#### Trên Linux/macOS (Bash):
```bash
INGESTOR_REPLAY="arrival" \
REPLAY_SPEED="150" \
HEURISTIC_LOCAL_WATERMARK_CLOSE="true" \
HEURISTIC_WARMUP_SAMPLES="2000" \
HEURISTIC_WARMUP_S="5.0" \
PYTHONUNBUFFERED="1" \
python reports/run_experiment.py --mode heuristic --punctuation max-event-time --dataset nyc_taxi_events_half.csv --ps 0.1,0.2,0.3,0.4,0.5,0.75,0.9,0.95,0.99,0.999,0.9999 --max-wait 2000 --settle 30
```
