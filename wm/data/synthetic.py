"""
log_generator.py
-----------------
Sinh dataset "Web_Server_Logs" mô phỏng tình huống đề bài:
- Mỗi log có EVENT-TIME (lúc request thực sự xảy ra)
- và ARRIVAL-TIME / processing-time (lúc log tới được hệ thống stream).
- Một phần log "Out-of-Order": tới trễ rất nhiều (vd: event lúc 10:01
  nhưng 10:05 mới tới hệ thống).
- Có cả bản ghi DUPLICATE (mô phỏng network retransmission) để test
  phần Robustness (deduplication).

Thời gian biểu diễn bằng giây (float) cho gọn; muốn ra giờ:phút thì
chỉ cần format lại.
"""
import random
import pandas as pd

BASE_EPOCH = 1_700_000_000.0  # mốc thời gian gốc tùy ý (giây)


def generate_logs(
    n_events: int = 5000,
    duration_s: float = 300.0,
    seed: int = 42,
    out_of_order_fraction: float = 0.30,   # 30% log bị trễ nhiều
    normal_delay_ms=(0, 200),              # log bình thường: trễ 0-200ms
    late_delay_ms=(500, 8000),             # log trễ: 0.5s - 8s
    duplicate_fraction: float = 0.02,      # 2% log bị gửi lặp
) -> pd.DataFrame:
    rng = random.Random(seed)
    events = []

    for i in range(n_events):
        event_time = BASE_EPOCH + rng.uniform(0, duration_s)

        if rng.random() < out_of_order_fraction:
            delay = rng.uniform(*late_delay_ms) / 1000.0   # log tới TRỄ
        else:
            delay = rng.uniform(*normal_delay_ms) / 1000.0

        events.append({
            "event_id": f"evt-{i}",
            "event_time": event_time,                 # khi request xảy ra
            "arrival_time": event_time + delay,       # khi log tới hệ thống
            "endpoint": rng.choice(
                ["/", "/login", "/api", "/static", "/checkout"]),
            "status": rng.choices([200, 404, 500],
                                  weights=[0.90, 0.07, 0.03])[0],
        })

    # Tạo bản ghi trùng (duplicate) -> dùng test deduplication
    duplicates = []
    for e in events:
        if rng.random() < duplicate_fraction:
            d = dict(e)
            d["arrival_time"] = e["arrival_time"] + rng.uniform(0, 1.0)
            duplicates.append(d)
    events.extend(duplicates)

    # Stream được xử lý theo THỨ TỰ TỚI (processing order), KHÔNG phải
    # theo event-time -> đây chính là gốc rễ của vấn đề out-of-order.
    events.sort(key=lambda e: e["arrival_time"])
    return pd.DataFrame(events)


if __name__ == "__main__":
    df = generate_logs()
    print(df.head(10).to_string(index=False))
    print(f"\nTotal rows (kể cả duplicate): {len(df)}")
    print(f"Unique events: {df['event_id'].nunique()}")
