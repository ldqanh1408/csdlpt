"""
nasa_loader.py
--------------
Đọc dataset web log THẬT: NASA-HTTP (Internet Traffic Archive, 1995).

Định dạng mỗi dòng (Common Log Format của NASA):
  host - - [DD/Mon/YYYY:HH:MM:SS -0400] "METHOD /path HTTP/1.0" status bytes

NASA-HTTP chỉ có 1 mốc thời gian = lúc request xảy ra => dùng làm
EVENT-TIME thật. Hệ phân tán không bao giờ ghi sẵn "log tới lúc nào",
nên ARRIVAL-TIME được SINH = event_time + độ trễ mô phỏng. Đây là cách
mô hình hóa "communication delay / message out-of-order" trong mô hình
lỗi của Özsu & Valduriez — phần out-of-order BẮT BUỘC phải tự sinh dù
dùng dữ liệu thật.

Cách dùng:
  1) Tải NASA_access_log_Jul95.gz (Internet Traffic Archive / Kaggle).
  2) df = load_nasa("NASA_access_log_Jul95.gz", limit=200_000)
  3) Đưa df vào watermark_engine y như log_generator (cùng schema:
     event_id, event_time, arrival_time, endpoint, status).
"""
import re
import gzip
import random
from datetime import datetime, timezone, timedelta

import pandas as pd


def load_nasa_csv(
    path: str = "dataset/data.csv",
    limit: int | None = 200_000,
    seed: int = 42,
    out_of_order_fraction: float = 0.30,
    normal_delay_ms=(0, 200),
    late_delay_ms=(500, 8000),
    duplicate_fraction: float = 0.02,
) -> pd.DataFrame:
    """Đọc NASA-HTTP đã pre-parse sang CSV (schema: index, host, time,
    method, url, response, bytes). `time` là epoch giây = EVENT-TIME thật.

    Phải SINH `arrival_time` = event_time + delay mô phỏng (out-of-order) vì
    log thật không lưu lúc tới hệ stream — mô hình hoá 'communication delay'
    [Ö&V, Distributed Reliability] cho dữ liệu thật.
    """
    rng = random.Random(seed)
    usecols = ["host", "time", "method", "url", "response", "bytes"]
    if limit:
        df = pd.read_csv(path, usecols=usecols, nrows=limit)
    else:
        df = pd.read_csv(path, usecols=usecols)

    # Dòng có status hỏng (vd '-') -> ép 0, không sập [Robustness].
    df["status"] = pd.to_numeric(df["response"], errors="coerce").fillna(0).astype(int)
    df["event_time"] = pd.to_numeric(df["time"], errors="coerce")
    df = df.dropna(subset=["event_time"]).reset_index(drop=True)

    # Sinh delay (vector hoá, nhanh trên 200k dòng).
    n = len(df)
    late_lo, late_hi = late_delay_ms
    norm_lo, norm_hi = normal_delay_ms
    is_late = [rng.random() < out_of_order_fraction for _ in range(n)]
    delays = [
        (rng.uniform(late_lo, late_hi) if is_late[i] else rng.uniform(norm_lo, norm_hi)) / 1000.0
        for i in range(n)
    ]
    df["arrival_time"] = df["event_time"] + pd.Series(delays)
    df["endpoint"] = df["url"].astype(str)
    df["event_id"] = [f"nasa-{i}" for i in range(n)]
    df = df[["event_id", "event_time", "arrival_time", "endpoint", "status", "host"]]

    # Bản ghi trùng (mô phỏng retransmission) -> test deduplication.
    dup_mask = pd.Series([rng.random() < duplicate_fraction for _ in range(n)])
    dups = df[dup_mask].copy()
    if len(dups):
        dups["arrival_time"] = dups["arrival_time"] + pd.Series(
            [rng.uniform(0, 1.0) for _ in range(len(dups))], index=dups.index)
        df = pd.concat([df, dups], ignore_index=True)

    df = df.sort_values("arrival_time").reset_index(drop=True)
    print(f"[nasa_loader.csv] rows={n} duplicates_added={len(dups)} total={len(df)}")
    return df


# host ... [timestamp] "request" status bytes
LOG_RE = re.compile(
    r'^(\S+)\s+\S+\s+\S+\s+\[([^\]]+)\]\s+"([^"]*)"\s+(\d{3}|-)\s+(\S+)'
)
TS_FMT = "%d/%b/%Y:%H:%M:%S %z"


def _open(path):
    return gzip.open(path, "rt", errors="replace") if path.endswith(".gz") \
        else open(path, "r", errors="replace")


def load_nasa(
    path: str,
    limit: int | None = 200_000,
    seed: int = 42,
    out_of_order_fraction: float = 0.30,
    normal_delay_ms=(0, 200),
    late_delay_ms=(500, 8000),
    duplicate_fraction: float = 0.02,
) -> pd.DataFrame:
    rng = random.Random(seed)
    rows, bad = [], 0

    with _open(path) as f:
        for i, line in enumerate(f):
            if limit and len(rows) >= limit:
                break
            m = LOG_RE.match(line)
            if not m:
                bad += 1                       # dòng hỏng -> đếm, KHÔNG crash
                continue
            host, ts_raw, request, status, nbytes = m.groups()
            try:
                dt = datetime.strptime(ts_raw, TS_FMT)
                event_time = dt.timestamp()    # epoch giây = EVENT-TIME thật
            except ValueError:
                bad += 1
                continue

            parts = request.split()
            endpoint = parts[1] if len(parts) >= 2 else "-"
            try:
                status_i = int(status)
            except ValueError:
                status_i = 0

            # ARRIVAL-TIME = event-time + độ trễ MÔ PHỎNG (out-of-order)
            if rng.random() < out_of_order_fraction:
                delay = rng.uniform(*late_delay_ms) / 1000.0
            else:
                delay = rng.uniform(*normal_delay_ms) / 1000.0

            rows.append({
                "event_id": f"nasa-{i}",
                "event_time": event_time,
                "arrival_time": event_time + delay,
                "endpoint": endpoint,
                "status": status_i,
                "host": host,
            })

    df = pd.DataFrame(rows)

    # Bản ghi trùng (mô phỏng retransmission) -> test deduplication
    dups = []
    for r in df.to_dict("records"):
        if rng.random() < duplicate_fraction:
            d = dict(r)
            d["arrival_time"] += rng.uniform(0, 1.0)
            dups.append(d)
    if dups:
        df = pd.concat([df, pd.DataFrame(dups)], ignore_index=True)

    # Stream xử lý theo THỨ TỰ TỚI (arrival-time), KHÔNG theo event-time
    df = df.sort_values("arrival_time").reset_index(drop=True)
    print(f"[nasa_loader] parsed={len(rows)} bad_lines={bad} "
          f"duplicates_added={len(dups)} total_rows={len(df)}")
    return df


if __name__ == "__main__":
    import sys
    p = sys.argv[1] if len(sys.argv) > 1 else "NASA_access_log_Jul95.gz"
    df = load_nasa(p, limit=50_000)
    print(df.head(8).to_string(index=False))
    print("Status codes:", df["status"].value_counts().head().to_dict())
