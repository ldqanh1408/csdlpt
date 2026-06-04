"""
End-to-end test cho từng tổ hợp (watermark mode × punctuation mode).

Mục tiêu (e2e/goal): nạp một stream sự kiện qua đúng đường xử lý của worker/engine,
phát punctuation token đúng theo công thức T_commit mà ingestor dùng (run.py:send_punctuation),
rồi xác nhận hệ thống đạt mục tiêu của từng chiến lược:

  - Strict  → 0% mất dữ liệu (late_dropped == 0, completeness == 100%) cho cả ba
              punctuation mode: data-driven, max-event-time, wall-clock.
  - Heuristic → không mất sự kiện (on_time + late_dropped == received) và immediate
              completeness ≈ percentile p (eventual completeness 100% qua DLQ).

Chạy hoàn toàn in-process (không cần Docker) nên dùng được trong CI.
"""

import os
import time

import pytest

from common.types import LogEvent, PunctuationToken
from strict import StrictWorker
from heuristic import HeuristicWatermarkEngine

WINDOW_S = 5.0
DELTA_S = 10.0
PUNCTUATION_MODES = ["data-driven", "max-event-time", "wall-clock"]


def _t_commit(mode, *, eof, pid, max_et_global, max_et_part,
              delta=DELTA_S, win=WINDOW_S, now=None):
    """Tái hiện công thức T_commit của ingestor (run.py:send_punctuation).

    Engine sẽ tự trừ delta_base: local_watermark = T_commit - delta.
    """
    now = time.time() if now is None else now
    if mode == "wall-clock":
        return now - delta
    if mode == "max-event-time":
        pid_et = max_et_part.get(pid, 0.0)
        if pid_et <= 0:
            return now
        return (pid_et + delta + win) if eof else pid_et
    # data-driven (default): -inf khi đang nạp, jump khi EOF để flush mọi window.
    if not eof:
        return float("-inf")
    if max_et_global > 0:
        return max_et_global + delta + win
    return now


def _make_stream(base, n_per_part, partitions):
    """Stream event-time tăng dần theo từng partition (span = n*0.2s)."""
    streams = {pid: [] for pid in partitions}
    for pid in partitions:
        for i in range(n_per_part):
            et = base + i * 0.2
            streams[pid].append(LogEvent(
                event_id=f"e-{pid}-{i}",
                event_time=et,
                status=200 if i % 10 else 500,
            ))
    return streams


@pytest.mark.parametrize("punctuation_mode", PUNCTUATION_MODES)
def test_strict_e2e_zero_loss(punctuation_mode, tmp_path, monkeypatch):
    """Strict đạt 0% mất dữ liệu cho cả ba punctuation mode."""
    monkeypatch.setenv("CHECKPOINT_DIR", str(tmp_path))
    monkeypatch.setenv("PUNCTUATION_MODE", punctuation_mode)

    partitions = [0, 1]
    base = 1000.0
    n = 40
    streams = _make_stream(base, n, partitions)

    worker = StrictWorker("w0", partitions, window_size_s=WINDOW_S, delta_base_s=DELTA_S)

    max_et_part = {pid: 0.0 for pid in partitions}
    max_et_global = 0.0
    now0 = time.time()

    # Nạp event theo "giây" rồi phát punctuation, giống nhịp ingestor.
    for i in range(n):
        for pid in partitions:
            ev = streams[pid][i]
            if punctuation_mode == "wall-clock":
                # wall-clock: event tươi (event_time ≈ now - lag nhỏ) để watermark
                # (now - 2*delta) luôn nằm sau window_end.
                ev = LogEvent(event_id=ev.event_id,
                              event_time=now0 - 0.5,
                              status=ev.status)
            # Drive the engine in arrival order. The real worker drains its
            # reorder buffer continuously (a background loop), so events reach
            # the engine while the watermark is still low; _process_event models
            # that without the buffer's wall-clock release delay (which would
            # otherwise hold every event until after EOF in a fast test loop).
            worker._process_event(ev, pid)
            max_et_part[pid] = max(max_et_part[pid], ev.event_time)
            max_et_global = max(max_et_global, ev.event_time)

        for pid in partitions:
            tc = _t_commit(punctuation_mode, eof=False, pid=pid,
                           max_et_global=max_et_global, max_et_part=max_et_part)
            worker.on_punctuation(PunctuationToken(
                T_commit=tc, partition_id=pid, ingestor_id="ing-test"))

    # EOF flush punctuation: đẩy watermark vượt window cuối để đóng hết.
    for pid in partitions:
        tc = _t_commit(punctuation_mode, eof=True, pid=pid,
                       max_et_global=max_et_global, max_et_part=max_et_part)
        worker.on_punctuation(PunctuationToken(
            T_commit=tc, partition_id=pid, ingestor_id="ing-test"))

    worker.flush_all()

    s = worker.summary()
    expected = n * len(partitions)
    assert s["total_received"] == expected, s
    assert s["late_dropped"] == 0, f"{punctuation_mode}: lost {s['late_dropped']} events\n{s}"
    assert s["data_completeness_pct"] == 100.0, s
    closed = sum(len(eng.closed_windows) for eng in worker.engines.values())
    assert closed > 0, "no windows closed — punctuation pipeline did not advance watermark"


@pytest.mark.parametrize("punctuation_mode", PUNCTUATION_MODES)
def test_heuristic_e2e_no_event_lost(punctuation_mode, tmp_path, monkeypatch):
    """Heuristic: không mất sự kiện và immediate completeness ~ p (eventual 100% qua DLQ).

    Punctuation mode không đổi logic của engine heuristic (engine dùng lateness từ
    arrival-time), nên ta xác nhận tính bất biến đó: mọi mode đều cho cùng invariant.
    """
    monkeypatch.setenv("CHECKPOINT_DIR", str(tmp_path))
    monkeypatch.setenv("PUNCTUATION_MODE", punctuation_mode)
    monkeypatch.setenv("HEURISTIC_LOCAL_WATERMARK_CLOSE", "true")
    monkeypatch.setenv("HEURISTIC_WARMUP_SAMPLES", "500")
    monkeypatch.setenv("HEURISTIC_WARMUP_S", "0.0")
    monkeypatch.setenv("HEURISTIC_P_NORMAL", "0.9")

    eng = HeuristicWatermarkEngine(partition_id=0, window_size_s=WINDOW_S, p_normal=0.9)
    eng.cold_start.warmup_min_seconds = 0.0
    eng.cold_start.warmup_min_samples = 500

    N = 6000
    arr = 0.0
    for k in range(N):
        arr += 0.02
        # lateness có đuôi: phần lớn nhỏ, một số lớn → có late drop về DLQ.
        lateness = 0.2 + (k % 50) * 0.1
        et = arr - lateness
        eng.process(LogEvent(event_id=f"h{k}", event_time=et, status=200),
                    arrival_time=arr)
    eng.flush()

    m = eng.metrics
    received = m.total_received
    accounted = m.on_time + m.late_dropped
    # Không sự kiện nào biến mất: tất cả hoặc on-time hoặc được ghi nhận late (vào DLQ).
    assert accounted == received, f"{punctuation_mode}: {accounted} != {received}"
    # Late events thực sự được giữ trong DLQ để reconcile (eventual completeness 100%).
    assert len(eng.late_events) == m.late_dropped
    immediate = 100.0 * m.on_time / max(received, 1)
    assert immediate >= 80.0, f"{punctuation_mode}: immediate completeness too low: {immediate:.1f}%"


def test_heuristic_future_dated_event_does_not_poison_watermark(monkeypatch):
    """Một event lệch tương lai (event_time ≫ arrival) KHÔNG được đẩy max_event_time /
    W_h, nếu không W_h sẽ ghim trước mọi event thật → mass false-late + BOO.

    Tái hiện đúng failure đã thấy trên Docker (W_h ghim ở giá trị ~now vượt xa
    max event_time thật của dataset).
    """
    monkeypatch.setenv("HEURISTIC_LOCAL_WATERMARK_CLOSE", "true")
    monkeypatch.setenv("HEURISTIC_WARMUP_SAMPLES", "200")
    monkeypatch.setenv("HEURISTIC_WARMUP_S", "0.0")
    monkeypatch.setenv("HEURISTIC_P_NORMAL", "0.9")

    eng = HeuristicWatermarkEngine(partition_id=0, window_size_s=WINDOW_S, p_normal=0.9)
    eng.cold_start.warmup_min_seconds = 0.0
    eng.cold_start.warmup_min_samples = 200

    base = 1_000_000.0   # dataset event-time frontier
    arr = base
    # Steady, mildly out-of-order real stream.
    for k in range(3000):
        arr += 0.1
        eng.process(LogEvent(event_id=f"r{k}", event_time=arr - 3.0, status=200),
                    arrival_time=arr)
    real_frontier = eng.max_event_time

    # POISON: one event timestamped ~1 hour in the FUTURE relative to arrival
    # (e.g. event_time defaulted to wall-clock). Must be rejected.
    eng.process(LogEvent(event_id="poison", event_time=arr + 3600.0, status=200),
                arrival_time=arr)

    assert eng.max_event_time <= real_frontier + 1.0, (
        f"future-dated event poisoned max_event_time: {eng.max_event_time} > {real_frontier}")
    assert eng.W_h <= real_frontier + 1.0, f"W_h jumped ahead of real data: {eng.W_h}"
    assert any(e.get("future_skew") for e in eng.late_events), "poison event not routed to DLQ"

    # Subsequent real events still classified on-time (watermark not jammed).
    on_before = eng.metrics.on_time
    for k in range(500):
        arr += 0.1
        eng.process(LogEvent(event_id=f"s{k}", event_time=arr - 3.0, status=200),
                    arrival_time=arr)
    assert eng.metrics.on_time - on_before >= 450, "watermark jammed after poison event"
