# -*- coding: utf-8 -*-
import os
import time
import tempfile
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

from wm import WatermarkEngine, generate_logs, load_nasa_csv

def render_stream():
    st.markdown("""
<style>
    .ingestion-card {
        background: linear-gradient(135deg, #1E293B 0%, #0F172A 100%);
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
        margin-bottom: 20px;
    }
    .card-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        border-bottom: 1px solid #334155;
        padding-bottom: 12px;
        margin-bottom: 16px;
    }
    .status-badge {
        padding: 4px 12px;
        border-radius: 999px;
        font-size: 0.75rem;
        font-weight: bold;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .status-badge.ingesting {
        background-color: rgba(16, 185, 129, 0.15);
        color: #10B981;
        border: 1px solid #10B981;
        box-shadow: 0 0 10px rgba(16, 185, 129, 0.3);
        animation: pulse-green-badge 2s infinite;
    }
    .status-badge.completed {
        background-color: rgba(59, 130, 246, 0.15);
        color: #3B82F6;
        border: 1px solid #3B82F6;
    }
    .status-badge.paused {
        background-color: rgba(245, 158, 11, 0.15);
        color: #F59E0B;
        border: 1px solid #F59E0B;
    }
    .status-badge.ready {
        background-color: rgba(148, 163, 184, 0.15);
        color: #94A3B8;
        border: 1px solid #94A3B8;
    }
    .card-grid {
        display: grid;
        grid-template-columns: repeat(4, 1fr);
        gap: 16px;
        margin-bottom: 16px;
    }
    .metric-box {
        background: rgba(30, 41, 59, 0.5);
        border: 1px solid #334155;
        border-radius: 8px;
        padding: 12px;
        display: flex;
        flex-direction: column;
    }
    .metric-label {
        font-size: 0.75rem;
        color: #94A3B8;
        margin-bottom: 4px;
    }
    .metric-val {
        font-size: 1.25rem;
        font-weight: bold;
        color: #F8FAFC;
    }
    .metric-val.text-red {
        color: #EF4444 !important;
    }
    .metric-val.text-blue {
        color: #3B82F6 !important;
    }
    .queue-monitor {
        background: rgba(15, 23, 42, 0.4);
        border: 1px solid #1E293B;
        border-radius: 8px;
        padding: 12px;
    }
    .queue-info {
        display: flex;
        justify-content: space-between;
        font-size: 0.8rem;
        color: #94A3B8;
        margin-bottom: 6px;
    }
    .queue-bar-container {
        height: 8px;
        background: #1E293B;
        border-radius: 4px;
        overflow: hidden;
    }
    .queue-bar {
        height: 100%;
        background: linear-gradient(90deg, #3B82F6 0%, #10B981 100%);
        border-radius: 4px;
        transition: width 0.3s ease;
    }
    @keyframes pulse-green-badge {
        0% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.4); }
        70% { box-shadow: 0 0 0 10px rgba(16, 185, 129, 0); }
        100% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
    }
</style>
""", unsafe_allow_html=True)

    st.header("📈 Trực quan hóa Live Stream (Data in Motion)")
    st.markdown(
        "Cửa sổ thời gian được gán theo **event-time**. Stream log nạp theo **arrival-time** "
        "(mô phỏng out-of-order). Watermark bò lên theo công thức "
        "`max_event_time − allowed_lateness` để đóng cửa sổ và chốt kết quả thời gian thực."
    )

    fw1, fw2 = st.columns(2)
    fw1.info(
        "**Watermark** = `max_event_time − Wait Time`\n\n"
        "Khi `watermark ≥ window_end` → cửa sổ đóng, kết quả được chốt.\n\n"
        "**Wait Time lớn** (Strict) → gần như không mất data, latency cao.\n\n"
        "**Wait Time nhỏ** (Heuristic) → latency thấp, có thể mất late events."
    )
    fw2.info(
        "**Hai trục thời gian:**\n\n"
        "- **Event-time**: thời điểm log THỰC SỰ xảy ra trên server\n"
        "- **Arrival-time**: thời điểm log TỚI engine (có thể trễ)\n\n"
        "Engine gán cửa sổ theo **event-time**, xử lý theo **arrival-time** "
        "→ phải dùng watermark để biết khi nào 'đủ an toàn' đóng cửa sổ."
    )

    c1, c2, c3, c4 = st.columns(4)
    stream_source = c1.selectbox("Nguồn dữ liệu", ["Synthetic", "NASA HTTP Real"], key="stream_source")
    stream_events = c2.number_input("Số sự kiện", min_value=1000, max_value=100000, value=5000, step=1000, key="stream_events")
    stream_wait = c3.slider("Wait Time (ms)", 0, 8000, 2000, 250, key="stream_wait")
    stream_win = c4.slider("Window Size (giây)", 5, 30, 10, 5, key="stream_win")

    c1_sp, _ = st.columns([1, 3])
    stream_speed = c1_sp.slider("Tốc độ vẽ (events/frame)", 20, 1000, 200, 20,
                                key="stream_speed",
                                disabled=st.session_state.get("stream_active", False))

    # ── Init stream session state ──
    if "stream_active" not in st.session_state:
        st.session_state.stream_active = False

    # ── Play / Stop / Reset row ──
    sb1, sb2, sb3 = st.columns(3)
    play_stream_btn = sb1.button(
        "Chạy Live Stream", type="primary",
        disabled=st.session_state.stream_active,
        key="play_stream_btn",
        width="stretch",
    )
    stop_stream_btn = sb2.button(
        "Dừng",
        disabled=not st.session_state.stream_active,
        key="stop_stream_btn",
        width="stretch",
    )
    reset_stream_btn = sb3.button(
        "Reset",
        disabled=st.session_state.stream_active,
        key="reset_stream_btn",
        width="stretch",
    )

    # ── Status line (native badge) ──
    if st.session_state.stream_active:
        st.error("Đang stream realtime — bấm **Dừng** để pause.", icon="🔴")
    elif st.session_state.get("stream_done", False):
        st.success("Stream hoàn tất.", icon="✅")
    elif "stream_cursor" in st.session_state and st.session_state.stream_cursor > 0:
        st.warning("Đã pause — bấm **Chạy** để tiếp tục.", icon="⏸️")
    else:
        st.info("Sẵn sàng — bấm **Chạy** để bắt đầu stream.", icon="▶️")

    # ── Handle reset ──
    if reset_stream_btn:
        for k in ("stream_active", "stream_eng", "stream_rows",
                  "stream_cursor", "stream_speed_val", "stream_source_val",
                  "stream_done", "stream_done_summary"):
            st.session_state.pop(k, None)
        st.rerun()

    # ── Handle stop ──
    if stop_stream_btn:
        st.session_state.stream_active = False
        st.rerun()

    # ── Handle play (init engine if first time, else resume) ──
    if play_stream_btn:
        # If no previous stream OR stream_done, start fresh
        need_init = (
            "stream_eng" not in st.session_state
            or st.session_state.get("stream_done", False)
            or st.session_state.get("stream_source_val") != stream_source
        )
        if need_init:
            if stream_source == "Synthetic":
                df = generate_logs(n_events=stream_events)
            else:
                if not os.path.exists("dataset/data.csv"):
                    st.error("Không tìm thấy file dataset/data.csv!")
                    st.stop()
                df = load_nasa_csv("dataset/data.csv", limit=stream_events)

            st.session_state.stream_eng = WatermarkEngine(
                window_size_s=float(stream_win),
                allowed_lateness_s=stream_wait / 1000.0,
                checkpoint_interval=1000,
                checkpoint_path=os.path.join(tempfile.gettempdir(), "app_ckpt.json"),
            )
            st.session_state.stream_rows = list(df.itertuples(index=False))
            st.session_state.stream_cursor = 0
            st.session_state.stream_source_val = stream_source
            st.session_state.stream_done = False

        st.session_state.stream_speed_val = int(stream_speed)
        st.session_state.stream_active = True
        st.rerun()

    # ── Renderer trực tiếp (không placeholder) — fragment tự re-render ──
    def _render_stream_ui(eng, src, end_idx, total_rows, qlen):
        uniq = max(eng.metrics["unique"], 1)
        comp = 100.0 * eng.metrics["on_time"] / uniq
        
        # Calculate watermark string
        wm = eng.watermark
        if wm == float("-inf"):
            wm_val = "—"
        elif wm == float("inf"):
            wm_val = "∞ (đã flush)"
        else:
            try:
                if src == "Synthetic":
                    wm_val = f"{wm - 1_700_000_000:.1f} s"
                else:
                    wm_val = time.strftime("%H:%M:%S", time.localtime(wm))
            except:
                wm_val = f"{wm:.0f}"
                
        # Determine state for status badge
        active = st.session_state.get("stream_active", False)
        done = st.session_state.get("stream_done", False)
        cursor = st.session_state.get("stream_cursor", 0)
        
        if active:
            status_text = "INGESTING"
            status_class = "ingesting"
        elif done:
            status_text = "COMPLETED"
            status_class = "completed"
        elif cursor > 0:
            status_text = "PAUSED"
            status_class = "paused"
        else:
            status_text = "READY"
            status_class = "ready"
            
        q_pct = min(100.0 * qlen / max(eng.max_queue, 1), 100.0)
        
        # Custom HTML Ingestion Monitor Card
        html_card = f"""
        <div class="ingestion-card">
          <div class="card-header">
             <h3 style="margin:0; font-family:'Outfit', sans-serif;">⚡ Ingestion Monitor</h3>
             <span class="status-badge {status_class}">{status_text}</span>
          </div>
          <div class="card-grid">
             <div class="metric-box">
               <span class="metric-label">Completeness</span>
               <span class="metric-val">{comp:.2f}%</span>
             </div>
             <div class="metric-box">
               <span class="metric-label">Watermark</span>
               <span class="metric-val">{wm_val}</span>
             </div>
             <div class="metric-box">
               <span class="metric-label">Late Dropped</span>
               <span class="metric-val text-red">{eng.metrics['late_dropped']:,}</span>
             </div>
             <div class="metric-box">
               <span class="metric-label">Duplicates</span>
               <span class="metric-val text-blue">{eng.metrics['duplicates']:,}</span>
             </div>
          </div>
          <div class="queue-monitor">
             <div class="queue-info">
               <span>Queue depth: <strong>{qlen:,} / {eng.max_queue:,}</strong></span>
               <span>{q_pct:.1f}% capacity</span>
             </div>
             <div class="queue-bar-container">
               <div class="queue-bar" style="width: {q_pct}%"></div>
             </div>
          </div>
        </div>
        """
        st.markdown(html_card, unsafe_allow_html=True)

        # Closed windows Plotly bar chart
        if eng.closed_windows:
            cw_keys = sorted(eng.closed_windows.keys())
            if src == "Synthetic":
                cw_x = [f"W {k - 1_700_000_000:.0f}s" for k in cw_keys]
                cw_y = [eng.closed_windows[k]["count"] for k in cw_keys]
                x_label = "Cửa sổ (event-time)"
            else:
                cw_x = [time.strftime("%H:%M:%S", time.localtime(k)) for k in cw_keys]
                cw_y = [eng.closed_windows[k]["count"] for k in cw_keys]
                x_label = "Thời gian cửa sổ"
                
            fig_cw = go.Figure()
            fig_cw.add_trace(go.Bar(
                x=cw_x, y=cw_y,
                marker_color="#3B82F6",
                hovertemplate="Cửa sổ: %{x}<br>Số sự kiện: %{y:,}<extra></extra>"
            ))
            fig_cw.update_layout(
                height=280, margin=dict(l=10, r=10, t=30, b=30),
                title=dict(text="📊 Phân phối sự kiện trong các cửa sổ ĐÃ CHỐT",
                           font=dict(size=12, color="#E2E8F0")),
                plot_bgcolor="#0F172A", paper_bgcolor="#0F172A",
                font=dict(color="#94A3B8"),
                xaxis=dict(title=x_label, gridcolor="#1E293B"),
                yaxis=dict(title="Events Count", gridcolor="#1E293B"),
                showlegend=False
            )
            st.plotly_chart(fig_cw, width="stretch", key=f"stream_cw_chart_{end_idx}")
        else:
            st.info("Chưa có cửa sổ nào đóng — watermark chưa vượt qua window đầu tiên.")

        st.progress(end_idx / max(total_rows, 1),
                    text=f"⏱️ Stream: {end_idx:,}/{total_rows:,}")

    # ── LIVE STREAM FRAGMENT ────────────────────────────────────────
    # @st.fragment(run_every=...): fragment tự rerun theo timer, chỉ rerun
    # vùng này → header / sidebar / tabs KHÔNG bị rerender (hết flicker).
    _stream_interval = 0.5 if st.session_state.get("stream_active") else None

    @st.fragment(run_every=_stream_interval)
    def _stream_live_fragment():
        if "stream_eng" not in st.session_state:
            return
        eng = st.session_state.stream_eng
        rows_st = st.session_state.get("stream_rows", [])
        src = st.session_state.get("stream_source_val", "Synthetic")
        n_total = len(rows_st)
        active = st.session_state.get("stream_active", False)

        if active:
            cur = st.session_state.stream_cursor
            spd = st.session_state.get("stream_speed_val", 200)
            chunk = max(int(spd * 0.5), 25)
            end_idx = min(cur + chunk, n_total)
            for i in range(cur, end_idx):
                r = rows_st[i]
                qlen = (i * 7) % 2500
                eng.process({"event_id": r.event_id, "event_time": r.event_time,
                             "status": r.status}, queue_len=qlen)
            last_q = ((end_idx - 1) * 7) % 2500 if end_idx > 0 else 0
            st.session_state.stream_cursor = end_idx
            _render_stream_ui(eng, src, end_idx, n_total, last_q)
            if end_idx >= n_total:
                eng.flush()
                st.session_state.stream_done_summary = eng.summary()
                st.session_state.stream_done = True
                st.session_state.stream_active = False
                st.rerun()                   # full rerun → khôi phục UI tĩnh
            # else: run_every tự rerun fragment cho chunk kế tiếp
        else:
            cur = st.session_state.get("stream_cursor", 0)
            last_q = ((cur - 1) * 7) % 2500 if cur > 0 else 0
            _render_stream_ui(eng, src, cur, n_total, last_q)
            if st.session_state.get("stream_done", False):
                s = st.session_state.get("stream_done_summary", {})
                st.success(
                    f"✅ Hoàn tất! Completeness={s.get('data_completeness_pct', 0)}% · "
                    f"Result-latency TB={s.get('avg_result_latency_ms', 0)} ms · "
                    f"Proc p99={s.get('proc_latency_p99_us', 0)} µs · "
                    f"Windows={s.get('windows_emitted', 0)}"
                )

    _stream_live_fragment()
