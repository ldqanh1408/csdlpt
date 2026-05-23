# -*- coding: utf-8 -*-
import os
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from wm import generate_logs
from wm.sweep import run_once

def render_overview():
    st.subheader("🎯 Mục tiêu dự án")
    st.markdown(
        "Xây dựng hệ thống xử lý **stream log phân tán** với **event-time watermark** "
        "để xử lý log tới không đúng thứ tự (out-of-order). Bám sát rubric **Excellent** "
        "ở 4 trục: *Windowing Logic*, *State Management*, *Latency Analysis*, *Robustness*."
    )

    # ---------- Lộ trình demo ----------
    st.markdown("### Lộ trình demo đề xuất")
    s1, s2, s3, s4, s5 = st.columns(5)
    with s1:
        with st.container(border=True):
            st.caption("Bước 1")
            st.markdown("**Live Stream**")
            st.caption("Watermark bò lên, cửa sổ đóng theo event-time.")
    with s2:
        with st.container(border=True):
            st.caption("Bước 2")
            st.markdown("**Sweep Analysis**")
            st.caption("Quét Wait Time → đường cong Completeness ↔ Latency.")
    with s3:
        with st.container(border=True):
            st.caption("Bước 3")
            st.markdown("**Recovery**")
            st.caption("Crash giữa chừng + checkpoint atomic → Exactly-Once.")
    with s4:
        with st.container(border=True):
            st.caption("Bước 4")
            st.markdown("**Cluster phân tán**")
            st.caption("N node song song, đo hot-key skew.")
    with s5:
        with st.container(border=True):
            st.caption("Điểm nhấn")
            st.markdown("**Kill Node Live**")
            st.caption("Click diagram → kill / revive. DLQ trên đĩa.")

    # ---------- 4 chức năng chính ----------
    st.markdown("### Bốn chức năng chính")
    f1, f2, f3, f4 = st.columns(4)
    with f1:
        with st.container(border=True):
            st.markdown("**Live Stream**")
            st.caption("Phát luồng log theo thời gian thực. Quan sát watermark bò lên, "
                       "cửa sổ đóng dần, queue depth dao động.")
    with f2:
        with st.container(border=True):
            st.markdown("**Sweep Analysis**")
            st.caption("Quét nhiều mức Wait Time → đường cong đánh đổi "
                       "Completeness vs Latency, xuất tradeoff.csv/png.")
    with f3:
        with st.container(border=True):
            st.markdown("**Recovery & Backpressure**")
            st.caption("Crash giữa chừng + khôi phục từ checkpoint atomic, "
                       "burst tải kiểm chứng drop có kiểm soát.")
    with f4:
        with st.container(border=True):
            st.markdown("**Cluster phân tán**")
            st.caption("N node song song với `hash(host) % N`, đo hot-key skew "
                       "và gộp metric toàn cluster.")

    st.markdown("---")

    # ---------- Kiến trúc & Dataset ----------
    col_arch, col_data = st.columns([1.05, 1])

    with col_arch:
        st.markdown("### 🏗️ Kiến trúc hệ thống")
        st.code(
            "┌─────────────────────────────────────────────────┐\n"
            "│  Source: NASA-HTTP CSV  /  Synthetic generator  │\n"
            "│         (event_id, event_time, host, status)    │\n"
            "└──────────────────────┬──────────────────────────┘\n"
            "                       │ sort by arrival-time\n"
            "                       ▼\n"
            "┌─────────────────────────────────────────────────┐\n"
            "│  Partitioner:  node_id = hash(host) % N         │\n"
            "└──────┬──────────────┬──────────────┬────────────┘\n"
            "       ▼              ▼              ▼\n"
            "  ┌─────────┐    ┌─────────┐    ┌─────────┐\n"
            "  │ Node 0  │    │ Node 1  │    │ Node N-1│\n"
            "  │ Engine  │    │ Engine  │ …  │ Engine  │\n"
            "  │ ckpt    │    │ ckpt    │    │ ckpt    │\n"
            "  └────┬────┘    └────┬────┘    └────┬────┘\n"
            "       └────────┬─────┴────────┬─────┘\n"
            "                ▼              ▼\n"
            "        ┌────────────────────────────┐\n"
            "        │  Coordinator: merge metrics │\n"
            "        │  Completeness / Latency /   │\n"
            "        │  Late-drops / Skew          │\n"
            "        └────────────────────────────┘\n",
            language="text",
        )

    with col_data:
        st.markdown("### 📦 Dataset")
        if os.path.exists("dataset/data.csv"):
            try:
                preview = pd.read_csv("dataset/data.csv", nrows=8)
                size_mb = os.path.getsize("dataset/data.csv") / (1024 * 1024)

                d1, d2, d3 = st.columns(3)
                d1.metric("Kích thước", f"{size_mb:.1f} MB")
                d2.metric("Cột", f"{len(preview.columns)}")
                d3.metric("Nguồn", "NASA-HTTP")

                st.caption("Xem 8 dòng đầu của `dataset/data.csv`:")
                st.dataframe(preview, width="stretch", height=240)
            except Exception as e:
                st.error(f"Không đọc được dataset: {e}")
        else:
            st.warning("Chưa thấy `dataset/data.csv`. Có thể dùng nguồn **Synthetic** ở các tab khác.")
            st.caption("Synthetic generator sẽ tự sinh log có out-of-order + duplicate để test.")

    st.markdown("---")

    # ---------- Rubric mapping ----------
    st.markdown("### Bám rubric Excellent")
    r1, r2 = st.columns(2)
    with r1:
        with st.container(border=True):
            st.markdown("**1. Windowing Logic** — Event-time vs Processing-time")
            st.caption("`wm/engine.py::window_start_for()` gán cửa sổ theo event-time; "
                       "stream nạp theo arrival-time → watermark tự xử lý lệch.")
        with st.container(border=True):
            st.markdown("**2. State Management** — Checkpoint atomic + recovery")
            st.caption("`WatermarkEngine.checkpoint()` ghi `.tmp` rồi `os.replace`; "
                       "`restore()` + dedup ⇒ Exactly-Once sau crash.")
    with r2:
        with st.container(border=True):
            st.markdown("**3. Latency Analysis** — High-resolution timer")
            st.caption("`time.perf_counter_ns()` đo p50/p99 mỗi event; "
                       "phân biệt proc-latency vs result-latency → chỉ rõ bottleneck.")
        with st.container(border=True):
            st.markdown("**4. Robustness** — Backpressure + Dedup")
            st.caption("Dedup bằng `seen_ids`; queue có ngưỡng `max_queue`: "
                       "vượt → đếm `backpressure_drops` thay vì OOM crash.")

    st.markdown("---")

    # ---------- Quick demo preset ----------
    st.markdown("### Demo nhanh")
    st.caption(
        "Mini-sweep trên 10K event Synthetic ở 4 mức Wait Time (~5 giây). "
        "Kết quả hiển thị ngay tại đây."
    )
    quick_btn = st.button("Chạy Demo Nhanh", type="primary", key="quick_demo_btn")

    if quick_btn:
        with st.spinner("Đang chạy mini-sweep…"):
            df_q = generate_logs(n_events=10_000)
            quick_rows = []
            quick_progress = st.progress(0.0)
            quick_wait = [0, 500, 2000, 8000]
            for i, wt in enumerate(quick_wait):
                quick_rows.append(run_once(df_q, wt))
                quick_progress.progress((i + 1) / len(quick_wait))

        df_quick = pd.DataFrame(quick_rows)
        st.markdown("##### Bảng kết quả mini-sweep")
        st.dataframe(
            df_quick[[
                "allowed_lateness_ms", "data_completeness_pct",
                "late_dropped", "duplicates_filtered",
                "avg_result_latency_ms", "proc_latency_p99_us",
            ]].rename(columns={
                "allowed_lateness_ms": "Wait (ms)",
                "data_completeness_pct": "Completeness %",
                "late_dropped": "Late dropped",
                "duplicates_filtered": "Duplicates",
                "avg_result_latency_ms": "Result Latency (ms)",
                "proc_latency_p99_us": "Proc p99 (µs)",
            }),
            width="stretch",
        )

        fig_q = go.Figure()
        fig_q.add_trace(go.Scatter(
            x=[r["allowed_lateness_ms"] for r in quick_rows],
            y=[r["data_completeness_pct"] for r in quick_rows],
            mode="lines+markers",
            name="Độ đầy đủ (Completeness %)",
            line=dict(color="#3B82F6", width=3),
            marker=dict(size=9, color="#3B82F6", line=dict(color="white", width=1.5)),
            hovertemplate="Wait %{x}ms<br>Completeness %{y:.2f}%<extra></extra>"
        ))
        fig_q.add_trace(go.Scatter(
            x=[r["allowed_lateness_ms"] for r in quick_rows],
            y=[r["avg_result_latency_ms"] for r in quick_rows],
            mode="lines+markers",
            name="Độ trễ (Result Latency ms)",
            line=dict(color="#EF4444", width=2, dash="dash"),
            marker=dict(size=8, symbol="square", color="#EF4444", line=dict(color="white", width=1.5)),
            yaxis="y2",
            hovertemplate="Wait %{x}ms<br>Latency %{y:.1f}ms<extra></extra>"
        ))
        fig_q.update_layout(
            height=260,
            margin=dict(l=10, r=10, t=30, b=30),
            plot_bgcolor="#0F172A",
            paper_bgcolor="#0F172A",
            font=dict(color="#94A3B8"),
            xaxis=dict(title="Wait Time (ms)", gridcolor="#1E293B"),
            yaxis=dict(title="Completeness %", color="#3B82F6", gridcolor="#1E293B"),
            yaxis2=dict(title="Latency (ms)", color="#EF4444", overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h", yanchor="bottom", y=1.05, x=0),
            hovermode="x unified"
        )
        st.plotly_chart(fig_q, width="stretch", key="quick_demo_chart")
        st.success("✅ Demo nhanh hoàn tất! Chuyển sang các tab khác để chạy các kịch bản đầy đủ.")
