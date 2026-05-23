# -*- coding: utf-8 -*-
import os
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from wm import generate_logs, load_nasa_csv
from wm.sweep import run_once, write_report

def render_sweep():
    st.header("📊 Phân tích Đánh đổi (Wait Time vs Completeness)")
    st.markdown(
        "Quét nhiều mức **Wait Time (0ms → 8000ms)** để thấy quan hệ đánh đổi giữa "
        "**Completeness %** và **Result Latency**. Kết quả tự động lưu ra "
        "`tradeoff.csv` và `tradeoff.png`."
    )

    c1, c2 = st.columns(2)
    sweep_source = c1.selectbox("Nguồn dữ liệu", ["Synthetic", "NASA HTTP Real"], key="sweep_source")
    sweep_events = c2.number_input("Số sự kiện (sweep size)", min_value=1000, max_value=200000, value=20000, step=5000, key="sweep_events")

    run_sweep_btn = st.button("▶️ Chạy Sweep Analysis", type="primary", key="run_sweep_btn")

    if run_sweep_btn:
        st.markdown("### Đang chạy sweep…")
        progress_bar = st.progress(0.0)
        status_text = st.empty()

        status_text.text("Đang tải dữ liệu...")
        if sweep_source == "Synthetic":
            df = generate_logs(n_events=sweep_events)
        else:
            if not os.path.exists("dataset/data.csv"):
                st.error("Không tìm thấy file dataset/data.csv!")
                st.stop()
            df = load_nasa_csv("dataset/data.csv", limit=sweep_events)

        WAIT_TIMES_MS = [0, 100, 250, 500, 1000, 2000, 4000, 6000, 8000]
        rows = []

        # Realtime placeholders — chart + table cập nhật sau mỗi wait time
        live_chart = st.empty()
        live_table = st.empty()

        for idx, wt in enumerate(WAIT_TIMES_MS):
            status_text.text(f"⏱️ Đang chạy Wait Time = {wt} ms… ({idx+1}/{len(WAIT_TIMES_MS)})")
            rows.append(run_once(df, wt))
            progress_bar.progress((idx + 1) / len(WAIT_TIMES_MS))

            # ----- Vẽ curve TÍCH LUỸ realtime (Plotly, interactive) -----
            xs = [r["allowed_lateness_ms"] for r in rows]
            comp = [r["data_completeness_pct"] for r in rows]
            lat = [r["avg_result_latency_ms"] for r in rows]

            fig_live = go.Figure()
            fig_live.add_trace(go.Scatter(
                x=xs, y=comp, mode="lines+markers",
                name="Completeness %",
                line=dict(color="#3B82F6", width=3),
                marker=dict(size=10, color="#3B82F6",
                            line=dict(color="white", width=2)),
                hovertemplate="Wait %{x}ms<br>Completeness %{y:.2f}%<extra></extra>",
            ))
            if len(rows) > 1:
                fig_live.add_trace(go.Scatter(
                    x=xs, y=lat, mode="lines+markers",
                    name="Result latency (ms)",
                    line=dict(color="#EF4444", width=2, dash="dash"),
                    marker=dict(size=9, symbol="square", color="#EF4444",
                                line=dict(color="white", width=2)),
                    yaxis="y2",
                    hovertemplate="Wait %{x}ms<br>Latency %{y:.1f}ms<extra></extra>",
                ))
            fig_live.update_layout(
                height=340, margin=dict(l=10, r=10, t=40, b=30),
                title=dict(text=f"Quét tradeoff curve realtime · {idx+1}/{len(WAIT_TIMES_MS)} điểm",
                           font=dict(size=12, color="#E2E8F0")),
                plot_bgcolor="#0F172A", paper_bgcolor="#0F172A",
                font=dict(color="#94A3B8"),
                xaxis=dict(title="Wait Time (ms)", gridcolor="#1E293B"),
                yaxis=dict(title="Completeness %", color="#3B82F6",
                           gridcolor="#1E293B"),
                yaxis2=dict(title="Latency (ms)", color="#EF4444",
                            overlaying="y", side="right", showgrid=False),
                legend=dict(orientation="h", yanchor="bottom", y=1.05, x=0),
                hovermode="x unified",
            )
            live_chart.plotly_chart(fig_live, width="stretch",
                                    key=f"sweep_live_{idx}")

            # Bảng cập nhật
            df_live = pd.DataFrame(rows)[[
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
            })
            live_table.dataframe(df_live, width="stretch")

        status_text.text("Đang xuất báo cáo & vẽ biểu đồ cuối…")
        write_report(rows, csv_path="tradeoff.csv", png_path="tradeoff.png")
        df_res = pd.DataFrame(rows)

        st.markdown("#### Bảng số liệu thực nghiệm")
        st.dataframe(
            df_res[[
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

        st.markdown("#### Biểu đồ đánh đổi (Watermark Trade-off Curve)")
        xs = [r["allowed_lateness_ms"] for r in rows]
        comp = [r["data_completeness_pct"] for r in rows]
        lat = [r["avg_result_latency_ms"] for r in rows]

        fig_final = go.Figure()
        # Heuristic / Strict zones as background shapes
        fig_final.add_vrect(x0=0, x1=400, fillcolor="#F59E0B", opacity=0.10,
                            line_width=0, annotation_text="Heuristic zone",
                            annotation_position="top left",
                            annotation_font_size=10,
                            annotation_font_color="#FBBF24")
        if max(xs) > 3500:
            fig_final.add_vrect(x0=3500, x1=max(xs), fillcolor="#10B981",
                                opacity=0.10, line_width=0,
                                annotation_text="Strict zone",
                                annotation_position="top right",
                                annotation_font_size=10,
                                annotation_font_color="#34D399")
        fig_final.add_trace(go.Scatter(
            x=xs, y=comp, mode="lines+markers",
            name="Completeness %",
            line=dict(color="#3B82F6", width=3),
            marker=dict(size=11, color="#3B82F6",
                        line=dict(color="white", width=2)),
            hovertemplate="Wait %{x}ms<br>Completeness %{y:.2f}%<extra></extra>",
        ))
        fig_final.add_trace(go.Scatter(
            x=xs, y=lat, mode="lines+markers",
            name="Result latency (ms)",
            line=dict(color="#EF4444", width=2.5, dash="dash"),
            marker=dict(size=10, symbol="square", color="#EF4444",
                        line=dict(color="white", width=2)),
            yaxis="y2",
            hovertemplate="Wait %{x}ms<br>Latency %{y:.1f}ms<extra></extra>",
        ))
        fig_final.update_layout(
            height=420, margin=dict(l=10, r=10, t=50, b=40),
            title=dict(text="Watermark Trade-off: Completeness vs Wait Time",
                       font=dict(size=14, color="#E2E8F0")),
            plot_bgcolor="#0F172A", paper_bgcolor="#0F172A",
            font=dict(color="#94A3B8"),
            xaxis=dict(title="Wait Time / allowed_lateness (ms)",
                       gridcolor="#1E293B"),
            yaxis=dict(title="Completeness %", color="#3B82F6",
                       range=[min(comp) - 2, 101], gridcolor="#1E293B"),
            yaxis2=dict(title="Latency (ms)", color="#EF4444",
                        overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h", yanchor="bottom", y=1.05, x=0),
            hovermode="x unified",
        )
        st.plotly_chart(fig_final, width="stretch", key="sweep_final")
        st.success("Đã xuất `tradeoff.csv` và `tradeoff.png` thành công!", icon="✅")
