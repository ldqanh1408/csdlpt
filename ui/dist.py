# -*- coding: utf-8 -*-
import os
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from wm import generate_logs, load_nasa_csv
from wm.partition import run_cluster
from ui.helpers import generate_svg_cluster

def render_dist():
    st.header("🖥️ Mô phỏng Cluster Phân tán (Horizontal Partitioning)")
    st.markdown(
        "Phân mảnh ngang theo `node_id = hash(host) % N`. Mỗi node là một Watermark Engine "
        "độc lập (window, checkpoint riêng); kết quả gộp lại tại Coordinator."
    )

    c1, c2, c3 = st.columns(3)
    dist_nodes = c1.slider("Số Node", 2, 8, 4, key="dist_nodes")
    dist_source = c2.selectbox("Nguồn dữ liệu", ["Synthetic", "NASA HTTP Real"], key="dist_source")
    dist_events = c3.number_input("Số sự kiện", min_value=1000, max_value=200000, value=20000, step=5000, key="dist_events")

    run_dist_btn = st.button("▶️ Kích hoạt Cluster & Sweep", type="primary", key="run_dist_btn")

    if run_dist_btn:
        st.markdown("### Đang khởi chạy mô phỏng Cluster…")

        if dist_source == "NASA HTTP Real":
            if not os.path.exists("dataset/data.csv"):
                st.error("Không tìm thấy file dataset/data.csv!")
                st.stop()
            df = load_nasa_csv("dataset/data.csv", limit=dist_events)
        else:
            df = generate_logs(n_events=dist_events)
            df = df.assign(host=df["endpoint"])

        WAIT_TIMES_MS = [0, 250, 1000, 2000, 4000, 8000]
        results = []
        progress = st.progress(0.0)
        first_run_counts = None

        for idx, wt in enumerate(WAIT_TIMES_MS):
            r = run_cluster(df, wt, dist_nodes)
            results.append(r)
            if idx == 0:
                first_run_counts = r["per_node_counts"]
            progress.progress((idx + 1) / len(WAIT_TIMES_MS))

        df_dist = pd.DataFrame(results)

        st.markdown("#### Kết quả Sweep toàn Cluster")
        st.dataframe(
            df_dist[["allowed_lateness_ms", "completeness_pct", "late_dropped", "duplicates", "windows"]].rename(columns={
                "allowed_lateness_ms": "Wait (ms)",
                "completeness_pct": "Completeness % cluster",
                "late_dropped": "Late dropped (gộp)",
                "duplicates": "Duplicates (gộp)",
                "windows": "Windows đã chốt",
            }),
            width="stretch",
        )

        # Cluster Tradeoff Sweep Chart
        st.markdown("#### 📈 Biểu đồ Đánh đổi Cluster (Completeness & Late dropped vs Wait Time)")
        xs_dist = df_dist["allowed_lateness_ms"].tolist()
        comp_dist = df_dist["completeness_pct"].tolist()
        drop_dist = df_dist["late_dropped"].tolist()
        
        fig_dist_sweep = go.Figure()
        fig_dist_sweep.add_trace(go.Scatter(
            x=xs_dist, y=comp_dist, mode="lines+markers",
            name="Completeness %",
            line=dict(color="#10B981", width=3),
            marker=dict(size=10, color="#10B981", line=dict(color="white", width=2)),
            hovertemplate="Wait %{x}ms<br>Completeness %{y:.2f}%<extra></extra>",
        ))
        fig_dist_sweep.add_trace(go.Scatter(
            x=xs_dist, y=drop_dist, mode="lines+markers",
            name="Late dropped (gộp)",
            line=dict(color="#EF4444", width=2, dash="dash"),
            marker=dict(size=9, symbol="square", color="#EF4444", line=dict(color="white", width=2)),
            yaxis="y2",
            hovertemplate="Wait %{x}ms<br>Late dropped %{y:,}<extra></extra>",
        ))
        fig_dist_sweep.update_layout(
            height=320, margin=dict(l=10, r=10, t=40, b=30),
            title=dict(text="Cluster Trade-off: Completeness vs allowed_lateness",
                       font=dict(size=13, color="#E2E8F0")),
            plot_bgcolor="#0F172A", paper_bgcolor="#0F172A",
            font=dict(color="#94A3B8"),
            xaxis=dict(title="Wait Time / allowed_lateness (ms)", gridcolor="#1E293B"),
            yaxis=dict(title="Completeness %", color="#10B981", gridcolor="#1E293B"),
            yaxis2=dict(title="Late dropped (gộp)", color="#EF4444", overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h", yanchor="bottom", y=1.05, x=0),
            hovermode="x unified",
        )
        st.plotly_chart(fig_dist_sweep, width="stretch", key="dist_sweep_chart")

        if first_run_counts:
            mx, mn = max(first_run_counts), min(first_run_counts)
            total = sum(first_run_counts)
            skew_pct = 100.0 * (mx - mn) / max(total, 1)

            st.markdown("#### ⚖️ Phân phối tải & Hot-key Skew")
            st.warning(
                f"Skew = **{skew_pct:.2f}%** (chênh lệch Max−Min so với tổng). "
                f"`hash(host)%N` gom log của cùng host về một node ⇒ host hot tạo lệch tải."
            )

            node_labels = [f"Node {i}" for i in range(dist_nodes)]
            node_df = pd.Series(first_run_counts, index=node_labels)
            
            fig_bar = go.Figure()
            fig_bar.add_trace(go.Bar(
                x=node_df.index,
                y=node_df.values,
                marker_color=['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#EC4899', '#14B8A6', '#F97316'][:len(node_df)],
                hovertemplate="Node: %{x}<br>Processed Events: %{y:,}<extra></extra>"
            ))
            fig_bar.update_layout(
                height=320,
                margin=dict(l=10, r=10, t=30, b=30),
                plot_bgcolor="#0F172A",
                paper_bgcolor="#0F172A",
                font=dict(color="#94A3B8"),
                xaxis=dict(title="Cluster Node", gridcolor="#1E293B"),
                yaxis=dict(title="Processed Events Count", gridcolor="#1E293B"),
                showlegend=False
            )
            st.plotly_chart(fig_bar, width="stretch", key="dist_bar_chart")

            # Animated SVG cluster topology diagram
            st.markdown("#### 🗺️ Sơ đồ Cluster Topology")
            st.caption(
                "Mỗi node là một Watermark Engine độc lập. Dữ liệu được định tuyến tới các node "
                "bằng thuật toán băm `hash(host) % N`."
            )
            svg_content = generate_svg_cluster(
                statuses=["alive"] * dist_nodes,
                processed=first_run_counts,
                windows_closed=[0] * dist_nodes,
                dlq_counts=[0] * dist_nodes,
                ckpt_sizes=[0] * dist_nodes,
                highlight_node=None,
                tick=0,
            )
            st.components.v1.html(svg_content, height=430)
            st.success("Mô phỏng Cluster phân tán thành công!")
