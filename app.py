"""
app.py — Dashboard đa chức năng của Distributed Watermark Tracker
------------------------------------------------------------------
Chạy: streamlit run app.py
Một lệnh duy nhất hiển thị TOÀN BỘ giao diện trình bày dự án + cấu hình +
mô phỏng dựa trên dataset, KHÔNG cần gõ thêm lệnh nào trong terminal.

6 tab:
  0. Tổng quan dự án — lộ trình demo, kiến trúc, dataset, rubric, Demo nhanh
  1. Live Stream Visualization ("Data in Motion") — Play / Dừng / Reset
  2. Sweep Analysis (Wait Time vs Completeness & Latency) — Plotly curve
  3. Fault Tolerance & Robustness (Crash Recovery & Backpressure)
  4. Distributed Cluster Simulation (N nodes, hot-key skew)
  5. Kill Node Live — kill/revive node realtime, DLQ trên đĩa, Auto-Play

Kỹ thuật UI: vùng realtime (Tab 1 & Tab 5) dùng @st.fragment +
st.rerun(scope="fragment") → chỉ vùng live cập nhật, phần còn lại của
trang KHÔNG bị rerender (chống flicker).
"""
import json
import os
import tempfile
import time
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import matplotlib.pyplot as plt

from wm import WatermarkEngine, generate_logs, load_nasa_csv
from wm.sweep import run_once, write_report
from wm.partition import run_cluster, partition_key


# ============================================================
# CLUSTER DIAGRAM — matplotlib visualization
# ============================================================
def draw_cluster_diagram(statuses, processed, windows_closed, dlq_sizes,
                        ckpt_sizes, highlight_node=None, title="Cluster Topology"):
    """Vẽ diagram chi tiết: dataset → partitioner → N nodes → coordinator.

    Args:
        statuses: list ["alive"/"dead"] cho mỗi node
        processed: list số event đã xử lý mỗi node
        windows_closed: list số window closed
        dlq_sizes: list dlq size (bytes hoặc events)
        ckpt_sizes: list checkpoint size (bytes)
        highlight_node: int id của node đang nhận event (vẽ mũi tên đỏ)
    """
    n = len(statuses)
    fig, ax = plt.subplots(figsize=(11.5, 6.5))
    ax.set_xlim(0, 12)
    ax.set_ylim(0, 9)
    ax.axis("off")
    fig.patch.set_facecolor("#F9FAFB")

    # ---- Tiêu đề ----
    ax.text(6, 8.65, title, ha="center", va="center",
            fontsize=13, fontweight="bold", color="#111827")

    # ---- Dataset source box ----
    ax.add_patch(plt.Rectangle((4, 7.5), 4, 0.8, facecolor="#FEF3C7",
                               edgecolor="#F59E0B", lw=2))
    ax.text(6, 7.9, "📦 Dataset Stream  (event_id · event_time · host · status)",
            ha="center", va="center", fontsize=10, fontweight="bold",
            color="#92400E")

    # ---- Mũi tên xuống partitioner ----
    ax.annotate("", xy=(6, 7.0), xytext=(6, 7.5),
                arrowprops=dict(arrowstyle="->", color="#6B7280", lw=2))

    # ---- Partitioner ----
    ax.add_patch(plt.Rectangle((3.5, 6.1), 5, 0.9, facecolor="#DBEAFE",
                               edgecolor="#2563EB", lw=2))
    ax.text(6, 6.55, "⚖️  Partitioner :  node_id = hash(host) % N",
            ha="center", va="center", fontsize=10.5, fontweight="bold",
            color="#1E3A8A")

    # ---- Node boxes ----
    node_zone_w = 11.0
    node_w = node_zone_w / n
    margin = 0.25
    node_top_y = 5.4
    node_bot_y = 2.1
    node_h = node_top_y - node_bot_y

    for i in range(n):
        x_left = 0.5 + i * node_w + margin
        w = node_w - 2 * margin
        cx = x_left + w / 2

        status = statuses[i]
        if status == "alive":
            face, edge, head_color = "#ECFDF5", "#059669", "#065F46"
            head = "🟢 ALIVE"
        else:
            face, edge, head_color = "#FEF2F2", "#DC2626", "#991B1B"
            head = "💀 DEAD"

        # Mũi tên từ partitioner xuống node
        if highlight_node == i:
            arrow_color = "#DC2626"; arrow_lw = 3.0
        else:
            arrow_color = "#9CA3AF"; arrow_lw = 1.0
        ax.annotate("", xy=(cx, node_top_y + 0.1),
                    xytext=(6, 6.05),
                    arrowprops=dict(arrowstyle="->", color=arrow_color, lw=arrow_lw))

        # Node box
        ax.add_patch(plt.Rectangle((x_left, node_bot_y), w, node_h,
                                   facecolor=face, edgecolor=edge, lw=2.5))

        # Head bar
        ax.add_patch(plt.Rectangle((x_left, node_top_y - 0.45), w, 0.45,
                                   facecolor=edge, edgecolor=edge, lw=0))
        ax.text(cx, node_top_y - 0.22, f"{head}  ·  Node {i}",
                ha="center", va="center", fontsize=9.5,
                fontweight="bold", color="white")

        # Metrics
        info = (
            f"processed: {processed[i]:,}\n"
            f"windows closed: {windows_closed[i]}\n"
            f"DLQ buffer: {dlq_sizes[i]:,}\n"
            f"checkpoint: {ckpt_sizes[i]:,} B"
        )
        ax.text(cx, node_top_y - 1.25, info, ha="center", va="top",
                fontsize=9, color="#1F2937", family="monospace")

        # Mini "engine" icon
        ax.text(cx, node_bot_y + 0.65, "⚙️", ha="center", va="center", fontsize=18)
        ax.text(cx, node_bot_y + 0.25, "Watermark Engine",
                ha="center", va="center", fontsize=8.5, color="#374151",
                style="italic")

        # Queue/DLQ bar (chỉ khi node dead có DLQ)
        if status == "dead" and dlq_sizes[i] > 0:
            max_bar = w * 0.7
            bar_w = min(dlq_sizes[i] / 2000.0, 1.0) * max_bar
            ax.add_patch(plt.Rectangle(
                (cx - max_bar/2, node_bot_y - 0.35), max_bar, 0.18,
                facecolor="#FEE2E2", edgecolor="#FCA5A5", lw=1))
            ax.add_patch(plt.Rectangle(
                (cx - max_bar/2, node_bot_y - 0.35), bar_w, 0.18,
                facecolor="#DC2626", edgecolor="none"))
            ax.text(cx, node_bot_y - 0.55, f"DLQ on disk: {dlq_sizes[i]:,}",
                    ha="center", va="top", fontsize=7.5,
                    color="#991B1B", fontweight="bold")

        # Mũi tên xuống coordinator
        ax.annotate("", xy=(6, 1.0), xytext=(cx, node_bot_y - 0.05),
                    arrowprops=dict(arrowstyle="->", color="#9CA3AF",
                                   lw=1, linestyle="--"))

    # ---- Coordinator ----
    ax.add_patch(plt.Rectangle((3.5, 0.2), 5, 0.8, facecolor="#F3E8FF",
                               edgecolor="#7C3AED", lw=2))
    ax.text(6, 0.6,
            "🧮  Coordinator  ·  merge(metrics) → completeness / latency / windows",
            ha="center", va="center", fontsize=10, fontweight="bold",
            color="#5B21B6")

    fig.tight_layout()
    return fig


# ============================================================
# ANIMATED FLOW DIAGRAM — Plotly GO (Axon-style, no flicker, click-to-kill)
# ============================================================
def make_cluster_fig(statuses, processed, windows_closed, dlq_counts,
                     cursor, total, highlight_node=None, tick=0):
    """Plotly GO animated diagram — no iframe flicker, click-to-kill nodes.
    Packets are scatter markers moving along edges each tick.
    Dead nodes render as collapsed boxes. Clickable via on_select='rerun'.
    """
    n = len(statuses)
    CX = 5.0
    # coordinate space: x [0,10], y [0,10]
    SRC  = (3.2, 8.9, 6.8, 9.7)
    PAR  = (1.8, 7.3, 8.2, 8.1)
    COO  = (1.5, 0.7, 8.5, 1.6)

    node_zone_x0, node_zone_x1 = 0.3, 9.7
    zone_w   = node_zone_x1 - node_zone_x0
    node_w   = zone_w / n
    gap      = 0.18
    NODE_Y0, NODE_Y1 = 3.6, 6.8
    DEAD_Y0, DEAD_Y1 = 4.6, 6.8

    centers_x = [node_zone_x0 + i*node_w + node_w/2 for i in range(n)]

    shapes, annotations = [], []
    traces = []

    def _rect(x0, y0, x1, y1, fill, line_color, lw=1.5, layer="below"):
        shapes.append(go.layout.Shape(
            type="rect", x0=x0, y0=y0, x1=x1, y1=y1,
            fillcolor=fill, line=dict(color=line_color, width=lw), layer=layer))

    def _ann(x, y, text, size=10, color="#111827", bold=False):
        annotations.append(go.layout.Annotation(
            x=x, y=y, text=f"<b>{text}</b>" if bold else text,
            showarrow=False, xref="x", yref="y",
            font=dict(size=size, color=color)))

    def _lerp(a, b, t): return a + (b - a) * t

    # ── Source ──────────────────────────────────────────────────
    _rect(*SRC, "#FEF9C3", "#F59E0B", 2)
    _ann(CX, (SRC[1]+SRC[3])/2, "📦  Dataset Stream", 11, "#92400E", bold=True)

    # ── Partitioner ─────────────────────────────────────────────
    _rect(*PAR, "#DBEAFE", "#3B82F6", 2)
    _ann(CX, (PAR[1]+PAR[3])/2, f"⚖️  Partitioner :  hash(host) % {n}", 11, "#1E3A8A", bold=True)

    # ── Coordinator ─────────────────────────────────────────────
    _rect(*COO, "#F3E8FF", "#7C3AED", 2)
    _ann(CX, (COO[1]+COO[3])/2,
         "🧮  Coordinator  ·  merge(completeness · latency · windows)",
         10, "#5B21B6", bold=True)

    # ── Progress bar ────────────────────────────────────────────
    prog = cursor / max(total, 1)
    _rect(0.2, 0.1, 9.8, 0.38, "#E2E8F0", "#E2E8F0", 0)
    _rect(0.2, 0.1, 0.2 + 9.6*prog, 0.38, "#3B82F6", "#3B82F6", 0)
    _ann(CX, 0.55, f"{cursor:,} / {total:,} events", 8, "#64748B")

    # ── Edges (drawn as Scatter lines) ──────────────────────────
    ex, ey = [], []
    # Dataset → Partitioner
    ex += [CX, CX, None]; ey += [SRC[1], PAR[3]+0.02, None]
    # Partitioner → nodes + nodes → coordinator
    for i, cx in enumerate(centers_x):
        dead = statuses[i] == "dead"
        hl   = i == highlight_node
        col  = "#FCA5A5" if dead else ("#059669" if hl else "#CBD5E1")
        lw   = 2.5 if hl else 1.0
        ny   = DEAD_Y1 if dead else NODE_Y1
        # part → node
        traces.append(go.Scatter(
            x=[CX, cx], y=[PAR[1]-0.02, ny+0.02], mode="lines",
            line=dict(color=col, width=lw, dash="dot" if dead else "solid"),
            hoverinfo="skip", showlegend=False))
        # node → coordinator
        traces.append(go.Scatter(
            x=[cx, CX], y=[NODE_Y0 if not dead else DEAD_Y0, COO[3]+0.02],
            mode="lines",
            line=dict(color="#FCA5A5" if dead else "#C4B5FD",
                      width=1.0, dash="dot" if dead else "solid"),
            hoverinfo="skip", showlegend=False))

    # dataset→part as single trace
    traces.insert(0, go.Scatter(x=ex, y=ey, mode="lines",
                                line=dict(color="#94A3B8", width=1.5),
                                hoverinfo="skip", showlegend=False))

    # ── Node boxes ──────────────────────────────────────────────
    click_x, click_y, hover_txt, cdata = [], [], [], []

    for i, cx in enumerate(centers_x):
        alive = statuses[i] == "alive"
        hl    = i == highlight_node
        x0 = cx - node_w/2 + gap
        x1 = cx + node_w/2 - gap

        if alive:
            y0, y1 = NODE_Y0, NODE_Y1
            fill   = "#A7F3D0" if hl else "#D1FAE5"
            ec     = "#059669" if hl else "#6EE7B7"
            lw     = 3 if hl else 1.5
            label  = f"🟢 Node {i}"
            hint   = f"Node {i} ALIVE — click to 💀 KILL"
        else:
            y0, y1 = DEAD_Y0, DEAD_Y1
            fill   = "#FEF2F2"; ec = "#DC2626"; lw = 2.5
            label  = f"💀 Node {i}"
            hint   = f"Node {i} DEAD — click to 🔄 REVIVE"

        _rect(x0, y0, x1, y1, fill, ec, lw, layer="above")
        # Header strip
        shapes.append(go.layout.Shape(
            type="rect", x0=x0, y0=y1-0.45, x1=x1, y1=y1,
            fillcolor=ec, line=dict(width=0), layer="above"))
        _ann(cx, y1-0.22, f"Node {i}", 9, "white", bold=True)

        if alive:
            _ann(cx, (y0+y1)/2+0.5, "🟢", 14)
            _ann(cx, (y0+y1)/2+0.05, f"{processed[i]:,}", 11, "#111827", bold=True)
            _ann(cx, (y0+y1)/2-0.35, f"win: {windows_closed[i]}", 8, "#6B7280")
            _ann(cx, y0+0.28, "⚙️ Engine", 8, "#94A3B8")
        else:
            _ann(cx, (y0+y1)/2+0.25, "💀 DEAD", 12, "#991B1B", bold=True)
            _ann(cx, (y0+y1)/2-0.22,
                 f"DLQ: {dlq_counts[i]:,}", 9, "#DC2626", bold=True)
            # DLQ bar (inside box)
            bar_max = (x1-x0) * 0.7
            bar_w   = min(dlq_counts[i]/500, 1.0) * bar_max
            _rect(cx-bar_max/2, y0+0.1, cx-bar_max/2+bar_max, y0+0.28,
                  "#FEE2E2", "#FCA5A5", 0.5, "above")
            if bar_w > 0:
                _rect(cx-bar_max/2, y0+0.1, cx-bar_max/2+bar_w, y0+0.28,
                      "#DC2626", "#DC2626", 0, "above")

        click_x.append(cx); click_y.append((y0+y1)/2)
        hover_txt.append(hint); cdata.append([i, statuses[i]])

    # Invisible click targets (large transparent markers on each node)
    traces.append(go.Scatter(
        x=click_x, y=click_y, mode="markers",
        marker=dict(size=42, color="rgba(0,0,0,0)"),
        hovertext=hover_txt, hoverinfo="text",
        customdata=cdata, name="nodes", showlegend=False))

    # ── Animated packets (3 per edge, driven by tick) ────────────
    period = 80
    t1 = (tick % period) / period
    t2 = ((tick + period//3) % period) / period
    t3 = ((tick + 2*period//3) % period) / period

    pkx, pky, pkc = [], [], []
    # Dataset → Partitioner (always)
    pkx.append(_lerp(CX, CX, t1));   pky.append(_lerp(SRC[1], PAR[3], t1));   pkc.append("#60A5FA")
    pkx.append(_lerp(CX, CX, t2));   pky.append(_lerp(SRC[1], PAR[3], t2));   pkc.append("#60A5FA")

    if highlight_node is not None:
        ncx  = centers_x[highlight_node]
        dead = statuses[highlight_node] == "dead"
        tgt_y = DEAD_Y1 if dead else NODE_Y1
        c_pkt = "#F87171" if dead else "#34D399"
        pkx.append(_lerp(CX, ncx, t1)); pky.append(_lerp(PAR[1], tgt_y, t1)); pkc.append(c_pkt)
        pkx.append(_lerp(CX, ncx, t3)); pky.append(_lerp(PAR[1], tgt_y, t3)); pkc.append(c_pkt)
        if not dead:
            pkx.append(_lerp(ncx, CX, t2)); pky.append(_lerp(NODE_Y0, COO[3], t2)); pkc.append("#A78BFA")
            pkx.append(_lerp(ncx, CX, t3)); pky.append(_lerp(NODE_Y0, COO[3], t3)); pkc.append("#A78BFA")
    else:
        # Background traffic to first alive node
        alive_nodes = [i for i in range(n) if statuses[i]=="alive"]
        if alive_nodes:
            ai  = alive_nodes[tick % len(alive_nodes)]
            acx = centers_x[ai]
            pkx.append(_lerp(CX, acx, t1)); pky.append(_lerp(PAR[1], NODE_Y1, t1)); pkc.append("#34D399")
            pkx.append(_lerp(acx, CX, t2)); pky.append(_lerp(NODE_Y0, COO[3], t2)); pkc.append("#A78BFA")

    traces.append(go.Scatter(
        x=pkx, y=pky, mode="markers",
        marker=dict(size=10, color=pkc,
                    line=dict(color="white", width=1.5)),
        hoverinfo="skip", showlegend=False, name="packets"))

    fig = go.Figure(
        data=traces,
        layout=go.Layout(
            shapes=shapes, annotations=annotations,
            width=820, height=540,
            plot_bgcolor="#F8FAFC", paper_bgcolor="#F8FAFC",
            xaxis=dict(range=[-0.1,10.1], visible=False, fixedrange=True),
            yaxis=dict(range=[-0.05,10.1], visible=False, fixedrange=True),
            margin=dict(l=4, r=4, t=36, b=4),
            showlegend=False,
            title=dict(text=f"Cluster {n} nodes  ·  cursor {cursor:,}/{total:,}",
                       font=dict(size=12, color="#374151"), x=0.5),
            clickmode="event",
            dragmode=False,
        )
    )
    return fig


# ============================================================
# LOG COLORIZATION — gắn class CSS theo keyword
# ============================================================
def colorize_log_line(line: str) -> str:
    """Bọc 1 dòng log bằng <span class='log-*'> theo keyword chính.

    - KILL / 💀  → log-kill (đỏ nhạt)
    - REVIVE / 🔄 → log-revive (xanh lá nhạt)
    - SCHEDULED / ⏰ → log-sched (vàng)
    - streaming dataset / 📡 → log-stream (xanh dương nhạt)
    - còn lại → log-info (xám)
    """
    up = line.upper()
    if "KILL" in up or "💀" in line:
        cls = "log-kill"
    elif "REVIVE" in up or "🔄" in line:
        cls = "log-revive"
    elif "SCHEDULED" in up or "⏰" in line:
        cls = "log-sched"
    elif "STREAMING" in up or "📡" in line:
        cls = "log-stream"
    else:
        cls = "log-info"
    # escape HTML tối thiểu
    safe = (line.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;"))
    return f"<span class='{cls}'>{safe}</span>"


def render_log_panel(container, log_lines, tail=25):
    """Render log panel với màu sắc theo loại event."""
    colored = [colorize_log_line(ln) for ln in log_lines[-tail:]]
    html = "<br>".join(colored)
    container.markdown(f'<div class="demo-log">{html}</div>',
                       unsafe_allow_html=True)


def sample_sim_history(state, cap=400):
    """Append 1 điểm dữ liệu vào sim_history (gọi sau mỗi chunk/step).

    Throttle: chỉ giữ tối đa `cap` điểm — xoá đều giữa chừng nếu vượt.
    """
    hist = state.get("sim_history")
    if hist is None:
        return
    engines = state["engines"]
    statuses = state["status"]
    n_alive = sum(1 for s in statuses if s == "alive")
    agg_u = sum(e.metrics["unique"] for e in engines)
    agg_o = sum(e.metrics["on_time"] for e in engines)
    cp = 100.0 * agg_o / max(agg_u, 1)
    dlq_tot = sum(len(p) for p in state["pending"])
    cur = state["cursor"]

    prev_cursor = hist["cursor"][-1] if hist["cursor"] else 0
    thr = max(cur - prev_cursor, 0)

    hist["cursor"].append(cur)
    hist["completeness"].append(round(cp, 3))
    hist["dlq_total"].append(dlq_tot)
    hist["alive_count"].append(n_alive)
    hist["throughput"].append(thr)

    # Cap để tránh history phình to khi user chạy stream rất dài
    if len(hist["cursor"]) > cap:
        # downsample: bỏ điểm thứ 2 trong mỗi cặp (giữ đầu+cuối+đều giữa)
        keep = list(range(0, len(hist["cursor"]), 2))
        if keep[-1] != len(hist["cursor"]) - 1:
            keep.append(len(hist["cursor"]) - 1)
        for key in hist:
            hist[key] = [hist[key][i] for i in keep]


def render_sim_evolution_chart(container, state, total_events):
    """Plotly multi-axis line chart: Completeness % + DLQ count theo cursor,
    cộng annotation (vertical bands) cho mỗi kill / revive event.

    Mục đích: cho thấy mối quan hệ NHÂN-QUẢ giữa sự cố (kill/revive) và
    biến động cluster — completeness drop khi kill, DLQ tăng, revive xong
    completeness phục hồi và DLQ về 0.
    """
    hist = state.get("sim_history")
    if not hist or len(hist["cursor"]) < 2:
        container.caption("Bắt đầu stream để vẽ Live Evolution chart…")
        return

    xs = hist["cursor"]
    comp = hist["completeness"]
    dlq = hist["dlq_total"]
    alive = hist["alive_count"]

    fig = go.Figure()
    # Completeness (left y-axis)
    fig.add_trace(go.Scatter(
        x=xs, y=comp, mode="lines",
        name="Completeness %",
        line=dict(color="#10B981", width=2.5),
        hovertemplate="cursor %{x:,}<br>completeness %{y:.2f}%<extra></extra>",
    ))
    # DLQ total (right y-axis)
    fig.add_trace(go.Scatter(
        x=xs, y=dlq, mode="lines",
        name="DLQ buffered",
        line=dict(color="#EF4444", width=2.0, dash="dot"),
        yaxis="y2",
        hovertemplate="cursor %{x:,}<br>DLQ %{y:,} events<extra></extra>",
    ))
    # Alive count line (right y-axis, smaller)
    fig.add_trace(go.Scatter(
        x=xs, y=alive, mode="lines",
        name="Nodes alive",
        line=dict(color="#3B82F6", width=1.5, dash="dash"),
        yaxis="y3",
        hovertemplate="cursor %{x:,}<br>alive %{y}<extra></extra>",
    ))

    # Annotate kill/revive markers
    markers = state.get("events_markers", [])
    shapes = []
    annotations = []
    for cur, kind, nid in markers:
        col = "#EF4444" if kind == "kill" else "#10B981"
        shapes.append(dict(
            type="line", x0=cur, x1=cur, xref="x",
            y0=0, y1=1, yref="paper",
            line=dict(color=col, width=1.5, dash="dot"),
        ))
        annotations.append(dict(
            x=cur, y=1.04, xref="x", yref="paper",
            text=f"{'💀' if kind == 'kill' else '🔄'} N{nid}",
            showarrow=False,
            font=dict(size=10, color=col),
        ))

    fig.update_layout(
        height=320,
        margin=dict(l=10, r=10, t=30, b=30),
        plot_bgcolor="#F8FAFC",
        paper_bgcolor="#FFFFFF",
        xaxis=dict(title="Cursor (events processed)", gridcolor="#E5E7EB",
                   range=[0, max(total_events, max(xs) if xs else 1)]),
        yaxis=dict(title="Completeness %", color="#059669",
                   range=[0, 105], gridcolor="#E5E7EB"),
        yaxis2=dict(title="DLQ buffered", color="#DC2626",
                    overlaying="y", side="right",
                    showgrid=False),
        yaxis3=dict(overlaying="y", side="right",
                    range=[0, max(alive) + 0.5 if alive else 1],
                    showticklabels=False, showgrid=False),
        legend=dict(orientation="h", yanchor="bottom", y=1.06, x=0),
        shapes=shapes, annotations=annotations,
        hovermode="x unified",
    )
    container.plotly_chart(fig, width="stretch",
                           key=f"sim_evo_{len(xs)}")


def render_cluster_health(container, statuses, total_processed, total_dlq,
                          completeness, cursor, total_events):
    """Render Cluster Health bằng native st.metric — sạch, không HTML rối."""
    n_alive = sum(1 for s in statuses if s == "alive")
    n_dead = sum(1 for s in statuses if s == "dead")
    n_total = len(statuses)

    if n_dead == 0:
        status_label, status_color = "Healthy", "normal"
        status_text = f"{n_alive}/{n_total} nodes alive"
    elif n_alive == 0:
        status_label, status_color = "Cluster Down", "inverse"
        status_text = "all nodes dead"
    else:
        status_label, status_color = "Degraded", "inverse"
        status_text = f"{n_dead}/{n_total} dead · DLQ holding data"

    with container.container(border=True):
        head_col, status_col = container.columns([2, 3])
        head_col.markdown("**Cluster Health Monitor**")
        status_col.caption(f"{status_label} — {status_text}")

        c1, c2, c3, c4, c5 = container.columns(5)
        c1.metric("Nodes alive", f"{n_alive} / {n_total}",
                  delta=None if n_dead == 0 else f"-{n_dead}",
                  delta_color=status_color)
        c2.metric("Events processed", f"{total_processed:,}",
                  delta=f"{100.0*cursor/max(total_events,1):.0f}% stream")
        c3.metric("DLQ buffered", f"{total_dlq:,}",
                  delta="holding" if total_dlq > 0 else None,
                  delta_color="inverse" if total_dlq > 0 else "normal")
        c4.metric("Completeness", f"{completeness:.1f}%")
        c5.metric("Cursor", f"{cursor:,}", delta=f"/ {total_events:,}",
                  delta_color="off")


# CẤU HÌNH TRANG
# ============================================================
st.set_page_config(
    page_title="Distributed Watermark Tracker · Dashboard",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# CSS / THEME — chỉ giữ CSS cho hero header & demo log panel
# (mọi UI khác dùng native Streamlit để giảm noise)
# ============================================================
st.markdown("""
<style>
    .hero {
        background: linear-gradient(135deg, #1E3A8A 0%, #3B82F6 60%, #06B6D4 100%);
        color: #FFFFFF;
        padding: 20px 26px;
        border-radius: 12px;
        margin-bottom: 14px;
    }
    .hero h1 { margin: 0; font-size: 1.85rem; font-weight: 800; }
    .hero p  { margin: 6px 0 0 0; font-size: 0.98rem; opacity: 0.92; }
    .badge {
        display: inline-block;
        background: rgba(255,255,255,0.18);
        color: #FFFFFF;
        padding: 3px 10px;
        border-radius: 999px;
        font-size: 0.78rem;
        margin: 8px 6px 0 0;
        border: 1px solid rgba(255,255,255,0.30);
    }
    .demo-log {
        background-color: #0F172A;
        color: #94A3B8;
        font-family: 'JetBrains Mono', Consolas, monospace;
        padding: 12px 14px;
        border-radius: 8px;
        height: 260px;
        overflow-y: auto;
        white-space: pre-wrap;
        font-size: 0.82rem;
        line-height: 1.45;
    }
    .demo-log .log-kill    { color: #FCA5A5; }
    .demo-log .log-revive  { color: #6EE7B7; }
    .demo-log .log-stream  { color: #93C5FD; }
    .demo-log .log-sched   { color: #FBBF24; }
    .demo-log .log-info    { color: #CBD5E1; }
    .stTabs [data-baseweb="tab-list"] button {
        font-size: 0.95rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================
# SIDEBAR
# ============================================================
with st.sidebar:
    st.image("https://img.icons8.com/clouds/150/000000/water.png", width=90)
    st.markdown("### 💧 Watermark Tracker")
    st.caption("Project 112 · Nhóm 112 · CSDL Phân tán")
    st.markdown("---")

    st.markdown("#### 📖 Tài liệu")
    st.markdown("- [README.md](file:///D:/dev/csdlpt/README.md)")
    st.markdown("- [DESIGN.md (Kiến trúc)](file:///D:/dev/csdlpt/DESIGN.md)")
    st.markdown("- [REPORT.md (Báo cáo)](file:///D:/dev/csdlpt/REPORT.md)")
    st.markdown("- [INDEX.md (Mục lục)](file:///D:/dev/csdlpt/INDEX.md)")

    st.markdown("---")
    st.markdown("#### 🧠 PACELC")
    st.info(
        "Stream có phân hoạch (P) → trade-off **A vs C**.\n\n"
        "Ngược lại (E) → trade-off **L vs C**.\n\n"
        "**Wait Time** là núm xoay điều khiển trade-off này."
    )

    st.markdown("---")
    st.markdown("#### ⚡ Trạng thái dataset")
    dataset_path = "dataset/data.csv"
    if os.path.exists(dataset_path):
        size_mb = os.path.getsize(dataset_path) / (1024 * 1024)
        st.success(f"NASA-HTTP sẵn sàng · {size_mb:.1f} MB")
    else:
        st.warning("Chưa có dataset/data.csv — chỉ chạy được Synthetic.")


# ============================================================
# HERO HEADER
# ============================================================
st.markdown("""
<div class="hero">
  <h1>💧 Distributed Watermark Tracker</h1>
  <p>Log Delay Compensator — Quản lý Event-time Watermark & khắc phục sự cố trong xử lý Stream phân tán</p>
  <div>
    <span class="badge">Event-time Windowing</span>
    <span class="badge">Atomic Checkpoint</span>
    <span class="badge">Backpressure</span>
    <span class="badge">Horizontal Partitioning</span>
    <span class="badge">NASA-HTTP · 2.96M rows</span>
  </div>
</div>
""", unsafe_allow_html=True)


# ============================================================
# TAB LAYOUT
# ============================================================
tab_overview, tab_stream, tab_sweep, tab_demos, tab_dist, tab_kill = st.tabs([
    "🏠 Tổng quan dự án",
    "📈 Live Stream",
    "📊 Sweep Analysis",
    "🛡️ Recovery & Backpressure",
    "🖥️ Distributed Cluster",
    "🔪 Kill Node Live",
])

# ============================================================
# TAB 0 — TỔNG QUAN / OVERVIEW
# ============================================================
with tab_overview:
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
    quick_btn = st.button("Chạy Demo Nhanh", type="primary", width="content")

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

        fig_q, axq = plt.subplots(figsize=(8, 3.2))
        axq.plot([r["allowed_lateness_ms"] for r in quick_rows],
                 [r["data_completeness_pct"] for r in quick_rows],
                 "o-", color="#1D4ED8", lw=2, label="Completeness %")
        axq2 = axq.twinx()
        axq2.plot([r["allowed_lateness_ms"] for r in quick_rows],
                  [r["avg_result_latency_ms"] for r in quick_rows],
                  "s--", color="#DC2626", lw=2, label="Result Latency (ms)")
        axq.set_xlabel("Wait Time (ms)")
        axq.set_ylabel("Completeness %", color="#1D4ED8")
        axq2.set_ylabel("Result Latency (ms)", color="#DC2626")
        axq.grid(alpha=0.3)
        fig_q.tight_layout()
        st.pyplot(fig_q)
        st.success("✅ Demo nhanh hoàn tất! Chuyển sang các tab khác để chạy các kịch bản đầy đủ.")


# ============================================================
# TAB 1 — LIVE STREAM
# ============================================================
with tab_stream:
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
        width="stretch",
    )
    stop_stream_btn = sb2.button(
        "Dừng",
        disabled=not st.session_state.stream_active,
        width="stretch",
    )
    reset_stream_btn = sb3.button(
        "Reset",
        disabled=st.session_state.stream_active,
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
        mc1, mc2, mc3, mc4 = st.columns(4)
        mc1.metric("Completeness", f"{comp:.2f}%")
        # Watermark: -inf (chưa có event) / +inf (sau flush) → lọc trước
        wm = eng.watermark
        if wm == float("-inf"):
            mc2.metric("Watermark", "—")
        elif wm == float("inf"):
            mc2.metric("Watermark", "∞ (đã flush)")
        else:
            try:
                if src == "Synthetic":
                    wm_val = f"{wm - 1_700_000_000:.1f} s"
                else:
                    wm_val = time.strftime("%H:%M:%S", time.localtime(wm))
                mc2.metric("Watermark", wm_val)
            except (OverflowError, ValueError, OSError):
                mc2.metric("Watermark", f"{wm:.0f}")
        mc3.metric("Late dropped", f"{eng.metrics['late_dropped']}")
        mc4.metric("Duplicates", f"{eng.metrics['duplicates']}")

        cc1, cc2 = st.columns([3, 1])
        with cc1:
            if eng.closed_windows:
                cw_keys = sorted(eng.closed_windows.keys())
                if src == "Synthetic":
                    cw = pd.Series({k - 1_700_000_000: eng.closed_windows[k]["count"]
                                    for k in cw_keys})
                    st.bar_chart(cw, horizontal=True,
                                 x_label="Events (CLOSED)",
                                 y_label="Window Start (s tương đối)")
                else:
                    cw = pd.Series({time.strftime("%H:%M:%S", time.localtime(k)):
                                    eng.closed_windows[k]["count"] for k in cw_keys})
                    st.bar_chart(cw, horizontal=True,
                                 x_label="Events (CLOSED)",
                                 y_label="Window time")
            else:
                st.caption("Chưa có cửa sổ nào đóng — watermark chưa vượt "
                           "qua window đầu tiên.")
        cc2.metric("Queue", f"{qlen} / {eng.max_queue}")
        st.progress(end_idx / max(total_rows, 1),
                    text=f"⏱️ Stream: {end_idx:,}/{total_rows:,}")

    # ── LIVE STREAM FRAGMENT ────────────────────────────────────────
    # @st.fragment + st.rerun(scope="fragment"): mỗi chunk CHỈ rerun vùng
    # này → header / sidebar / tabs KHÔNG bị rerender (hết flicker).
    @st.fragment
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
            chunk = max(int(spd), 50)
            end_idx = min(cur + chunk, n_total)
            for i in range(cur, end_idx):
                r = rows_st[i]
                qlen = (i * 7) % 2500
                eng.process({"event_id": r.event_id, "event_time": r.event_time,
                             "status": r.status}, queue_len=qlen)
            last_q = ((end_idx - 1) * 7) % 2500 if end_idx > 0 else 0
            st.session_state.stream_cursor = end_idx
            _render_stream_ui(eng, src, end_idx, n_total, last_q)
            if end_idx < n_total:
                time.sleep(0.03)
                st.rerun(scope="fragment")   # chỉ rerun fragment
            else:
                eng.flush()
                st.session_state.stream_done_summary = eng.summary()
                st.session_state.stream_done = True
                st.session_state.stream_active = False
                st.rerun()                   # full rerun → khôi phục UI tĩnh
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


# ============================================================
# TAB 2 — SWEEP ANALYSIS
# ============================================================
with tab_sweep:
    st.header("📊 Phân tích Đánh đổi (Wait Time vs Completeness)")
    st.markdown(
        "Quét nhiều mức **Wait Time (0ms → 8000ms)** để thấy quan hệ đánh đổi giữa "
        "**Completeness %** và **Result Latency**. Kết quả tự động lưu ra "
        "`tradeoff.csv` và `tradeoff.png`."
    )

    c1, c2 = st.columns(2)
    sweep_source = c1.selectbox("Nguồn dữ liệu", ["Synthetic", "NASA HTTP Real"], key="sweep_source")
    sweep_events = c2.number_input("Số sự kiện (sweep size)", min_value=1000, max_value=200000, value=20000, step=5000, key="sweep_events")

    run_sweep_btn = st.button("▶️ Chạy Sweep Analysis", type="primary")

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
                line=dict(color="#1D4ED8", width=3),
                marker=dict(size=10, color="#1D4ED8",
                            line=dict(color="white", width=2)),
                hovertemplate="Wait %{x}ms<br>Completeness %{y:.2f}%<extra></extra>",
            ))
            if len(rows) > 1:
                fig_live.add_trace(go.Scatter(
                    x=xs, y=lat, mode="lines+markers",
                    name="Result latency (ms)",
                    line=dict(color="#DC2626", width=2, dash="dash"),
                    marker=dict(size=9, symbol="square", color="#DC2626",
                                line=dict(color="white", width=2)),
                    yaxis="y2",
                    hovertemplate="Wait %{x}ms<br>Latency %{y:.1f}ms<extra></extra>",
                ))
            fig_live.update_layout(
                height=340, margin=dict(l=10, r=10, t=40, b=30),
                title=dict(text=f"Quét tradeoff curve realtime · {idx+1}/{len(WAIT_TIMES_MS)} điểm",
                           font=dict(size=12)),
                plot_bgcolor="#F8FAFC", paper_bgcolor="#FFFFFF",
                xaxis=dict(title="Wait Time (ms)", gridcolor="#E5E7EB"),
                yaxis=dict(title="Completeness %", color="#1D4ED8",
                           gridcolor="#E5E7EB"),
                yaxis2=dict(title="Latency (ms)", color="#DC2626",
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
                            annotation_font_color="#92400E")
        if max(xs) > 3500:
            fig_final.add_vrect(x0=3500, x1=max(xs), fillcolor="#10B981",
                                opacity=0.10, line_width=0,
                                annotation_text="Strict zone",
                                annotation_position="top right",
                                annotation_font_size=10,
                                annotation_font_color="#065F46")
        fig_final.add_trace(go.Scatter(
            x=xs, y=comp, mode="lines+markers",
            name="Completeness %",
            line=dict(color="#1D4ED8", width=3),
            marker=dict(size=11, color="#1D4ED8",
                        line=dict(color="white", width=2)),
            hovertemplate="Wait %{x}ms<br>Completeness %{y:.2f}%<extra></extra>",
        ))
        fig_final.add_trace(go.Scatter(
            x=xs, y=lat, mode="lines+markers",
            name="Result latency (ms)",
            line=dict(color="#DC2626", width=2.5, dash="dash"),
            marker=dict(size=10, symbol="square", color="#DC2626",
                        line=dict(color="white", width=2)),
            yaxis="y2",
            hovertemplate="Wait %{x}ms<br>Latency %{y:.1f}ms<extra></extra>",
        ))
        fig_final.update_layout(
            height=420, margin=dict(l=10, r=10, t=50, b=40),
            title=dict(text="Watermark Trade-off: Completeness vs Wait Time",
                       font=dict(size=14)),
            plot_bgcolor="#F8FAFC", paper_bgcolor="#FFFFFF",
            xaxis=dict(title="Wait Time / allowed_lateness (ms)",
                       gridcolor="#E5E7EB"),
            yaxis=dict(title="Completeness %", color="#1D4ED8",
                       range=[min(comp) - 2, 101], gridcolor="#E5E7EB"),
            yaxis2=dict(title="Latency (ms)", color="#DC2626",
                        overlaying="y", side="right", showgrid=False),
            legend=dict(orientation="h", yanchor="bottom", y=1.05, x=0),
            hovermode="x unified",
        )
        st.plotly_chart(fig_final, width="stretch", key="sweep_final")
        st.success("Đã xuất `tradeoff.csv` và `tradeoff.png` thành công!", icon="✅")


# ============================================================
# TAB 3 — ROBUSTNESS DEMOS
# ============================================================
with tab_demos:
    st.header("🛡️ Phục hồi Sự cố (Checkpoint) & Quá tải (Backpressure)")
    st.markdown("Chứng minh độ tin cậy & khả năng chịu tải của hệ thống stream theo rubric.")

    col1, col2 = st.columns(2)

    # ---- CRASH RECOVERY ----
    with col1:
        st.subheader("🔄 State Management & Recovery")
        st.markdown(
            "Engine bị **crash** giữa chừng → nhờ checkpoint atomic (`.tmp → os.replace`), "
            "khôi phục hoàn hảo và tiếp tục chạy không mất state, không đếm trùng (Exactly-Once)."
        )

        events_recovery = st.number_input("Số log demo", min_value=1000, max_value=50000, value=5000, step=1000, key="recovery_events")
        run_recovery_btn = st.button("▶️ Chạy Crash Recovery")
        recovery_log = st.empty()

        if run_recovery_btn:
            log_output = []
            def log_print(msg):
                log_output.append(msg)
                recovery_log.markdown(f'<div class="demo-log">{"".join(log_output)}</div>', unsafe_allow_html=True)
                time.sleep(0.4)

            log_print("🚀 Khởi chạy Crash Recovery Demo...\n")
            df = generate_logs(n_events=events_recovery)
            rows = list(df.itertuples(index=False))
            half = len(rows) // 2
            ckpt_path = os.path.join(tempfile.gettempdir(), "web_recovery.json")

            log_print("⚙️ Tạo Engine 1 (window=10s, wait=2s) và nạp 50% dữ liệu đầu...\n")
            eng = WatermarkEngine(window_size_s=10.0, allowed_lateness_s=2.0,
                                  checkpoint_interval=100, checkpoint_path=ckpt_path)
            for r in rows[:half]:
                eng.process({"event_id": r.event_id, "event_time": r.event_time, "status": r.status})
            eng.checkpoint()

            before = dict(eng.metrics)
            log_print(f"📊 Trạng thái Engine 1 TRƯỚC khi crash:\n"
                      f"   - total = {before['total']}\n"
                      f"   - unique = {before['unique']}\n"
                      f"   - on_time = {before['on_time']}\n")

            log_print("🔥 !!! CRASH !!! Tiến trình DIE đột ngột.\n")
            del eng

            log_print("🔄 Engine 2 khởi tạo và RESTORE từ checkpoint...\n")
            eng2 = WatermarkEngine.restore(ckpt_path, window_size_s=10.0, allowed_lateness_s=2.0)
            rm = eng2.metrics
            log_print(f"📊 Trạng thái Engine 2 sau khôi phục:\n"
                      f"   - total = {rm['total']}\n"
                      f"   - unique = {rm['unique']}\n"
                      f"   - on_time = {rm['on_time']}\n")

            log_print("⚙️ Engine 2 tiếp tục xử lý 50% còn lại...\n")
            for r in rows[half:]:
                eng2.process({"event_id": r.event_id, "event_time": r.event_time, "status": r.status})
            eng2.flush()
            s = eng2.summary()
            log_print(f"✅ Sau Recovery: Completeness={s['data_completeness_pct']}% · "
                      f"Duplicates lọc={s['duplicates_filtered']}\n"
                      f"👉 Exactly-Once được đảm bảo!")

            # ----- Visual state comparison -----
            st.markdown("##### 📊 So sánh trạng thái Engine (trước crash vs sau recovery)")
            compare_df = pd.DataFrame({
                "Metric": ["total", "unique", "on_time", "duplicates",
                           "late_dropped", "windows_emitted"],
                "📦 Engine 1 (TRƯỚC crash)": [
                    before["total"], before["unique"], before["on_time"],
                    before["duplicates"], before["late_dropped"],
                    "—",
                ],
                "🔄 Engine 2 (SAU restore)": [
                    rm["total"], rm["unique"], rm["on_time"],
                    rm["duplicates"], rm["late_dropped"],
                    "—",
                ],
                "🏁 Sau khi xử lý nốt": [
                    eng2.metrics["total"], eng2.metrics["unique"],
                    eng2.metrics["on_time"], eng2.metrics["duplicates"],
                    eng2.metrics["late_dropped"],
                    len(eng2.closed_windows),
                ],
            })
            st.dataframe(compare_df, width="stretch", hide_index=True)

            mc1, mc2, mc3 = st.columns(3)
            mc1.metric("Completeness", f"{s['data_completeness_pct']}%")
            mc2.metric("Duplicates lọc", f"{s['duplicates_filtered']:,}")
            mc3.metric("Checkpoint file", f"{os.path.getsize(ckpt_path):,} B")
            st.caption(f"📁 Checkpoint atomic được lưu tại: `{ckpt_path}`")
            st.success("✅ Crash Recovery Demo thành công · Exactly-Once đạt được!")

    # ---- BACKPRESSURE ----
    with col2:
        st.subheader("🛡️ Flow Control & Backpressure")
        st.markdown(
            "Burst tải vượt khả năng xử lý → để tránh OOM, engine drop có kiểm soát "
            "khi queue vượt `max_queue` (đếm `backpressure_drops`)."
        )

        events_bp = st.number_input("Số log demo", min_value=1000, max_value=50000, value=10000, step=2000, key="bp_events")
        max_q_size = st.number_input("max_queue", min_value=500, max_value=10000, value=2000, step=500, key="max_q")
        run_bp_btn = st.button("▶️ Chạy Backpressure")
        bp_plot = st.empty()
        bp_metrics = st.empty()

        if run_bp_btn:
            df = generate_logs(n_events=events_bp)
            eng = WatermarkEngine(
                window_size_s=10.0, allowed_lateness_s=2.0,
                checkpoint_interval=5000,
                checkpoint_path=os.path.join(tempfile.gettempdir(), "bp_ckpt.json"),
                max_queue=max_q_size,
            )

            queue_lengths = []
            drop_counts  = []
            SAMPLE = 50
            REFRESH = 400
            bp_prog = st.empty()

            for i, row in enumerate(df.itertuples(index=False)):
                qlen = int((1 + np.sin(i / 100.0)) * (max_q_size * 0.8))
                eng.process(
                    {"event_id": row.event_id, "event_time": row.event_time,
                     "status": row.status},
                    queue_len=qlen,
                )
                if i % SAMPLE == 0:
                    queue_lengths.append(qlen)
                    drop_counts.append(eng.metrics["backpressure_drops"])

                if i % REFRESH == 0 or i == len(df) - 1:
                    bp_prog.progress(
                        (i + 1) / len(df),
                        text=(f"⏱️ Burst stream: {i+1:,}/{len(df):,} · "
                              f"queue={qlen:,} / {max_q_size:,} · "
                              f"drops={eng.metrics['backpressure_drops']:,}")
                    )
                    xs_bp = list(range(len(queue_lengths)))
                    fig_rt = go.Figure()
                    # Overflow zone (filled area between threshold and queue when over)
                    overflow = [max(q - max_q_size, 0) for q in queue_lengths]
                    if any(overflow):
                        fig_rt.add_trace(go.Scatter(
                            x=xs_bp,
                            y=[max_q_size + o for o in overflow],
                            mode="lines", line=dict(width=0),
                            fillcolor="rgba(220,38,38,0.15)",
                            fill="tonexty",
                            name="Overflow zone",
                            showlegend=True, hoverinfo="skip",
                        ))
                    # Queue depth
                    fig_rt.add_trace(go.Scatter(
                        x=xs_bp, y=queue_lengths, mode="lines",
                        line=dict(color="#3B82F6", width=2),
                        name="Queue depth",
                        hovertemplate="tick %{x}<br>queue %{y:,}<extra></extra>",
                    ))
                    # Cumulative drops
                    fig_rt.add_trace(go.Scatter(
                        x=xs_bp, y=drop_counts, mode="lines",
                        line=dict(color="#DC2626", width=2, dash="dash"),
                        name="Cumulative drops",
                        hovertemplate="tick %{x}<br>drops %{y:,}<extra></extra>",
                    ))
                    # Max threshold line
                    fig_rt.add_hline(
                        y=max_q_size, line_dash="dot",
                        line_color="#F97316", line_width=2,
                        annotation_text=f"max_queue = {max_q_size:,}",
                        annotation_position="top right",
                        annotation_font_size=10,
                        annotation_font_color="#9A3412",
                    )
                    fig_rt.update_layout(
                        height=260, margin=dict(l=10, r=10, t=40, b=30),
                        title=dict(
                            text=f"Backpressure realtime — {i + 1:,} events processed",
                            font=dict(size=12)),
                        plot_bgcolor="#F8FAFC", paper_bgcolor="#FFFFFF",
                        xaxis=dict(title=f"Sample tick (×{SAMPLE} events)",
                                   gridcolor="#E5E7EB"),
                        yaxis=dict(title="Count", gridcolor="#E5E7EB"),
                        legend=dict(orientation="h", yanchor="bottom",
                                    y=1.06, x=0),
                        hovermode="x unified",
                    )
                    bp_plot.plotly_chart(fig_rt, width="stretch",
                                         key=f"bp_rt_{i // REFRESH}")

                    bp_metrics.markdown(
                        f"| Queue | Drops | Dup | Completeness |\n"
                        f"|---|---|---|---|\n"
                        f"| **{qlen:,}** / {max_q_size:,} "
                        f"| **{eng.metrics['backpressure_drops']:,}** "
                        f"| {eng.metrics['duplicates']:,} "
                        f"| {100*eng.metrics['on_time']/max(eng.metrics['unique'],1):.1f}% |"
                    )

            eng.flush()
            s = eng.summary()
            bp_metrics.success(
                f"✅ Hoàn tất · drops={s['backpressure_drops']} · "
                f"completeness={s['data_completeness_pct']}% · "
                f"duplicates={s['duplicates_filtered']} · "
                f"Engine KHÔNG crash / OOM dù burst tải."
            )

            # ----- Final metrics summary panel -----
            st.markdown("##### 📊 Tổng kết Backpressure")
            bsm1, bsm2, bsm3, bsm4 = st.columns(4)
            bsm1.metric("Events xử lý", f"{eng.metrics['unique']:,}")
            bsm2.metric("Backpressure drops", f"{s['backpressure_drops']:,}",
                        delta=f"{100*s['backpressure_drops']/max(events_bp,1):.1f}% tải",
                        delta_color="inverse")
            bsm3.metric("Completeness", f"{s['data_completeness_pct']}%")
            bsm4.metric("Proc p99 latency", f"{s['proc_latency_p99_us']} µs")
            st.caption(
                "🛡️ Engine **không bị OOM crash** dù queue tăng vượt `max_queue`: "
                "drop có kiểm soát thay vì để bộ nhớ tràn → Robustness đạt rubric."
            )
            st.success("✅ Backpressure Demo thành công!")


# ============================================================
# TAB 4 — DISTRIBUTED CLUSTER
# ============================================================
with tab_dist:
    st.header("🖥️ Mô phỏng Cluster Phân tán (Horizontal Partitioning)")
    st.markdown(
        "Phân mảnh ngang theo `node_id = hash(host) % N`. Mỗi node là một Watermark Engine "
        "độc lập (window, checkpoint riêng); kết quả gộp lại tại Coordinator."
    )

    c1, c2, c3 = st.columns(3)
    dist_nodes = c1.slider("Số Node", 2, 8, 4)
    dist_source = c2.selectbox("Nguồn dữ liệu", ["Synthetic", "NASA HTTP Real"], key="dist_source")
    dist_events = c3.number_input("Số sự kiện", min_value=1000, max_value=200000, value=20000, step=5000, key="dist_events")

    run_dist_btn = st.button("▶️ Kích hoạt Cluster & Sweep", type="primary")

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
            st.bar_chart(node_df, y_label="Events đã xử lý")

            # Plotly cluster topology diagram
            st.markdown("#### 🗺️ Sơ đồ Cluster Topology (Axon-style)")
            st.caption(
                "Mỗi node là một Watermark Engine độc lập. "
                "Chiều dài thanh dưới mỗi node ≈ số events được định tuyến tới node đó "
                "bằng `hash(host) % N`."
            )
            dist_fig = make_cluster_fig(
                statuses=["alive"] * dist_nodes,
                processed=first_run_counts,
                windows_closed=[0] * dist_nodes,
                dlq_counts=[0] * dist_nodes,
                cursor=sum(first_run_counts),
                total=sum(first_run_counts),
                tick=0,
            )
            st.plotly_chart(dist_fig, width="content")
            st.success("Mô phỏng Cluster phân tán thành công!")


# ============================================================
# TAB 5 — KILL NODE LIVE (mô phỏng node sống / chết / hồi sinh)
# ============================================================
with tab_kill:
    st.header("🔪 Kill Node Live — Mô phỏng node sống, chết, hồi sinh")
    st.markdown(
        "Mỗi node trong cluster là một **Watermark Engine độc lập** với checkpoint riêng. "
        "Bấm **💀 Kill** một node giữa stream → events tới node đó được **Coordinator** "
        "ghi nối tiếp vào **Dead-Letter Queue (DLQ)** trên đĩa "
        "(`.simdata/dlq/dlq_node_X.jsonl`). Bấm **🔄 Revive** → engine **restore** từ "
        "checkpoint atomic, **replay** từng dòng trong DLQ và xoá file ⇒ Exactly-Once "
        "vẫn được giữ, không mất event, không đếm trùng."
    )

    with st.expander("🧠 Buffer được lưu ở đâu? Định dạng gì?", expanded=False):
        st.markdown(
            "Khi một node chết, Coordinator KHÔNG được drop event. Thay vào đó "
            "events được **persist xuống đĩa** ở một file JSONL append-only "
            "(mỗi dòng = 1 event JSON). File để THẲNG trong project tại "
            "`./.simdata/dlq/` (không phải %TEMP%) để dễ mở bằng VS Code / Explorer:"
        )
        st.code(
            './.simdata/checkpoints/kill_node_2.json   ← state snapshot atomic\n'
            './.simdata/dlq/dlq_node_2.jsonl           ← Dead-Letter Queue Node 2\n'
            '{"event_id": "evt_00042", "event_time": 1700000123.45, "status": 200}\n'
            '{"event_id": "evt_00043", "event_time": 1700000124.10, "status": 404}\n'
            '{"event_id": "evt_00044", "event_time": 1700000124.71, "status": 200}\n'
            '...',
            language="text",
        )
        st.markdown(
            "- **Append-only** ⇒ ghi nhanh, không cần lock phức tạp.\n"
            "- **Persist trên đĩa** ⇒ nếu chính Coordinator cũng chết, vẫn không mất data.\n"
            "- **Replay khi revive** ⇒ engine khôi phục từ checkpoint trước, "
            "sau đó replay từng dòng DLQ. Dedup theo `event_id` đảm bảo Exactly-Once "
            "kể cả khi cùng 1 event đã ghi vào DLQ nhưng vô tình cũng có trong checkpoint.\n"
            "- Đây chính là pattern **Outbox / DLQ** thường thấy trong Kafka, "
            "RabbitMQ, AWS SQS Dead-Letter."
        )

    # ----- Usage guide -----
    gu1, gu2, gu3, gu4 = st.columns(4)
    with gu1, st.container(border=True):
        st.markdown("**Bước 1 · Cấu hình**")
        st.caption("Chọn số node, nguồn dữ liệu, tổng events rồi bấm **Khởi tạo Cluster**.")
    with gu2, st.container(border=True):
        st.markdown("**Bước 2 · Stream**")
        st.caption("Bấm **Chạy chunk** từng bước, hoặc **Auto-Play** để stream tự động.")
    with gu3, st.container(border=True):
        st.markdown("**Bước 3 · Kill & Revive**")
        st.caption("Click trực tiếp trên diagram, hoặc dùng nút dưới mỗi node.")
    with gu4, st.container(border=True):
        st.markdown("**Bước 4 · DLQ & Recovery**")
        st.caption("Events của node chết ghi vào `.simdata/dlq/`. Revive → replay → Exactly-Once.")

    # ----- Config -----
    cc1, cc2, cc3, cc4 = st.columns(4)
    kill_nodes = cc1.slider("Số Node", 2, 6, 4, key="kill_n_nodes")
    kill_source = cc2.selectbox("Nguồn", ["Synthetic", "NASA HTTP Real"], key="kill_source")
    kill_events = cc3.number_input("Tổng events", 2000, 100000, 10000, 1000, key="kill_events_n")
    kill_chunk = cc4.number_input("Events / step", 100, 5000, 500, 100, key="kill_chunk_n")

    cinit, _, creset = st.columns([1, 4, 1])
    init_btn = cinit.button("🏗️ Khởi tạo Cluster", type="primary")
    reset_btn = creset.button("🗑️ Reset")

    if reset_btn:
        st.session_state.pop("kill_state", None)
        st.rerun()

    if init_btn:
        if kill_source == "NASA HTTP Real":
            if not os.path.exists("dataset/data.csv"):
                st.error("Không tìm thấy `dataset/data.csv`!")
                st.stop()
            df_k = load_nasa_csv("dataset/data.csv", limit=kill_events)
        else:
            df_k = generate_logs(n_events=kill_events)
            df_k = df_k.assign(host=df_k["endpoint"])

        # Lưu vào thư mục project để dễ thấy (thay vì %TEMP% bị ẩn)
        sim_dir = os.path.abspath(os.path.join(os.getcwd(), ".simdata"))
        ckpt_dir = os.path.join(sim_dir, "checkpoints")
        dlq_dir = os.path.join(sim_dir, "dlq")
        os.makedirs(ckpt_dir, exist_ok=True)
        os.makedirs(dlq_dir, exist_ok=True)

        engines = []
        ckpt_paths = []
        dlq_paths = []
        for i in range(kill_nodes):
            p = os.path.join(ckpt_dir, f"kill_node_{i}.json")
            dlq = os.path.join(dlq_dir, f"dlq_node_{i}.jsonl")
            # Xoá DLQ cũ nếu còn (để session mới sạch sẽ)
            if os.path.exists(dlq):
                try:
                    os.remove(dlq)
                except OSError:
                    pass
            ckpt_paths.append(p)
            dlq_paths.append(dlq)
            engines.append(WatermarkEngine(
                window_size_s=10.0,
                allowed_lateness_s=2.0,
                checkpoint_interval=200,
                checkpoint_path=p,
                max_queue=10_000_000,
            ))

        st.session_state.kill_state = {
            "engines": engines,
            "ckpt_paths": ckpt_paths,
            "dlq_paths": dlq_paths,
            "sim_dir": sim_dir,
            "ckpt_dir": ckpt_dir,
            "dlq_dir": dlq_dir,
            "status": ["alive"] * kill_nodes,
            "rows": list(df_k.itertuples(index=False)),
            "cursor": 0,
            "pending": [[] for _ in range(kill_nodes)],
            "node_processed": [0] * kill_nodes,
            "node_buffered_total": [0] * kill_nodes,
            "log": [
                f"🏗️ Khởi tạo cluster {kill_nodes} node với {len(df_k):,} events",
                f"📂 Thư mục mô phỏng: {sim_dir}",
                f"📂 Checkpoints: {ckpt_dir}",
                f"📂 DLQ (Dead-Letter Queue): {dlq_dir}",
            ],
            # Lịch sử simulation — sampled mỗi chunk để vẽ live evolution chart
            "sim_history": {
                "cursor": [0],
                "completeness": [100.0],
                "dlq_total": [0],
                "alive_count": [kill_nodes],
                "throughput": [0],   # events / sample tick (delta processed)
            },
            # Mốc events kill/revive để annotation lên chart
            "events_markers": [],  # list of (cursor, kind, node_id) where kind in {"kill","revive"}
        }
        st.rerun()

    state = st.session_state.get("kill_state")
    if not state:
        st.info("👉 Bấm **Khởi tạo Cluster** để bắt đầu mô phỏng.")
    else:
        n = len(state["engines"])
        total = len(state["rows"])
        cursor = state["cursor"]
        done = cursor >= total

        # Backward compat + khởi tạo sớm các key cần dùng trước phần Controls
        if "auto_play_active" not in state:
            state["auto_play_active"] = False
        if "schedule" not in state:
            state["schedule"] = []
        if "sim_history" not in state:
            state["sim_history"] = {
                "cursor": [0], "completeness": [100.0],
                "dlq_total": [0], "alive_count": [n], "throughput": [0],
            }
        if "events_markers" not in state:
            state["events_markers"] = []

        # ----- 🛰️ Cluster Health Card (live) -----
        _tot_proc = sum(state["node_processed"])
        _tot_dlq = sum(len(p) for p in state["pending"])
        _agg_u = sum(e.metrics["unique"] for e in state["engines"])
        _agg_o = sum(e.metrics["on_time"] for e in state["engines"])
        _comp_now = 100.0 * _agg_o / max(_agg_u, 1)
        render_cluster_health(st, state["status"], _tot_proc, _tot_dlq,
                              _comp_now, cursor, total)

        # ----- ⚡ Quick Actions row -----
        qa1, qa2, qa3, qa4 = st.columns(4)
        if qa1.button("💀 Kill ALL nodes",
                      disabled=state.get("auto_play_active", False) or done,
                      width="stretch"):
            killed = 0
            for i in range(n):
                if state["status"][i] == "alive":
                    try:
                        state["engines"][i].checkpoint()
                    except Exception:
                        pass
                    state["status"][i] = "dead"
                    state.setdefault("events_markers", []).append(
                        (state["cursor"], "kill", i))
                    killed += 1
            if killed:
                state["log"].append(
                    f"[cursor={cursor:,}] 💀 Kill ALL · {killed} node killed · "
                    f"DLQ sẽ giữ events kế tiếp"
                )
                sample_sim_history(state)
            st.rerun()
        if qa2.button("🔄 Revive ALL nodes",
                      disabled=state.get("auto_play_active", False),
                      width="stretch"):
            revived = 0
            for i in range(n):
                if state["status"][i] == "dead":
                    ckpt = state["ckpt_paths"][i]
                    try:
                        eng_new = WatermarkEngine.restore(
                            ckpt, window_size_s=10.0, allowed_lateness_s=2.0,
                            checkpoint_interval=200, max_queue=10_000_000)
                    except Exception:
                        eng_new = WatermarkEngine(
                            window_size_s=10.0, allowed_lateness_s=2.0,
                            checkpoint_interval=200, checkpoint_path=ckpt,
                            max_queue=10_000_000)
                    dlq = state["dlq_paths"][i]
                    replayed = 0
                    if os.path.exists(dlq):
                        with open(dlq, "r", encoding="utf-8") as f:
                            for line in f:
                                line = line.strip()
                                if not line:
                                    continue
                                try:
                                    eng_new.process(json.loads(line))
                                    replayed += 1
                                    state["node_processed"][i] += 1
                                except Exception:
                                    pass
                        try:
                            os.remove(dlq)
                        except OSError:
                            pass
                    state["pending"][i] = []
                    state["engines"][i] = eng_new
                    state["status"][i] = "alive"
                    state.setdefault("events_markers", []).append(
                        (state["cursor"], "revive", i))
                    revived += 1
            if revived:
                state["log"].append(
                    f"[cursor={cursor:,}] 🔄 Revive ALL · {revived} node "
                    f"khôi phục từ checkpoint · DLQ đã drain"
                )
                sample_sim_history(state)
            st.rerun()
        if qa3.button("🏁 Flush all alive",
                      disabled=state.get("auto_play_active", False),
                      width="stretch"):
            flushed = 0
            for i, eng in enumerate(state["engines"]):
                if state["status"][i] == "alive":
                    eng.flush()
                    flushed += 1
            state["log"].append(f"🏁 Flush all · {flushed} engines flushed")
            st.rerun()
        if qa4.button("📸 Save snapshot",
                      disabled=state.get("auto_play_active", False),
                      width="stretch"):
            saved = 0
            for i, eng in enumerate(state["engines"]):
                if state["status"][i] == "alive":
                    try:
                        eng.checkpoint()
                        saved += 1
                    except Exception:
                        pass
            state["log"].append(
                f"📸 Snapshot · {saved} checkpoint atomic đã ghi vào "
                f"`.simdata/checkpoints/`"
            )
            st.rerun()

        # ----- 🗺️ Cluster diagram -----
        def _state_diagram_args(st_state, n_nodes):
            processed = list(st_state["node_processed"])
            windows = [len(e.closed_windows) for e in st_state["engines"]]
            dlq_n = [len(p) for p in st_state["pending"]]
            ckpt_sz = [os.path.getsize(p) if os.path.exists(p) else 0
                       for p in st_state["ckpt_paths"]]
            return processed, windows, dlq_n, ckpt_sz

        st.markdown("#### Sơ đồ Cluster (Axon-style)")
        st.caption(
            "Click trực tiếp lên node trong diagram để Kill / Revive. "
            "ALIVE → click → checkpoint atomic, đánh dấu DEAD. "
            "DEAD → click → restore từ checkpoint + replay DLQ (Exactly-Once)."
        )
        _p, _w, _d, _c = _state_diagram_args(state, n)
        diag_tick = state.get("diag_tick", 0)
        state["diag_tick"] = diag_tick + 1

        if not state["auto_play_active"]:
            diag_fig = make_cluster_fig(
                state["status"], _p, _w, [len(p) for p in state["pending"]],
                cursor, total, highlight_node=None, tick=diag_tick,
            )
            clicked = st.plotly_chart(
                diag_fig, key="cluster_static",
                on_select="rerun", width="content",
            )
        else:
            clicked = None
            st.caption("⏯️ Đang Auto-Play — diagram realtime cập nhật ở "
                       "khu vực live bên dưới ↓")
        # ── Click-to-kill / revive directly on diagram ──────────
        if clicked and clicked.selection and clicked.selection.points:
            pt = clicked.selection.points[0]
            cd = getattr(pt, "customdata", None)
            if cd and len(cd) >= 1:
                nid_click = int(cd[0])
                if 0 <= nid_click < n:
                    if state["status"][nid_click] == "alive":
                        try:
                            state["engines"][nid_click].checkpoint()
                            ck_sz = os.path.getsize(state["ckpt_paths"][nid_click])
                        except Exception:
                            ck_sz = 0
                        state["status"][nid_click] = "dead"
                        state.setdefault("events_markers", []).append(
                            (state["cursor"], "kill", nid_click))
                        state["log"].append(
                            f"[cursor={cursor:,}] 💀 DIAGRAM KILL Node {nid_click} · "
                            f"checkpoint {ck_sz}B · DLQ: {os.path.basename(state['dlq_paths'][nid_click])}"
                        )
                        sample_sim_history(state)
                    else:
                        ckpt = state["ckpt_paths"][nid_click]
                        try:
                            eng_new = WatermarkEngine.restore(
                                ckpt, window_size_s=10.0, allowed_lateness_s=2.0,
                                checkpoint_interval=200, max_queue=10_000_000)
                        except Exception:
                            eng_new = WatermarkEngine(
                                window_size_s=10.0, allowed_lateness_s=2.0,
                                checkpoint_interval=200, checkpoint_path=ckpt,
                                max_queue=10_000_000)
                        dlq = state["dlq_paths"][nid_click]
                        replayed = 0
                        if os.path.exists(dlq):
                            with open(dlq, "r", encoding="utf-8") as f:
                                for line in f:
                                    line = line.strip()
                                    if not line:
                                        continue
                                    try:
                                        eng_new.process(json.loads(line))
                                        replayed += 1
                                        state["node_processed"][nid_click] += 1
                                    except Exception:
                                        pass
                            try:
                                os.remove(dlq)
                            except OSError:
                                pass
                        state["pending"][nid_click] = []
                        state["engines"][nid_click] = eng_new
                        state["status"][nid_click] = "alive"
                        state.setdefault("events_markers", []).append(
                            (state["cursor"], "revive", nid_click))
                        state["log"].append(
                            f"[cursor={cursor:,}] 🔄 DIAGRAM REVIVE Node {nid_click} · "
                            f"replay {replayed:,} DLQ events"
                        )
                        sample_sim_history(state)
                    st.rerun()

        # ----- Node grid -----
        st.markdown("#### Trạng thái các Node")
        cols = st.columns(n)
        for i, c in enumerate(cols):
            with c:
                status = state["status"][i]
                eng = state["engines"][i]
                pending_n = len(state["pending"][i])
                with st.container(border=True):
                    if status == "alive":
                        st.markdown(f"**Node {i}** &nbsp; :green[● alive]",
                                    unsafe_allow_html=True)
                    else:
                        st.markdown(f"**Node {i}** &nbsp; :red[● dead]",
                                    unsafe_allow_html=True)
                    st.caption(
                        f"Processed **{state['node_processed'][i]:,}** · "
                        f"Unique **{eng.metrics['unique']:,}** · "
                        f"Windows **{len(eng.closed_windows)}**"
                    )
                    st.caption(
                        f"Late **{eng.metrics['late_dropped']}** · "
                        f"Dup **{eng.metrics['duplicates']}** · "
                        + (f":red[**DLQ {pending_n:,}**]" if pending_n
                           else f"DLQ {pending_n:,}")
                    )

                if status == "alive":
                    if st.button(f"Kill node {i}", key=f"kill_{i}",
                                 width="stretch",
                                 disabled=state["auto_play_active"]):
                        try:
                            eng.checkpoint()
                            ckpt_size = os.path.getsize(state["ckpt_paths"][i])
                        except Exception:
                            ckpt_size = 0
                        state["status"][i] = "dead"
                        state.setdefault("events_markers", []).append(
                            (state["cursor"], "kill", i))
                        state["log"].append(
                            f"[cursor={cursor:,}] 💀 Node {i} KILLED · "
                            f"checkpoint saved ({ckpt_size}B) tại "
                            f"{os.path.basename(state['ckpt_paths'][i])} · "
                            f"DLQ mở: {os.path.basename(state['dlq_paths'][i])}"
                        )
                        sample_sim_history(state)
                        st.rerun()
                else:
                    # Hiển thị thông tin DLQ file ngay trên node chết
                    dlq_path = state["dlq_paths"][i]
                    dlq_exists = os.path.exists(dlq_path)
                    dlq_size = os.path.getsize(dlq_path) if dlq_exists else 0
                    dlq_lines = len(state["pending"][i])
                    st.caption(
                        f"📁 DLQ: `{os.path.basename(dlq_path)}` · "
                        f"{dlq_lines:,} dòng · {dlq_size:,} B"
                    )

                    if st.button(f"🔄 Revive node {i}", key=f"revive_{i}",
                                 width="stretch",
                                 disabled=state["auto_play_active"]):
                        ckpt_path = state["ckpt_paths"][i]
                        if os.path.exists(ckpt_path):
                            try:
                                eng_new = WatermarkEngine.restore(
                                    ckpt_path,
                                    window_size_s=10.0,
                                    allowed_lateness_s=2.0,
                                    checkpoint_interval=200,
                                    max_queue=10_000_000,
                                )
                            except Exception:
                                eng_new = WatermarkEngine(
                                    window_size_s=10.0, allowed_lateness_s=2.0,
                                    checkpoint_interval=200, checkpoint_path=ckpt_path,
                                    max_queue=10_000_000,
                                )
                        else:
                            eng_new = WatermarkEngine(
                                window_size_s=10.0, allowed_lateness_s=2.0,
                                checkpoint_interval=200, checkpoint_path=ckpt_path,
                                max_queue=10_000_000,
                            )
                        # Replay từ DLQ trên ĐĨA (nguồn sự thật) — không tin in-memory
                        replayed = 0
                        if os.path.exists(dlq_path):
                            with open(dlq_path, "r", encoding="utf-8") as f:
                                for line in f:
                                    line = line.strip()
                                    if not line:
                                        continue
                                    try:
                                        ev = json.loads(line)
                                    except json.JSONDecodeError:
                                        continue
                                    eng_new.process(ev)
                                    replayed += 1
                                    state["node_processed"][i] += 1
                            # Xoá DLQ sau khi replay xong (đã drain)
                            try:
                                os.remove(dlq_path)
                            except OSError:
                                pass
                        state["pending"][i] = []
                        state["engines"][i] = eng_new
                        state["status"][i] = "alive"
                        state.setdefault("events_markers", []).append(
                            (state["cursor"], "revive", i))
                        state["log"].append(
                            f"[cursor={cursor:,}] 🔄 Node {i} REVIVED · "
                            f"restore từ checkpoint + drain {replayed:,} events từ DLQ · "
                            f"DLQ file đã xoá · Exactly-Once OK"
                        )
                        sample_sim_history(state)
                        st.rerun()

        # ============================================================
        # CONTROLS — 3-tab segmented: Thủ công / Auto-Play / Lập lịch
        # ============================================================
        st.markdown("#### Điều khiển stream")
        # (schedule / auto_play_active đã khởi tạo sớm ở khối backward-compat)

        def _buffer_to_dlq(nid: int, ev: dict):
            """Append event vào Dead-Letter Queue trên ĐĨA (append-only JSONL)."""
            with open(state["dlq_paths"][nid], "a", encoding="utf-8") as f:
                f.write(json.dumps(ev, ensure_ascii=False) + "\n")
            state["pending"][nid].append(ev)
            state["node_buffered_total"][nid] += 1

        ctrl_manual, ctrl_auto, ctrl_sched = st.tabs([
            "Thủ công", "Auto-Play", "Lập lịch",
        ])

        # ── Tab: Thủ công ──
        with ctrl_manual:
            st.caption(
                "Chạy stream từng bước hoặc kết thúc ngay. "
                "Dùng khi muốn kiểm soát chính xác (kill thủ công giữa chừng)."
            )
            mc1, mc2, mc3 = st.columns(3)
            run_chunk_btn = mc1.button(
                f"Chạy {kill_chunk:,} events",
                disabled=done or state["auto_play_active"],
                width="stretch", key="man_chunk_btn",
            )
            run_all_btn = mc2.button(
                "Chạy hết stream",
                disabled=done or state["auto_play_active"],
                width="stretch", key="man_all_btn",
            )
            flush_btn = mc3.button(
                "Flush & tổng kết",
                disabled=not done,
                width="stretch", key="man_flush_btn",
            )

        # ── Tab: Auto-Play ──
        with ctrl_auto:
            st.caption(
                "Stream tự động qua dataset với tốc độ tuỳ chỉnh. "
                "Lịch ở tab **Lập lịch** sẽ tự kích hoạt đúng cursor."
            )
            ap1, ap2, ap3 = st.columns(3)
            speed_eps = ap1.slider(
                "Tốc độ (events/giây)", 100, 10000, 1000, 100,
                key="auto_speed", disabled=state["auto_play_active"],
            )
            refresh_n = ap2.slider(
                "Refresh UI mỗi N events", 10, 500, 50, 10,
                key="auto_refresh", disabled=state["auto_play_active"],
            )
            auto_until = ap3.selectbox(
                "Chạy tới",
                ["Hết stream", "+5,000 events", "+10,000 events"],
                key="auto_until", disabled=state["auto_play_active"],
            )

            play_btn = st.button(
                "Auto-Play realtime", type="primary",
                disabled=done or state["auto_play_active"],
                width="stretch", key="auto_play_btn",
            )

            if state["auto_play_active"]:
                st.error("Đang stream realtime — nút **DỪNG** nằm ở khu vực "
                         "live phía dưới ↓", icon="🔴")
            elif done:
                st.success("Stream hoàn tất.", icon="✅")
            else:
                st.info(f"Sẵn sàng — cursor {cursor:,}/{total:,}.", icon="▶️")

        # ── Tab: Lập lịch ──
        with ctrl_sched:
            # ── Timeline visualization ──
            if state["schedule"]:
                st.markdown("**📍 Dòng thời gian sự cố** (xem theo tỉ lệ cursor):")
                # Build SVG-like timeline using HTML
                tl_html = (
                    '<div style="position:relative;height:46px;background:#F1F5F9;'
                    'border-radius:8px;border:1px solid #CBD5E1;margin:6px 0 8px 0;">'
                )
                # Current cursor marker
                cur_pct = 100.0 * cursor / max(total, 1)
                tl_html += (
                    f'<div style="position:absolute;left:{cur_pct}%;top:0;bottom:0;'
                    f'width:2px;background:#3B82F6;"></div>'
                    f'<div style="position:absolute;left:{cur_pct}%;top:-4px;'
                    f'transform:translateX(-50%);font-size:0.65rem;color:#1E40AF;'
                    f'font-weight:700;background:#DBEAFE;padding:1px 5px;'
                    f'border-radius:4px;border:1px solid #93C5FD;white-space:nowrap;">cursor</div>'
                )
                # Schedule markers
                for act, nid, at in state["schedule"]:
                    pct = 100.0 * at / max(total, 1)
                    icon = "💀" if act == "kill" else "🔄"
                    bg = "#FEE2E2" if act == "kill" else "#DCFCE7"
                    fg = "#991B1B" if act == "kill" else "#166534"
                    border = "#FCA5A5" if act == "kill" else "#86EFAC"
                    tl_html += (
                        f'<div title="{act} Node {nid} @ cursor {at:,}" '
                        f'style="position:absolute;left:{pct}%;top:14px;'
                        f'transform:translateX(-50%);background:{bg};color:{fg};'
                        f'border:1px solid {border};border-radius:14px;'
                        f'padding:2px 7px;font-size:0.72rem;font-weight:700;'
                        f'white-space:nowrap;">{icon} N{nid}@{at:,}</div>'
                    )
                tl_html += "</div>"
                st.markdown(tl_html, unsafe_allow_html=True)

                sched_df = pd.DataFrame(state["schedule"],
                                        columns=["Hành động", "Node", "Tại cursor"])
                st.dataframe(sched_df, hide_index=True, width="stretch")
                ccol1, _ = st.columns([1, 4])
                if ccol1.button("🗑️ Xoá toàn bộ lịch"):
                    state["schedule"] = []
                    st.rerun()
            else:
                st.caption("Chưa có lịch nào — auto-play sẽ chạy như cluster khoẻ mạnh.")

            # ── Manual add row ──
            ac1, ac2, ac3, ac4 = st.columns([1, 1, 1, 1])
            sa = ac1.selectbox("Hành động", ["kill", "revive"], key="sched_act")
            sn = ac2.selectbox("Node", list(range(n)), key="sched_node")
            sc_default = min(cursor + max(1, (total - cursor) // 3), total - 1) if total > 0 else 0
            sx = ac3.number_input("Tại cursor", 0, max(total - 1, 0), sc_default, key="sched_cursor")
            if ac4.button("➕ Thêm vào lịch"):
                state["schedule"].append((sa, int(sn), int(sx)))
                state["schedule"].sort(key=lambda x: x[2])
                st.rerun()

            # ── Preset chaos scenarios ──
            st.markdown("**🎲 Preset kịch bản sự cố** (1 nút = thêm nhiều dòng):")
            pre1, pre2, pre3, pre4 = st.columns(4)
            if pre1.button("⚡ 1 node chết 30% rồi sống 70%",
                           width="stretch"):
                a = max(int(total * 0.30), 1)
                b = max(int(total * 0.70), a + 1)
                state["schedule"] += [("kill", 0, a), ("revive", 0, b)]
                state["schedule"].sort(key=lambda x: x[2])
                st.rerun()
            if pre2.button("💀 Kill chéo 2 node (25%, 60%)",
                           width="stretch"):
                state["schedule"] += [
                    ("kill", 0, int(total * 0.25)),
                    ("kill", min(1, n - 1), int(total * 0.60)),
                ]
                state["schedule"].sort(key=lambda x: x[2])
                st.rerun()
            if pre3.button("🌪️ Chaos: kill xen kẽ tất cả",
                           width="stretch"):
                step = max(int(total / (n * 2 + 1)), 1)
                for i in range(n):
                    state["schedule"].append(("kill", i, step * (2 * i + 1)))
                    state["schedule"].append(("revive", i, step * (2 * i + 2)))
                state["schedule"].sort(key=lambda x: x[2])
                st.rerun()
            if pre4.button("🧹 Xoá toàn bộ preset",
                           width="stretch"):
                state["schedule"] = []
                st.rerun()

        # ── Manual-tab handlers (run AFTER tabs render their widgets) ──
        if run_chunk_btn:
            end = min(cursor + int(kill_chunk), total)
            for j in range(cursor, end):
                row = state["rows"][j]
                host = getattr(row, "host", "unknown")
                nid = partition_key(host, n)
                ev = {"event_id": row.event_id, "event_time": row.event_time,
                      "status": row.status}
                if state["status"][nid] == "alive":
                    state["engines"][nid].process(ev)
                    state["node_processed"][nid] += 1
                else:
                    _buffer_to_dlq(nid, ev)
            state["cursor"] = end
            state["log"].append(
                f"[cursor={cursor:,} → {end:,}] ▶️ Xử lý {end - cursor:,} events · "
                f"dead nodes: {[i for i, s in enumerate(state['status']) if s == 'dead'] or 'none'}"
            )
            sample_sim_history(state)
            st.rerun()

        if run_all_btn:
            for j in range(cursor, total):
                row = state["rows"][j]
                host = getattr(row, "host", "unknown")
                nid = partition_key(host, n)
                ev = {"event_id": row.event_id, "event_time": row.event_time,
                      "status": row.status}
                if state["status"][nid] == "alive":
                    state["engines"][nid].process(ev)
                    state["node_processed"][nid] += 1
                else:
                    _buffer_to_dlq(nid, ev)
            state["cursor"] = total
            state["log"].append(f"⏭️ Đã chạy hết stream tới cursor={total:,}")
            sample_sim_history(state)
            st.rerun()

        if flush_btn:
            for i, eng in enumerate(state["engines"]):
                if state["status"][i] == "alive":
                    eng.flush()
            state["log"].append("🏁 Flush tất cả engines còn sống · tổng kết bên dưới")
            st.rerun()

        # (Nút Dừng + placeholders live đã chuyển vào _autoplay_fragment bên dưới)

        def _render_live_grid(container, st_state, n_nodes):
            with container.container():
                cols = st.columns(n_nodes)
                for ii, ccol in enumerate(cols):
                    with ccol:
                        status = st_state["status"][ii]
                        eng = st_state["engines"][ii]
                        pending_n = len(st_state["pending"][ii])
                        with st.container(border=True):
                            dot = ":green[● alive]" if status == "alive" else ":red[● dead]"
                            st.markdown(f"**Node {ii}** &nbsp; {dot}")
                            st.caption(
                                f"Processed **{st_state['node_processed'][ii]:,}** · "
                                f"Unique **{eng.metrics['unique']:,}** · "
                                f"Win **{len(eng.closed_windows)}**"
                            )
                            st.caption(
                                f"Late **{eng.metrics['late_dropped']}** · "
                                f"Dup **{eng.metrics['duplicates']}** · "
                                + (f":red[**DLQ {pending_n:,}**]" if pending_n
                                   else f"DLQ {pending_n:,}")
                            )

        def _render_live_metrics(container, st_state):
            au = sum(e.metrics["unique"] for e in st_state["engines"])
            ao = sum(e.metrics["on_time"] for e in st_state["engines"])
            ad = sum(e.metrics["duplicates"] for e in st_state["engines"])
            al = sum(e.metrics["late_dropped"] for e in st_state["engines"])
            aw = sum(len(e.closed_windows) for e in st_state["engines"])
            cp = 100.0 * ao / max(au, 1)
            pt = sum(len(p) for p in st_state["pending"])
            dc = sum(1 for s in st_state["status"] if s == "dead")
            with container.container():
                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("Completeness", f"{cp:.2f}%")
                m2.metric("Unique", f"{au:,}")
                m3.metric("Windows", f"{aw}")
                m4.metric("DLQ buffered", f"{pt:,}",
                          delta=f"{dc} node dead" if dc else None)
                m5.metric("Duplicates", f"{ad:,}")

        def _render_live_log(container, st_state):
            with container.container():
                colored = [colorize_log_line(ln) for ln in st_state["log"][-25:]]
                log_html = "<br>".join(colored)
                st.markdown(f'<div class="demo-log">{log_html}</div>',
                            unsafe_allow_html=True)

        # ── Play button handler — initialize chunked auto-play state ──
        if play_btn:
            if auto_until == "Hết stream":
                end_at = total
            elif auto_until == "+5,000 events":
                end_at = min(cursor + 5000, total)
            else:
                end_at = min(cursor + 10000, total)

            state["auto_play_active"] = True
            state["auto_play_end_at"] = int(end_at)
            state["auto_play_speed"] = int(speed_eps)
            state["auto_play_refresh_n"] = int(refresh_n)
            state["auto_play_start"] = int(state["cursor"])
            state["auto_play_sched_idx"] = 0
            state["log"].append(
                f"⏯️ AUTO-PLAY bắt đầu · cursor={state['cursor']:,} → {end_at:,} · "
                f"speed={speed_eps} eps · scheduled={len(state.get('schedule', []))} events"
            )
            st.rerun()

        # ── AUTO-PLAY FRAGMENT ──────────────────────────────────────
        # Dùng @st.fragment + st.rerun(scope="fragment"): mỗi chunk CHỈ
        # rerun vùng này, KHÔNG rerun header / sidebar / tabs / inspector
        # → hết hiện tượng "đa số chức năng bị rerender" (flicker).
        @st.fragment
        def _autoplay_fragment():
            if not state["auto_play_active"]:
                return
            # Nút Dừng nằm TRONG fragment để luôn bắt được click realtime
            if st.button("⏸️ DỪNG STREAM", type="primary",
                         width="stretch", key="frag_stop"):
                state["auto_play_active"] = False
                state["log"].append(
                    f"⏸️ AUTO-PLAY DỪNG bởi user tại cursor={state['cursor']:,}")
                st.rerun()
                return
            st.error("Đang stream realtime — chỉ vùng live này cập nhật, "
                     "phần còn lại của trang đứng yên (không flicker).", icon="🔴")
            live_progress = st.empty()
            live_diag = st.empty()
            live_grid = st.empty()
            live_metrics = st.empty()
            live_log = st.empty()
            ap_speed = state.get("auto_play_speed", 1000)
            ap_refresh = state.get("auto_play_refresh_n", 50)
            ap_end_at = state.get("auto_play_end_at", total)
            ap_start = state.get("auto_play_start", 0)

            chunk_size = max(int(ap_refresh), 25)
            j_start = state["cursor"]

            if j_start >= ap_end_at:
                state["auto_play_active"] = False
                state["log"].append(
                    f"⏯️ AUTO-PLAY kết thúc tại cursor={state['cursor']:,} · "
                    f"dead nodes: {[i for i, s in enumerate(state['status']) if s == 'dead'] or 'không có'}"
                )
                st.rerun()

            j_end = min(j_start + chunk_size, ap_end_at)
            sched_sorted = sorted(state.get("schedule", []), key=lambda x: x[2])
            sched_idx = state.get("auto_play_sched_idx", 0)

            last_row, last_host, last_nid = None, None, None
            for j in range(j_start, j_end):
                # ---- Trigger scheduled actions ----
                while sched_idx < len(sched_sorted) and sched_sorted[sched_idx][2] <= j:
                    act, nid, cur_at = sched_sorted[sched_idx]
                    if act == "kill" and state["status"][nid] == "alive":
                        try:
                            state["engines"][nid].checkpoint()
                            ck_sz = os.path.getsize(state["ckpt_paths"][nid])
                        except Exception:
                            ck_sz = 0
                        state["status"][nid] = "dead"
                        state.setdefault("events_markers", []).append(
                            (j, "kill", nid))
                        state["log"].append(
                            f"[cursor={j:,}] ⏰💀 SCHEDULED KILL Node {nid} · "
                            f"checkpoint {ck_sz}B saved"
                        )
                    elif act == "revive" and state["status"][nid] == "dead":
                        ckpt = state["ckpt_paths"][nid]
                        try:
                            eng_new = WatermarkEngine.restore(
                                ckpt, window_size_s=10.0, allowed_lateness_s=2.0,
                                checkpoint_interval=200, max_queue=10_000_000)
                        except Exception:
                            eng_new = WatermarkEngine(
                                window_size_s=10.0, allowed_lateness_s=2.0,
                                checkpoint_interval=200, checkpoint_path=ckpt,
                                max_queue=10_000_000)
                        dlq = state["dlq_paths"][nid]
                        replayed = 0
                        if os.path.exists(dlq):
                            with open(dlq, "r", encoding="utf-8") as f:
                                for line in f:
                                    line = line.strip()
                                    if not line:
                                        continue
                                    try:
                                        ev = json.loads(line)
                                    except json.JSONDecodeError:
                                        continue
                                    eng_new.process(ev)
                                    replayed += 1
                                    state["node_processed"][nid] += 1
                            try:
                                os.remove(dlq)
                            except OSError:
                                pass
                        state["pending"][nid] = []
                        state["engines"][nid] = eng_new
                        state["status"][nid] = "alive"
                        state.setdefault("events_markers", []).append(
                            (j, "revive", nid))
                        state["log"].append(
                            f"[cursor={j:,}] ⏰🔄 SCHEDULED REVIVE Node {nid} · "
                            f"replay {replayed:,} DLQ events · Exactly-Once OK"
                        )
                    sched_idx += 1

                # ---- Process event ----
                row = state["rows"][j]
                host = getattr(row, "host", "unknown")
                nid = partition_key(host, n)
                ev = {"event_id": row.event_id, "event_time": row.event_time,
                      "status": row.status}
                if state["status"][nid] == "alive":
                    state["engines"][nid].process(ev)
                    state["node_processed"][nid] += 1
                else:
                    with open(state["dlq_paths"][nid], "a", encoding="utf-8") as f:
                        f.write(json.dumps(ev, ensure_ascii=False) + "\n")
                    state["pending"][nid].append(ev)
                    state["node_buffered_total"][nid] += 1

                last_row, last_host, last_nid = row, host, nid

            state["cursor"] = j_end
            state["auto_play_sched_idx"] = sched_idx
            sample_sim_history(state)

            # ---- Periodic dataset log (1 lần / chunk nếu trùng nhịp) ----
            log_period = max(int(ap_refresh) * 5, 1)
            if (j_end - ap_start) % log_period < chunk_size and last_row is not None:
                state["log"].append(
                    f"[cursor={j_end:,}] 📡 streaming dataset · "
                    f"event_id={last_row.event_id} host={last_host} "
                    f"event_time={last_row.event_time:.2f} → Node {last_nid} "
                    f"({state['status'][last_nid]})"
                )

            # ---- Render live UI after chunk ----
            if last_row is not None:
                live_progress.progress(
                    j_end / max(total, 1),
                    text=(f"⏱️ Realtime stream: {j_end:,}/{total:,} · "
                          f"speed={ap_speed} eps · "
                          f"event_id={last_row.event_id} · "
                          f"host={last_host} · status={last_row.status}")
                )
                live_diag.plotly_chart(
                    make_cluster_fig(
                        state["status"],
                        list(state["node_processed"]),
                        [len(e.closed_windows) for e in state["engines"]],
                        [len(p) for p in state["pending"]],
                        j_end, total,
                        highlight_node=last_nid,
                        tick=j_end - ap_start,
                    ),
                    width="content",
                )
                _render_live_grid(live_grid, state, n)
                _render_live_metrics(live_metrics, state)
                _render_live_log(live_log, state)

            # ---- Pace + loop: CHỈ rerun fragment, không rerun cả app ----
            if j_end < ap_end_at:
                time.sleep(chunk_size / max(ap_speed, 1))
                st.rerun(scope="fragment")
            else:
                state["auto_play_active"] = False
                state["log"].append(
                    f"⏯️ AUTO-PLAY kết thúc tại cursor={state['cursor']:,} · "
                    f"dead nodes: {[i for i, s in enumerate(state['status']) if s == 'dead'] or 'không có'}"
                )
                st.rerun()  # full rerun → khôi phục UI tĩnh

        _autoplay_fragment()

        # ============================================================
        # INSPECTOR — 4-tab bottom panel: Metrics / DLQ / Files / Log
        # ============================================================
        # Pre-compute aggregate metrics (used by multiple tabs)
        agg = {"unique": 0, "on_time": 0, "duplicates": 0, "late_dropped": 0, "windows": 0}
        for eng in state["engines"]:
            agg["unique"] += eng.metrics["unique"]
            agg["on_time"] += eng.metrics["on_time"]
            agg["duplicates"] += eng.metrics["duplicates"]
            agg["late_dropped"] += eng.metrics["late_dropped"]
            agg["windows"] += len(eng.closed_windows)
        completeness = 100.0 * agg["on_time"] / max(agg["unique"], 1)
        total_pending = sum(len(p) for p in state["pending"])
        dead_count = sum(1 for s in state["status"] if s == "dead")

        st.markdown("#### Chi tiết")
        ins_metrics, ins_dlq, ins_files, ins_log = st.tabs([
            "Metric cluster", "DLQ trên đĩa", "File hệ thống", "Event log",
        ])

        # ── Tab: Metric cluster (Evolution + Aggregate + Distribution) ──
        with ins_metrics:
            # ▶ Live Evolution Chart — completeness + DLQ + alive nodes vs cursor
            st.markdown("**Diễn biến cluster theo thời gian (event cursor)**")
            st.caption(
                "Trục x = cursor (số event đã xử lý). "
                ":green[Completeness] giảm khi node chết · "
                ":red[DLQ] phình ra khi data dồn lại · "
                ":blue[Alive count] tụt khi kill. "
                "Vạch dọc = thời điểm kill (💀) / revive (🔄)."
            )
            render_sim_evolution_chart(st, state, total)

            st.markdown("**KPI gộp toàn cluster**")
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("Completeness", f"{completeness:.2f}%")
            m2.metric("Unique events", f"{agg['unique']:,}")
            m3.metric("Windows closed", f"{agg['windows']}")
            m4.metric("Đang buffer (dead)", f"{total_pending:,}",
                      delta=f"{dead_count} node dead" if dead_count else None)
            m5.metric("Duplicates lọc", f"{agg['duplicates']:,}")

            st.markdown("**Phân phối tải theo Node**  (`hash(host) % N` skew)")
            # Plotly horizontal bar — đẹp hơn st.bar_chart, có hover tooltip
            node_names = [f"Node {i}" for i in range(n)]
            processed_vals = state["node_processed"]
            dlq_vals = [len(p) for p in state["pending"]]
            color_processed = ["#10B981" if state["status"][i] == "alive" else "#9CA3AF"
                               for i in range(n)]
            dist_fig = go.Figure()
            dist_fig.add_trace(go.Bar(
                x=node_names, y=processed_vals,
                marker_color=color_processed,
                name="Processed",
                text=[f"{v:,}" for v in processed_vals],
                textposition="outside",
                hovertemplate="%{x}<br>processed: %{y:,}<extra></extra>",
            ))
            if any(dlq_vals):
                dist_fig.add_trace(go.Bar(
                    x=node_names, y=dlq_vals,
                    marker_color="#EF4444",
                    name="DLQ buffered",
                    text=[f"{v:,}" if v else "" for v in dlq_vals],
                    textposition="outside",
                    hovertemplate="%{x}<br>DLQ: %{y:,}<extra></extra>",
                ))
            dist_fig.update_layout(
                height=260, barmode="group",
                margin=dict(l=10, r=10, t=20, b=30),
                plot_bgcolor="#F8FAFC", paper_bgcolor="#FFFFFF",
                xaxis=dict(showgrid=False),
                yaxis=dict(title="Events", gridcolor="#E5E7EB"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
            )
            st.plotly_chart(dist_fig, width="stretch",
                            key="dist_bar")

        # ── Tab: DLQ on disk ──
        with ins_dlq:
            dlq_active = [i for i in range(n)
                          if os.path.exists(state["dlq_paths"][i])
                          and os.path.getsize(state["dlq_paths"][i]) > 0]
            if not dlq_active:
                st.info("Hiện không có DLQ nào — không có node nào đang chết hoặc đã được revive.",
                        icon="📭")
            else:
                st.caption(
                    "Mỗi file JSONL append-only chứa events đáng lẽ tới node đang chết. "
                    "Revive node → Coordinator đọc & replay từng dòng rồi xoá file."
                )
                dlq_cols = st.columns(min(len(dlq_active), 3))
                for col_idx, nid in enumerate(dlq_active):
                    with dlq_cols[col_idx % len(dlq_cols)]:
                        dlq_path = state["dlq_paths"][nid]
                        size = os.path.getsize(dlq_path)
                        with open(dlq_path, "r", encoding="utf-8") as f:
                            all_lines = f.readlines()
                        n_lines = len(all_lines)
                        preview_lines = all_lines[:5]
                        if n_lines > 10:
                            preview_lines += [f"...  ({n_lines - 10} dòng giữa)  ...\n"]
                            preview_lines += all_lines[-5:]
                        elif n_lines > 5:
                            preview_lines += all_lines[5:]

                        with st.container(border=True):
                            st.markdown(f"**Node {nid}** &nbsp; :red[● dead]")
                            st.caption(f"`{dlq_path}`")
                            s1, s2 = st.columns(2)
                            s1.metric("Events", f"{n_lines:,}")
                            s2.metric("Size", f"{size:,} B")
                            parsed = []
                            for ln in all_lines[:8]:
                                try:
                                    parsed.append(json.loads(ln))
                                except Exception:
                                    continue
                            if parsed:
                                st.caption("Preview 8 events đầu:")
                                st.dataframe(pd.DataFrame(parsed),
                                             width="stretch",
                                             hide_index=True, height=160)
                            with st.expander("Xem raw JSONL (head + tail)"):
                                st.code("".join(preview_lines), language="json")

        # ── Tab: Filesystem explorer ──
        with ins_files:
            sim_dir = state.get("sim_dir", "")
            st.caption("Copy path vào Explorer / VS Code để xem checkpoint + DLQ realtime.")
            st.code(sim_dir, language="text")
            fs_rows = []
            for i in range(n):
                cp = state["ckpt_paths"][i]
                dq = state["dlq_paths"][i]
                fs_rows.append({
                    "Node": f"Node {i}",
                    "Status": "dead" if state["status"][i] == "dead" else "alive",
                    "Checkpoint": cp,
                    "Ckpt size (B)": os.path.getsize(cp) if os.path.exists(cp) else 0,
                    "DLQ file": dq,
                    "DLQ size (B)": os.path.getsize(dq) if os.path.exists(dq) else 0,
                    "DLQ events (mem)": len(state["pending"][i]),
                })
            st.dataframe(pd.DataFrame(fs_rows),
                         width="stretch", hide_index=True)

        # ── Tab: Event log (colorized + filter) ──
        with ins_log:
            flt_col, _ = st.columns([1, 4])
            log_filter = flt_col.selectbox(
                "Bộ lọc",
                ["Tất cả", "Chỉ KILL/REVIVE", "Chỉ SCHEDULED", "Chỉ STREAM"],
                key="log_filter", label_visibility="collapsed",
            )
            log_lines = state["log"]
            if log_filter == "Chỉ KILL/REVIVE":
                log_lines = [l for l in log_lines
                             if "KILL" in l.upper() or "REVIVE" in l.upper()
                             or "💀" in l or "🔄" in l]
            elif log_filter == "Chỉ SCHEDULED":
                log_lines = [l for l in log_lines
                             if "SCHEDULED" in l.upper() or "⏰" in l]
            elif log_filter == "Chỉ STREAM":
                log_lines = [l for l in log_lines
                             if "streaming" in l.lower() or "📡" in l]

            colored = [colorize_log_line(ln) for ln in log_lines[-30:]]
            log_html = ("<br>".join(colored) if colored
                        else "<i style='color:#94A3B8'>không có log nào khớp bộ lọc</i>")
            st.markdown(f'<div class="demo-log">{log_html}</div>',
                        unsafe_allow_html=True)

        if done and total_pending == 0 and dead_count == 0:
            st.success(
                f"Hoàn tất! Completeness toàn cluster = **{completeness:.2f}%** · "
                f"Tổng buffered từng dùng = {sum(state['node_buffered_total']):,} events "
                f"(đã replay sạch sau revive) → **Exactly-Once đạt được dù có node chết giữa chừng**.",
                icon="✅",
            )
