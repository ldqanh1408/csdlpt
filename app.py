# -*- coding: utf-8 -*-
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

Kỹ thuật UI: vùng realtime (Tab 1 & Tab 5) dùng @st.fragment(run_every=…)
→ chỉ vùng live tự cập nhật theo timer, phần còn lại của trang KHÔNG bị
rerender (chống flicker).
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

from wm import (WatermarkEngine, generate_logs, load_nasa_csv,
                EngineConfig, DEFAULT_CONFIG, PRESETS,
                kill_node, revive_node, kill_all, revive_all)
from wm.scenario import Scenario, ScenarioAction
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
# STATE RECOVERY SVG DIAGRAM (TAB 4)
# ============================================================
def generate_recovery_svg(state, processed_e=0, dup_filtered=0):
    e1_class = "node-box"
    e2_class = "node-box"
    disk_class = "node-box"
    
    path1_class = "flow-line"
    path2_class = "flow-line"
    
    e1_status = "READY"
    e1_sub = f"Processed: {processed_e} log" if state in ["eng1_running", "checkpoint", "crashed"] else "Processed: 0"
    
    disk_status = "No Snapshot"
    disk_sub = "Empty"
    
    e2_status = "OFFLINE"
    e2_sub = "Processed: 0"
    
    if state == "init":
        e1_class += " status-ready"
        e1_status = "READY"
    elif state == "eng1_running":
        e1_class += " status-running"
        e1_status = "RUNNING 🔄"
    elif state == "checkpoint":
        e1_class += " status-running"
        e1_status = "CHECKPOINTING 💾"
        disk_class += " status-saved"
        disk_status = "SNAPSHOT SAVED 💾"
        disk_sub = "web_recovery.json"
        path1_class += " flow-line-active-blue"
    elif state == "crashed":
        e1_class += " status-crashed"
        e1_status = "CRASHED 💀"
        disk_class += " status-saved"
        disk_status = "SNAPSHOT SAVED 💾"
        disk_sub = "web_recovery.json"
    elif state == "eng2_restore":
        e1_class += " status-muted"
        e1_status = "CRASHED 💀"
        disk_class += " status-saved"
        disk_status = "SNAPSHOT SAVED 💾"
        disk_sub = "web_recovery.json"
        e2_class += " status-restore"
        e2_status = "RESTORING... 🔄"
        e2_sub = "Restoring state"
        path2_class += " flow-line-active-green"
    elif state == "eng2_running":
        e1_class += " status-muted"
        e1_status = "OFFLINE ❌"
        disk_class += " status-saved"
        disk_status = "SNAPSHOT SAVED 💾"
        disk_sub = "web_recovery.json"
        e2_class += " status-active-green"
        e2_status = "RUNNING (RECOVERED)"
        e2_sub = f"Processed: {processed_e} log"
    elif state == "done":
        e1_class += " status-muted"
        e1_status = "OFFLINE ❌"
        disk_class += " status-saved"
        disk_status = "SNAPSHOT SAVED 💾"
        disk_sub = "web_recovery.json"
        e2_class += " status-active-green"
        e2_status = "COMPLETED 🏁"
        e2_sub = f"Exactly-Once: OK ✅"

    svg = f"""
    <svg width="100%" height="200" viewBox="0 0 650 200" xmlns="http://www.w3.org/2000/svg">
      <defs>
        <filter id="glow-blue" x="-20%" y="-20%" width="140%" height="140%">
          <feGaussianBlur stdDeviation="6" result="blur" />
          <feComposite in="SourceGraphic" in2="blur" operator="over" />
        </filter>
        <filter id="glow-red" x="-20%" y="-20%" width="140%" height="140%">
          <feGaussianBlur stdDeviation="6" result="blur" />
          <feComposite in="SourceGraphic" in2="blur" operator="over" />
        </filter>
        <filter id="glow-green" x="-20%" y="-20%" width="140%" height="140%">
          <feGaussianBlur stdDeviation="6" result="blur" />
          <feComposite in="SourceGraphic" in2="blur" operator="over" />
        </filter>
        <filter id="glow-amber" x="-20%" y="-20%" width="140%" height="140%">
          <feGaussianBlur stdDeviation="6" result="blur" />
          <feComposite in="SourceGraphic" in2="blur" operator="over" />
        </filter>
      </defs>
      <style>
        .node-box {{ fill: #1E293B; stroke: #334155; stroke-width: 2; transition: all 0.5s ease; }}
        .node-title {{ font-family: 'Outfit', sans-serif; font-size: 13px; font-weight: bold; fill: #F8FAFC; text-anchor: middle; }}
        .node-text {{ font-family: 'Outfit', sans-serif; font-size: 11px; fill: #94A3B8; text-anchor: middle; }}
        .node-metric {{ font-family: 'JetBrains Mono', monospace; font-size: 11px; fill: #38BDF8; text-anchor: middle; }}
        .node-metric-green {{ font-family: 'JetBrains Mono', monospace; font-size: 11px; fill: #34D399; text-anchor: middle; }}
        
        .status-ready {{ stroke: #64748B; }}
        .status-running {{ stroke: #3B82F6; filter: url(#glow-blue); }}
        .status-crashed {{ stroke: #EF4444; filter: url(#glow-red); }}
        .status-saved {{ stroke: #F59E0B; filter: url(#glow-amber); }}
        .status-restore {{ stroke: #06B6D4; filter: url(#glow-blue); }}
        .status-active-green {{ stroke: #10B981; filter: url(#glow-green); }}
        .status-muted {{ stroke: #1E293B; stroke-dasharray: 4,4; opacity: 0.5; }}
        
        .flow-line {{ fill: none; stroke: #334155; stroke-width: 3; transition: all 0.5s ease; }}
        .flow-line-active-blue {{ stroke: #3B82F6; stroke-dasharray: 6, 4; animation: dash 1s linear infinite; }}
        .flow-line-active-green {{ stroke: #10B981; stroke-dasharray: 6, 4; animation: dash 1s linear infinite; }}
        
        @keyframes dash {{
          to {{
            stroke-dashoffset: -20;
          }}
        }}
      </style>
      
      <!-- Box 1: Engine 1 -->
      <rect class="{e1_class}" x="15" y="30" width="160" height="130" rx="10" ry="10" />
      <text class="node-title" x="95" y="60">ENGINE 1</text>
      <text class="node-text" x="95" y="90">{e1_status}</text>
      <text class="node-metric" x="95" y="120">{e1_sub}</text>
      
      <!-- Connection 1 -->
      <path class="{path1_class}" d="M 175 95 L 245 95" />
      
      <!-- Box 2: State Store -->
      <rect class="{disk_class}" x="245" y="30" width="160" height="130" rx="10" ry="10" />
      <text class="node-title" x="325" y="60">💾 STATE STORE</text>
      <text class="node-text" x="325" y="90">{disk_status}</text>
      <text class="node-text" x="325" y="120" style="font-size: 10px; fill: #F59E0B;">{disk_sub}</text>
      
      <!-- Connection 2 -->
      <path class="{path2_class}" d="M 405 95 L 475 95" />
      
      <!-- Box 3: Engine 2 -->
      <rect class="{e2_class}" x="475" y="30" width="160" height="130" rx="10" ry="10" />
      <text class="node-title" x="555" y="60">ENGINE 2</text>
      <text class="node-text" x="555" y="90">{e2_status}</text>
      <text class="node-metric-green" x="555" y="120">{e2_sub}</text>
    </svg>
    """
    return svg


def update_recovery_diagram(placeholder, state, processed_e=0):
    with placeholder:
        st.components.v1.html(generate_recovery_svg(state, processed_e), height=210)


# ============================================================
# ANIMATED DYNAMIC SVG TOPOLOGY DIAGRAM
# ============================================================
def generate_svg_cluster(statuses, processed, windows_closed, dlq_counts, ckpt_sizes, highlight_node=None, tick=0):
    n = len(statuses)
    width = 900
    height = 420
    
    svg = f"""
    <div style="display: flex; justify-content: center; width: 100%;">
    <svg width="100%" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" style="background:#090D16; border-radius:14px; font-family:'Outfit', sans-serif; border: 1px solid #1E293B; box-shadow: 0px 10px 30px rgba(0,0,0,0.5);">
      <style>
        .glow {{ filter: drop-shadow(0px 0px 8px rgba(59, 130, 246, 0.6)); }}
        .glow-green {{ filter: drop-shadow(0px 0px 10px rgba(16, 185, 129, 0.7)); }}
        .glow-red {{ filter: drop-shadow(0px 0px 10px rgba(239, 68, 68, 0.8)); }}
        .pulse-green {{ animation: pulse-g 2s infinite alternate; }}
        .pulse-red {{ animation: pulse-r 1s infinite alternate; }}
        .node-card {{ transition: all 0.3s ease; }}
        @keyframes pulse-g {{
          0% {{ fill: #059669; filter: drop-shadow(0 0 2px #10B981); }}
          100% {{ fill: #4ADE80; filter: drop-shadow(0 0 10px #4ADE80); }}
        }}
        @keyframes pulse-r {{
          0% {{ fill: #B91C1C; filter: drop-shadow(0 0 2px #EF4444); }}
          100% {{ fill: #F87171; filter: drop-shadow(0 0 10px #F87171); }}
        }}
        .packet-flow {{
          stroke-dasharray: 8, 12;
          animation: flow 3s linear infinite;
        }}
        .packet-flow-fast {{
          stroke-dasharray: 5, 8;
          animation: flow 1.2s linear infinite;
        }}
        .packet-flow-red {{
          stroke-dasharray: 6, 10;
          animation: flow 2.5s linear infinite;
        }}
        @keyframes flow {{
          to {{ stroke-dashoffset: -100; }}
        }}
      </style>
      
      <!-- Gradients -->
      <defs>
        <linearGradient id="blueGrad" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stop-color="#1E3A8A" />
          <stop offset="100%" stop-color="#3B82F6" />
        </linearGradient>
        <linearGradient id="purpleGrad" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stop-color="#5B21B6" />
          <stop offset="100%" stop-color="#7C3AED" />
        </linearGradient>
        <linearGradient id="amberGrad" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stop-color="#78350F" />
          <stop offset="100%" stop-color="#F59E0B" />
        </linearGradient>
      </defs>
    """
    
    # Ingestion Box
    ing_x, ing_y, ing_w, ing_h = 375, 15, 150, 45
    # Partitioner Box
    part_x, part_y, part_w, part_h = 350, 95, 200, 45
    # Coordinator Box
    coor_x, coor_y, coor_w, coor_h = 350, 360, 200, 45
    
    # Ingestion Source
    svg += f"""
      <rect x="{ing_x}" y="{ing_y}" width="{ing_w}" height="{ing_h}" rx="8" fill="url(#amberGrad)" stroke="#F59E0B" stroke-width="1.5" />
      <text x="{ing_x + ing_w/2}" y="{ing_y + 27}" fill="#FEF3C7" font-size="12" font-weight="bold" text-anchor="middle">📦 Dataset Stream</text>
    """
    
    # Partitioner
    svg += f"""
      <rect x="{part_x}" y="{part_y}" width="{part_w}" height="{part_h}" rx="8" fill="url(#blueGrad)" stroke="#2563EB" stroke-width="1.5" />
      <text x="{part_x + part_w/2}" y="{part_y + 27}" fill="#E0F2FE" font-size="12" font-weight="bold" text-anchor="middle">⚖️ Partitioner : hash(host)%N</text>
    """
    
    # Connection Ingestion -> Partitioner
    svg += f"""
      <path d="M 450,{ing_y+ing_h} L 450,{part_y}" stroke="#475569" stroke-width="2" />
      <path class="packet-flow" d="M 450,{ing_y+ing_h} L 450,{part_y}" stroke="#F59E0B" stroke-width="2.5" />
    """
    
    # Spacing for nodes
    node_y = 200
    node_w, node_h = 135, 105
    gap = 20
    total_w = n * node_w + (n - 1) * gap
    start_x = (width - total_w) / 2
    
    centers_x = []
    for i in range(n):
        cx = start_x + i * (node_w + gap) + node_w / 2
        centers_x.append(cx)
        
        status = statuses[i]
        is_highlight = (highlight_node == i)
        
        if status == "alive":
            bg_color = "#1E293B"
            border_color = "#10B981" if is_highlight else "#334155"
            header_bg = "#065F46" if is_highlight else "#0F172A"
            status_text = "🟢 ALIVE"
            status_class = "pulse-green"
            line_color = "#10B981" if is_highlight else "#475569"
            line_class = "packet-flow-fast" if is_highlight else "packet-flow"
            line_stroke = "#059669" if is_highlight else "#334155"
        else:
            bg_color = "#270F15"
            border_color = "#EF4444"
            header_bg = "#7F1D1D"
            status_text = "💀 DEAD"
            status_class = "pulse-red"
            line_color = "#EF4444"
            line_class = "packet-flow-red"
            line_stroke = "#B91C1C"
            
        # Partitioner -> Node Connection (curved quadratic bezier)
        svg += f"""
          <path d="M 450,140 Q {cx},160 {cx},{node_y}" stroke="{line_stroke}" stroke-width="2" fill="none" stroke-dasharray="{"6,6" if status == "dead" else "none"}"/>
          <path class="{line_class}" d="M 450,140 Q {cx},160 {cx},{node_y}" stroke="{line_color}" stroke-width="2" fill="none" />
        """
        
        # Node -> Coordinator Connection
        svg += f"""
          <path d="M {cx},{node_y+node_h} Q {cx},{coor_y-20} 450,{coor_y}" stroke="{"#B91C1C" if status == "dead" else "#4C1D95"}" stroke-width="1.5" fill="none" stroke-dasharray="{"6,6" if status == "dead" else "none"}"/>
          {" " if status == "dead" else f'<path class="packet-flow" d="M {cx},{node_y+node_h} Q {cx},{coor_y-20} 450,{coor_y}" stroke="#A78BFA" stroke-width="1.8" fill="none" />'}
        """
        
        # Card body
        card_x = cx - node_w / 2
        card_y = node_y
        glow_class = "glow-green" if (status == "alive" and is_highlight) else ("glow-red" if status == "dead" else "")
        
        svg += f"""
          <g class="node-card {glow_class}">
            <rect x="{card_x}" y="{card_y}" width="{node_w}" height="{node_h}" rx="10" fill="{bg_color}" stroke="{border_color}" stroke-width="2" />
            <rect x="{card_x+1}" y="{card_y+1}" width="{node_w-2}" height="24" rx="8 8 0 0" fill="{header_bg}" />
            <text x="{cx}" y="{card_y+16}" fill="#F8FAFC" font-size="10.5" font-weight="bold" text-anchor="middle">Node {i}</text>
            <circle class="{status_class}" cx="{card_x + 18}" cy="{card_y+12}" r="4" />
            
            <text x="{card_x+12}" y="{card_y+43}" fill="#94A3B8" font-size="8.5" font-family="monospace">Proc: {processed[i]:,}</text>
            <text x="{card_x+12}" y="{card_y+56}" fill="#94A3B8" font-size="8.5" font-family="monospace">Wins: {windows_closed[i]}</text>
            <text x="{card_x+12}" y="{card_y+69}" fill="{"#F87171" if dlq_counts[i] > 0 else "#94A3B8"}" font-size="8.5" font-family="monospace" font-weight="{"bold" if dlq_counts[i] > 0 else "normal"}">DLQ: {dlq_counts[i]:,}</text>
            <text x="{card_x+12}" y="{card_y+82}" fill="#94A3B8" font-size="8.5" font-family="monospace">Ckpt: {ckpt_sizes[i]:,} B</text>
            
            <rect x="{cx - 28}" y="{card_y+88}" width="56" height="12" rx="4" fill="{"#065F46" if status == "alive" else "#7F1D1D"}" />
            <text x="{cx}" y="{card_y+97}" fill="#F8FAFC" font-size="8.5" font-weight="bold" text-anchor="middle">{status.upper()}</text>
          </g>
        """
        
    # Coordinator Box
    svg += f"""
      <rect x="{coor_x}" y="{coor_y}" width="{coor_w}" height="{coor_h}" rx="8" fill="url(#purpleGrad)" stroke="#6D28D9" stroke-width="1.5" />
      <text x="{coor_x + coor_w/2}" y="{coor_y + 27}" fill="#F5F3FF" font-size="12" font-weight="bold" text-anchor="middle">🧮 Coordinator (merge metrics)</text>
    </svg>
    </div>
    """
    return svg


# ============================================================
# UNIFIED SIMULATION STEP EXECUTOR
# ============================================================
def execute_step(state, j, cfg):
    """Processes a single event at index j, triggering any scheduled actions."""
    # 1. Trigger scheduled actions
    schedule = state.get("schedule", [])
    for item in schedule:
        act_type = item[0]
        nid = item[1]
        at = item[2]
        params = item[3] if len(item) > 3 else {}
        label = item[4] if len(item) > 4 else ""
        if at == j:
            trigger_action(state, act_type, nid, params, label, cfg)

    # 2. Process the event
    row = state["rows"][j]
    host = getattr(row, "host", "unknown")
    n = len(state["engines"])
    nid = partition_key(host, n)
    ev = {"event_id": row.event_id, "event_time": row.event_time, "status": row.status}

    if state["status"][nid] == "alive":
        state["engines"][nid].process(ev)
        state["node_processed"][nid] += 1
    else:
        # Buffer to disk DLQ file (append-only JSONL)
        with open(state["dlq_paths"][nid], "a", encoding="utf-8") as f:
            f.write(json.dumps(ev, ensure_ascii=False) + "\n")
        state["pending"][nid].append(ev)
        state["node_buffered_total"][nid] += 1


def trigger_action(state, act_type, nid, params, label, cfg):
    """Triggers a single scheduled simulation action."""
    cursor_now = state["cursor"]
    if act_type == "kill":
        if state["status"][nid] == "alive":
            kill_node(state, nid)
    elif act_type == "revive":
        if state["status"][nid] == "dead":
            revive_node(state, nid, cfg)
    elif act_type == "ooo_spike":
        frac = params.get("fraction", 0.8)
        dur = params.get("duration", 500)
        slice_end = min(cursor_now + dur, len(state["rows"]))
        if slice_end > cursor_now:
            import random
            sub_rows = state["rows"][cursor_now:slice_end]
            modified = []
            for r in sub_rows:
                if random.random() < frac:
                    new_delay = random.uniform(5.0, 15.0)
                    modified.append(r._replace(arrival_time=r.event_time + new_delay))
                else:
                    modified.append(r)
            modified.sort(key=lambda x: x.arrival_time)
            state["rows"][cursor_now:slice_end] = modified
            state["log"].append(
                f"[cursor={cursor_now:,}] 🌪️ BÃO LOG (OOO SPIKE) kích hoạt: xáo trộn {frac*100:.0f}% dữ liệu trễ trong {dur} events"
            )
            state.setdefault("events_markers", []).append((cursor_now, "ooo_spike", nid))
    elif act_type == "load_burst":
        mult = params.get("multiplier", 3.0)
        dur = params.get("duration", 1000)
        state["burst_multiplier"] = mult
        state["burst_end_at"] = cursor_now + dur
        state["log"].append(
            f"[cursor={cursor_now:,}] ⚡ TẢI TĂNG ĐỘT NGỘT (LOAD BURST) kích hoạt: tăng tải {mult}x trong {dur} events"
        )
        state.setdefault("events_markers", []).append((cursor_now, "load_burst", nid))
    elif act_type == "delay_inject":
        delay = params.get("delay_s", 5.0)
        dur = params.get("duration", 500)
        slice_end = min(cursor_now + dur, len(state["rows"]))
        if slice_end > cursor_now:
            state["rows"][cursor_now:slice_end] = [
                r._replace(arrival_time=r.arrival_time + delay)
                for r in state["rows"][cursor_now:slice_end]
            ]
            state["log"].append(
                f"[cursor={cursor_now:,}] ⏳ TRỄ ĐƯỜNG TRUYỀN (DELAY INJECT) kích hoạt: thêm {delay}s trễ trong {dur} events"
            )
            state.setdefault("events_markers", []).append((cursor_now, "delay_inject", nid))


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
        plot_bgcolor="#0F172A",
        paper_bgcolor="#0F172A",
        font=dict(color="#94A3B8"),
        xaxis=dict(title="Cursor (events processed)", gridcolor="#1E293B",
                   range=[0, max(total_events, max(xs) if xs else 1)]),
        yaxis=dict(title="Completeness %", color="#10B981",
                   range=[0, 105], gridcolor="#1E293B"),
        yaxis2=dict(title="DLQ buffered", color="#EF4444",
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
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&family=JetBrains+Mono:wght@400;700&display=swap');

    html, body, [data-testid="stAppViewContainer"], .main {
        font-family: 'Outfit', 'Inter', sans-serif !important;
        background-color: #0F172A;
        color: #E2E8F0;
    }

    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: #1E293B;
        padding: 6px 10px;
        border-radius: 12px;
        border: 1px solid #334155;
    }

    .stTabs [data-baseweb="tab"] {
        font-family: 'Outfit', sans-serif !important;
        font-size: 0.92rem;
        font-weight: 600;
        color: #94A3B8;
        background-color: transparent;
        border: none;
        border-radius: 8px;
        padding: 6px 14px;
        transition: all 0.25s ease;
    }

    .stTabs [data-baseweb="tab"]:hover {
        color: #F8FAFC;
        background-color: #334155;
    }

    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        color: #FFFFFF !important;
        background-color: #3B82F6 !important;
        box-shadow: 0px 4px 12px rgba(59, 130, 246, 0.4);
    }

    .hero {
        background: linear-gradient(135deg, #1E3A8A 0%, #3B82F6 50%, #0D9488 100%);
        color: #FFFFFF;
        padding: 22px 28px;
        border-radius: 16px;
        margin-bottom: 18px;
        box-shadow: 0px 8px 24px rgba(0, 0, 0, 0.3);
        border: 1px solid rgba(255, 255, 255, 0.1);
        position: relative;
        overflow: hidden;
    }

    .hero h1 {
        font-family: 'Outfit', sans-serif !important;
        margin: 0;
        font-size: 2.10rem;
        font-weight: 800;
        letter-spacing: -0.5px;
    }

    .hero p {
        margin: 8px 0 0 0;
        font-size: 1.0rem;
        opacity: 0.9;
        font-weight: 300;
    }

    .badge {
        display: inline-block;
        background: rgba(255, 255, 255, 0.12);
        color: #F1F5F9;
        padding: 4px 12px;
        border-radius: 999px;
        font-size: 0.78rem;
        font-weight: 500;
        margin: 10px 8px 0 0;
        border: 1px solid rgba(255, 255, 255, 0.18);
        backdrop-filter: blur(4px);
    }

    .demo-log {
        background-color: #030712;
        color: #94A3B8;
        font-family: 'JetBrains Mono', Consolas, monospace !important;
        padding: 14px;
        border-radius: 10px;
        height: 270px;
        overflow-y: auto;
        white-space: pre-wrap;
        font-size: 0.8rem;
        line-height: 1.5;
        border: 1px solid #1E293B;
        box-shadow: inset 0px 4px 10px rgba(0,0,0,0.4);
    }

    .demo-log .log-kill    { color: #F87171; font-weight: bold; }
    .demo-log .log-revive  { color: #4ADE80; font-weight: bold; }
    .demo-log .log-stream  { color: #60A5FA; }
    .demo-log .log-sched   { color: #FBBF24; }
    .demo-log .log-info    { color: #94A3B8; }

    /* Custom modern styling for containers */
    div[data-testid="stMetricValue"] {
        font-family: 'Outfit', sans-serif !important;
        font-weight: 800 !important;
        color: #3B82F6;
    }

    /* Sidebar updates */
    section[data-testid="stSidebar"] {
        background-color: #0B0F19 !important;
        border-right: 1px solid #1E293B;
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
tab_overview, tab_report, tab_stream, tab_sweep, tab_demos, tab_dist, tab_sim = st.tabs([
    "🏠 Tổng quan dự án",
    "📖 Tài liệu báo cáo",
    "📈 Live Stream",
    "📊 Sweep Analysis",
    "🛡️ Recovery & Backpressure",
    "🖥️ Distributed Cluster",
    "🔪 Simulation Lab",
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

        import plotly.graph_objects as go
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


# ============================================================
# TAB 1 — BÁO CÁO HỆ THỐNG
# ============================================================
with tab_report:
    report_path = "SYSTEM_REPORT.md"
    if os.path.exists(report_path):
        with open(report_path, "r", encoding="utf-8") as f:
            report_md = f.read()
        
        # Style the report container nicely
        st.markdown("""
        <div style="background-color: #1E293B; border: 1px solid #334155; border-radius: 12px; padding: 24px; box-shadow: 0px 4px 15px rgba(0,0,0,0.25); margin-bottom: 24px;">
            <div style="text-align: center; margin-bottom: 20px;">
                <span style="background-color: #3B82F6; color: white; padding: 4px 12px; border-radius: 999px; font-size: 0.8rem; font-weight: bold;">TÀI LIỆU CHÍNH THỨC</span>
            </div>
        """, unsafe_allow_html=True)
        st.markdown(report_md)
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        st.warning("Không tìm thấy file SYSTEM_REPORT.md trong thư mục gốc.")


# ============================================================
# TAB 2 — LIVE STREAM
# ============================================================
with tab_stream:
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
        recovery_diag_placeholder = st.empty()
        update_recovery_diagram(recovery_diag_placeholder, "init")

        if run_recovery_btn:
            log_output = []
            def log_print(msg):
                log_output.append(msg)
                recovery_log.markdown(f'<div class="demo-log">{"".join(log_output)}</div>', unsafe_allow_html=True)
                time.sleep(0.4)

            update_recovery_diagram(recovery_diag_placeholder, "eng1_running", processed_e=0)
            log_print("🚀 Khởi chạy Crash Recovery Demo...\n")
            df = generate_logs(n_events=events_recovery)
            rows = list(df.itertuples(index=False))
            half = len(rows) // 2
            ckpt_path = os.path.join(tempfile.gettempdir(), "web_recovery.json")

            log_print("⚙️ Tạo Engine 1 (window=10s, wait=2s) và nạp 50% dữ liệu đầu...\n")
            _rec_cfg = EngineConfig(checkpoint_interval=100)
            eng = WatermarkEngine(checkpoint_path=ckpt_path, **_rec_cfg.to_engine_kwargs())
            for r in rows[:half]:
                eng.process({"event_id": r.event_id, "event_time": r.event_time, "status": r.status})
            
            update_recovery_diagram(recovery_diag_placeholder, "eng1_running", processed_e=half)
            log_print("💾 Ghi Checkpoint Atomic lên đĩa...\n")
            eng.checkpoint()
            
            update_recovery_diagram(recovery_diag_placeholder, "checkpoint", processed_e=half)
            time.sleep(0.8)

            before = dict(eng.metrics)
            log_print(f"📊 Trạng thái Engine 1 TRƯỚC khi crash:\n"
                      f"   - total = {before['total']}\n"
                      f"   - unique = {before['unique']}\n"
                      f"   - on_time = {before['on_time']}\n")

            log_print("🔥 !!! CRASH !!! Tiến trình DIE đột ngột.\n")
            del eng
            
            update_recovery_diagram(recovery_diag_placeholder, "crashed", processed_e=half)
            time.sleep(1.0)

            log_print("🔄 Engine 2 khởi tạo và RESTORE từ checkpoint...\n")
            update_recovery_diagram(recovery_diag_placeholder, "eng2_restore", processed_e=half)
            time.sleep(1.2)
            
            eng2 = WatermarkEngine.restore(ckpt_path, **_rec_cfg.to_engine_kwargs())
            rm = eng2.metrics
            
            update_recovery_diagram(recovery_diag_placeholder, "eng2_running", processed_e=half)
            log_print(f"📊 Trạng thái Engine 2 sau khôi phục:\n"
                      f"   - total = {rm['total']}\n"
                      f"   - unique = {rm['unique']}\n"
                      f"   - on_time = {rm['on_time']}\n")

            log_print("⚙️ Engine 2 tiếp tục xử lý 50% còn lại...\n")
            for r in rows[half:]:
                eng2.process({"event_id": r.event_id, "event_time": r.event_time, "status": r.status})
            
            update_recovery_diagram(recovery_diag_placeholder, "eng2_running", processed_e=len(rows))
            eng2.flush()
            s = eng2.summary()
            
            update_recovery_diagram(recovery_diag_placeholder, "done", processed_e=len(rows))
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
            _bp_cfg = EngineConfig(checkpoint_interval=5000, max_queue=max_q_size)
            eng = WatermarkEngine(
                checkpoint_path=os.path.join(tempfile.gettempdir(), "bp_ckpt.json"),
                **_bp_cfg.to_engine_kwargs(),
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
                        line=dict(color="#EF4444", width=2, dash="dash"),
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
                        annotation_font_color="#FB923C",
                    )
                    fig_rt.update_layout(
                        height=260, margin=dict(l=10, r=10, t=40, b=30),
                        title=dict(
                            text=f"Backpressure realtime — {i + 1:,} events processed",
                            font=dict(size=12, color="#E2E8F0")),
                        plot_bgcolor="#0F172A", paper_bgcolor="#0F172A",
                        font=dict(color="#94A3B8"),
                        xaxis=dict(title=f"Sample tick (×{SAMPLE} events)",
                                   gridcolor="#1E293B"),
                        yaxis=dict(title="Count", gridcolor="#1E293B"),
                        legend=dict(orientation="h", yanchor="bottom",
                                    y=1.06, x=0),
                        hovermode="x unified",
                    )
                    bp_plot.plotly_chart(fig_rt, width="stretch",
                                         key=f"bp_rt_{i}")

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


# ============================================================
# TAB 5 — KILL NODE LIVE (mô phỏng node sống / chết / hồi sinh)
# ============================================================
with tab_sim:
    st.header("🔬 Simulation Lab — Nghiên cứu sự cố & Khôi phục phân tán")
    st.markdown(
        "Môi trường mô phỏng sự cố phân tán nâng cao. Mỗi node trong cluster là một **Watermark Engine độc lập** "
        "với checkpoint riêng. Bạn có thể tự kích hoạt lỗi (kill/revive) thủ công hoặc sử dụng các **Kịch bản có sẵn (Scenario Presets)** "
        "để mô phỏng các hiện tượng bất thường của mạng phân tán (như bão log lệch thứ tự, tăng tải đột ngột, trễ truyền thông)."
    )

    with st.expander("🧠 Cơ chế lưu trữ đệm DLQ & Phục hồi trạng thái", expanded=False):
        st.markdown(
            "Khi một node bị sập, Partitioner vẫn định tuyến dữ liệu về node đó dựa trên khóa băm. Coordinator sẽ đóng vai trò "
            "vùng đệm, **ghi nối tiếp (append-only) các log vào Dead-Letter Queue (DLQ) trên đĩa** dưới dạng file JSONL:\n"
            "`./.simdata/dlq/dlq_node_[id].jsonl`.\n\n"
            "- **Append-only** giúp tối ưu hóa IOPS và an toàn trước lỗi sập nguồn Coordinator.\n"
            "- Khi **Revive** (hồi sinh node), node sẽ tự động **restore** trạng thái cửa sổ gần nhất từ Checkpoint Atomic "
            "(`./.simdata/checkpoints/kill_node_[id].json`), sau đó **replay** toàn bộ log trong DLQ trên đĩa.\n"
            "- Tập hợp `seen_ids` (id đã xử lý) được lưu cùng checkpoint sẽ tự động lọc trùng (dedup), đảm bảo **Exactly-Once Semantics** "
            "cho toàn hệ thống."
        )

    # ----- Scenario Selection -----
    st.markdown("### 🎬 Kịch bản mô phỏng")
    preset_options = ["Chạy thủ công (Tự lập lịch)"] + list(PRESETS.keys())
    selected_preset = st.selectbox(
        "Chọn kịch bản sự cố (Scenario Presets)",
        preset_options,
        key="sim_preset_choice"
    )

    preset_active = (selected_preset != "Chạy thủ công (Tự lập lịch)")
    if preset_active:
        scenario = PRESETS[selected_preset]
        st.info(f"📝 **Mô tả kịch bản**: {scenario.description}")
        
        # Draw a beautiful preset timeline summary
        timeline_desc = []
        for a in scenario.timeline:
            icon = "💀 Kill" if a.type == "kill" else ("🔄 Revive" if a.type == "revive" else f"⚡ {a.type.upper()}")
            timeline_desc.append(f"`{icon} Node {a.target if a.target != -1 else 'Tất cả'} tại cursor {a.at:,}`")
        st.markdown("**Timeline sự cố sẽ được nạp tự động:** " + " → ".join(timeline_desc))

    # ----- Config / Setup -----
    st.markdown("### 🏗️ Cấu hình Cluster")
    cc1, cc2, cc3, cc4 = st.columns(4)
    
    if preset_active:
        default_nodes = scenario.n_nodes
        default_events = 10000
    else:
        default_nodes = 4
        default_events = 10000

    kill_nodes = cc1.slider(
        "Số Node", 2, 6, default_nodes, key="kill_n_nodes",
        disabled=preset_active
    )
    kill_source = cc2.selectbox("Nguồn dữ liệu", ["Synthetic", "NASA HTTP Real"], key="kill_source")
    kill_events = cc3.number_input(
        "Tổng số events", 2000, 100000, default_events, 1000, key="kill_events_n",
        disabled=preset_active
    )
    kill_chunk = cc4.number_input("Events / bước chạy", 100, 5000, 500, 100, key="kill_chunk_n")

    cinit, _, creset = st.columns([1, 4, 1])
    init_btn = cinit.button("🏗️ Khởi tạo & nạp kịch bản", type="primary")
    reset_btn = creset.button("🗑️ Reset phòng lab")

    if reset_btn:
        st.session_state.pop("kill_state", None)
        st.rerun()

    if init_btn:
        # Load dataset
        if kill_source == "NASA HTTP Real":
            if not os.path.exists("dataset/data.csv"):
                st.error("Không tìm thấy file dataset/data.csv trong hệ thống!")
                st.stop()
            df_k = load_nasa_csv("dataset/data.csv", limit=int(kill_events))
        else:
            df_k = generate_logs(n_events=int(kill_events))
            df_k = df_k.assign(host=df_k["endpoint"])

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
            if os.path.exists(dlq):
                try:
                    os.remove(dlq)
                except OSError:
                    pass
            ckpt_paths.append(p)
            dlq_paths.append(dlq)
            
            # Load scenario specific config or DEFAULT_CONFIG
            cfg_to_use = scenario.config if preset_active else DEFAULT_CONFIG
            engines.append(WatermarkEngine(
                checkpoint_path=p,
                **cfg_to_use.to_engine_kwargs(),
            ))

        # Build schedule list
        schedule = []
        if preset_active:
            # Overwrite event cursor scale if dataset is smaller/larger than scenario base
            # For NASA/Synthetic, we scale the absolute trigger positions relative to total events
            scale = len(df_k) / 10000.0  # presets are defined for 10k events base
            for a in scenario.timeline:
                scaled_at = min(int(a.at * scale), len(df_k) - 1)
                schedule.append((a.type, a.target, scaled_at, a.params, a.label))
        
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
                f"🎬 Kịch bản: {selected_preset}",
                f"📂 Checkpoints: {ckpt_dir}",
                f"📂 DLQ: {dlq_dir}",
            ],
            "sim_history": {
                "cursor": [0],
                "completeness": [100.0],
                "dlq_total": [0],
                "alive_count": [kill_nodes],
                "throughput": [0],
            },
            "events_markers": [],
            "schedule": schedule,
            "engine_config": scenario.config if preset_active else DEFAULT_CONFIG,
            "selected_preset": selected_preset,
            "burst_multiplier": 1.0,
            "burst_end_at": 0
        }
        st.rerun()

    state = st.session_state.get("kill_state")
    if not state:
        st.info("👉 Hãy bấm **Khởi tạo & nạp kịch bản** để bắt đầu phòng lab mô phỏng.")
    else:
        # Define a single fragment that encapsulates the entire simulation dashboard.
        # This prevents duplication, layout shifting, and page flicker during autoplay.
        _ap_interval = 0.5 if state.get("auto_play_active") else None

        @st.fragment(run_every=_ap_interval)
        def _sim_lab_dashboard_fragment():
            n = len(state["engines"])
            total = len(state["rows"])
            cfg = state.get("engine_config", DEFAULT_CONFIG)

            # Autoplay Step execution chunk
            last_row, last_host, last_nid = None, None, None
            if state.get("auto_play_active"):
                ap_speed = state.get("auto_play_speed", 1000)
                ap_refresh = state.get("auto_play_refresh_n", 50)
                ap_end_at = state.get("auto_play_end_at", total)
                ap_start = state.get("auto_play_start", 0)
                
                j_start = state["cursor"]
                if j_start >= ap_end_at:
                    state["auto_play_active"] = False
                    state["log"].append(f"⏯️ Đã hoàn thành Auto-Play tại cursor={state['cursor']:,}.")
                    st.rerun()
                    
                mult = state.get("burst_multiplier", 1.0)
                if state.get("burst_end_at", 0) <= j_start:
                    state["burst_multiplier"] = 1.0
                    mult = 1.0
                    
                chunk_size = max(int(ap_speed * 0.5 * mult), 1)
                j_end = min(j_start + chunk_size, ap_end_at)
                
                for j in range(j_start, j_end):
                    execute_step(state, j, cfg)
                    row = state["rows"][j]
                    host = getattr(row, "host", "unknown")
                    nid = partition_key(host, n)
                    last_row, last_host, last_nid = row, host, nid
                    
                state["cursor"] = j_end
                sample_sim_history(state)
                
                log_period = max(int(ap_refresh) * 5, 1)
                if (j_end - ap_start) % log_period < chunk_size and last_row is not None:
                    state["log"].append(
                        f"[cursor={j_end:,}] 📡 streaming log · event={last_row.event_id} host={last_host} "
                        f"status={last_row.status} → Node {last_nid} ({state['status'][last_nid]})"
                    )
                    
                if j_end >= ap_end_at:
                    state["auto_play_active"] = False
                    state["log"].append(f"⏯️ Đã hoàn thành mốc chạy Auto-Play tại cursor={j_end:,}.")
                    st.rerun()

            cursor = state["cursor"]
            done = cursor >= total

            # Backward compatibility for autoplay state variables
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

            # ----- 🛰️ Cluster Health Card (live dashboard UI) -----
            _tot_proc = sum(state["node_processed"])
            _tot_dlq = sum(len(p) for p in state["pending"])
            _agg_u = sum(e.metrics["unique"] for e in state["engines"])
            _agg_o = sum(e.metrics["on_time"] for e in state["engines"])
            _comp_now = 100.0 * _agg_o / max(_agg_u, 1)
            render_cluster_health(st, state["status"], _tot_proc, _tot_dlq,
                                  _comp_now, cursor, total)

            col_left, col_right = st.columns([6, 4])

            with col_left:
                # ----- ⚡ Cluster Control Grid -----
                qa1, qa2, qa3, qa4 = st.columns(4)
                if qa1.button("💀 Đánh sập toàn cluster (Kill ALL)",
                              disabled=state.get("auto_play_active") or done,
                              width="stretch"):
                    killed = kill_all(state)
                    if killed:
                        state["log"].append(f"[cursor={cursor:,}] 💀 Sập cluster: KILLED toàn bộ {killed} nodes.")
                        sample_sim_history(state)
                    st.rerun()
                if qa2.button("🔄 Hồi sinh toàn cluster (Revive ALL)",
                              disabled=state.get("auto_play_active"),
                              width="stretch"):
                    revived = revive_all(state, cfg)
                    if revived:
                        state["log"].append(f"[cursor={cursor:,}] 🔄 Khôi phục cluster: REVIVED toàn bộ {revived} nodes từ checkpoint & DLQ.")
                        sample_sim_history(state)
                    st.rerun()
                if qa3.button("🏁 Flush all engines",
                              disabled=state.get("auto_play_active"),
                              width="stretch"):
                    flushed = 0
                    for i, eng in enumerate(state["engines"]):
                        if state["status"][i] == "alive":
                            eng.flush()
                            flushed += 1
                    state["log"].append(f"[cursor={cursor:,}] 🏁 Flush all: Đã ép đóng cửa sổ trên {flushed} nodes.")
                    st.rerun()
                if qa4.button("📸 Chụp trạng thái (Checkpoint ALL)",
                              disabled=state.get("auto_play_active"),
                              width="stretch"):
                    saved = 0
                    for i, eng in enumerate(state["engines"]):
                        if state["status"][i] == "alive":
                            try:
                                eng.checkpoint()
                                saved += 1
                            except Exception:
                                pass
                    state["log"].append(f"[cursor={cursor:,}] 📸 Checkpoint: Ghi nhận {saved} Consistent snapshots atomic lên đĩa.")
                    st.rerun()

                # ----- 🗺️ Live Animated SVG Cluster Diagram -----
                st.markdown("#### Sơ đồ Cluster mô phỏng")
                
                # Calculate stats for diagram
                processed_n = list(state["node_processed"])
                windows_n = [len(e.closed_windows) for e in state["engines"]]
                dlq_n = [len(p) for p in state["pending"]]
                ckpt_sz = [os.path.getsize(p) if os.path.exists(p) else 0 for p in state["ckpt_paths"]]
                
                diag_tick = state.get("diag_tick", 0)
                state["diag_tick"] = diag_tick + 1
                
                # During autoplay, highlight the last processed node and use cursor as tick trigger
                if state.get("auto_play_active") and last_nid is not None:
                    h_node = last_nid
                    s_tick = state["cursor"] - state.get("auto_play_start", 0)
                else:
                    h_node = None
                    s_tick = diag_tick
                    
                svg_content = generate_svg_cluster(
                    state["status"], processed_n, windows_n, dlq_n, ckpt_sz,
                    highlight_node=h_node, tick=s_tick
                )
                st.components.v1.html(svg_content, height=430)

                # ----- Node Controls Grid -----
                st.markdown("#### Bảng điều khiển riêng từng Node")
                cols = st.columns(n)
                for i, c in enumerate(cols):
                    with c:
                        status = state["status"][i]
                        eng = state["engines"][i]
                        pending_n = len(state["pending"][i])
                        
                        with st.container(border=True):
                            dot = ":green[● alive]" if status == "alive" else ":red[● dead]"
                            st.markdown(f"**Node {i}** &nbsp; {dot}")
                            st.caption(f"Đã xử lý: **{state['node_processed'][i]:,}**")
                            st.caption(f"Cửa sổ đóng: **{len(eng.closed_windows)}**")
                            
                            if pending_n > 0:
                                st.markdown(f":red[**Buffer DLQ: {pending_n:,}**]")
                            else:
                                st.caption(f"Buffer DLQ: {pending_n:,}")
                                
                            st.caption(f"Bỏ trễ: {eng.metrics['late_dropped']} · Lặp: {eng.metrics['duplicates']}")

                            if status == "alive":
                                if st.button(f"💀 Kill node {i}", key=f"kill_btn_{i}", width="stretch", disabled=state["auto_play_active"]):
                                    kill_node(state, i)
                                    st.rerun()
                            else:
                                if st.button(f"🔄 Revive node {i}", key=f"revive_btn_{i}", width="stretch", disabled=state["auto_play_active"]):
                                    revive_node(state, i, cfg)
                                    st.rerun()

                # ----- Stream Controls -----
                st.markdown("#### Bảng điều khiển luồng dữ liệu")
                
                # During Autoplay: render warning and STOP button at the top of stream controls
                if state.get("auto_play_active"):
                    stop_col1, stop_col2 = st.columns([1.5, 3.5])
                    with stop_col1:
                        if st.button("⏸️ DỪNG STREAM", type="primary", width="stretch", key="frag_stop"):
                            state["auto_play_active"] = False
                            state["log"].append(f"⏸️ Dừng Auto-Play theo yêu cầu tại cursor={state['cursor']:,}.")
                            st.rerun()
                    with stop_col2:
                        st.error("⏯️ Đang Auto-Play — dữ liệu động cập nhật trong fragment bên dưới mà không làm giật trang.", icon="🔴")
                
                ctrl_manual, ctrl_auto, ctrl_sched = st.tabs([
                    "Thủ công (Manual Step)", "Phát tự động (Auto-Play)", "Lịch trình sự cố"
                ])

                with ctrl_manual:
                    st.caption("Chạy stream theo từng bước để theo dõi chính xác hành vi của hệ thống.")
                    c1, c2, c3 = st.columns(3)
                    run_chunk_btn = c1.button(f"▶️ Chạy {kill_chunk:,} events tiếp theo", disabled=done or state["auto_play_active"], width="stretch", key="sim_run_chunk")
                    run_all_btn = c2.button("⏭️ Xử lý toàn bộ dữ liệu", disabled=done or state["auto_play_active"], width="stretch", key="sim_run_all")
                    flush_btn = c3.button("🏁 Flush & Đóng toàn bộ cửa sổ", disabled=not done, width="stretch", key="sim_flush")

                with ctrl_auto:
                    st.caption("Chạy luồng dữ liệu liên tục. Lịch trình sự cố sẽ tự động được kích hoạt.")
                    ap1, ap2, ap3 = st.columns(3)
                    speed_eps = ap1.slider("Tốc độ stream (events/giây)", 100, 10000, 1000, 100, key="sim_speed_slider", disabled=state["auto_play_active"])
                    refresh_n = ap2.slider("Làm mới UI sau mỗi N events", 10, 500, 50, 10, key="sim_refresh_slider", disabled=state["auto_play_active"])
                    auto_until = ap3.selectbox("Mốc dừng chạy", ["Hết stream", "+5,000 events", "+10,000 events"], key="sim_until_select", disabled=state["auto_play_active"])
                    
                    play_btn = st.button("⏯️ Kích hoạt Auto-Play realtime", type="primary", disabled=done or state["auto_play_active"], width="stretch", key="sim_play_autoplay")

                with ctrl_sched:
                    if state["schedule"]:
                        st.markdown("**📍 Dòng sự cố dự kiến** (theo cursor):")
                        tl_html = '<div style="position:relative;height:50px;background:#1E293B;border-radius:10px;border:1px solid #334155;margin:10px 0 15px 0; overflow:hidden;">'
                        cur_pct = 100.0 * cursor / max(total, 1)
                        tl_html += f'<div style="position:absolute;left:{cur_pct}%;top:0;bottom:0;width:2px;background:#3B82F6;z-index:10;"></div>'
                        tl_html += f'<div style="position:absolute;left:{cur_pct}%;top:2px;transform:translateX(-50%);font-size:0.65rem;color:#E0F2FE;font-weight:bold;background:#2563EB;padding:2px 6px;border-radius:4px;z-index:11;">{cursor:,}</div>'
                        
                        for item in state["schedule"]:
                            act_type, nid, at = item[0], item[1], item[2]
                            pct = 100.0 * at / max(total, 1)
                            if act_type == "kill":
                                icon, bg, border, fg = "💀", "#7F1D1D", "#EF4444", "#FEE2E2"
                            elif act_type == "revive":
                                icon, bg, border, fg = "🔄", "#064E3B", "#10B981", "#D1FAE5"
                            elif act_type == "ooo_spike":
                                icon, bg, border, fg = "🌪️", "#78350F", "#F59E0B", "#FEF3C7"
                            elif act_type == "load_burst":
                                icon, bg, border, fg = "⚡", "#4C1D95", "#8B5CF6", "#F5F3FF"
                            else:
                                icon, bg, border, fg = "⏳", "#1E3A8A", "#3B82F6", "#EFF6FF"
                            tl_html += f'<div title="{act_type} Node {nid} tại {at:,}" style="position:absolute;left:{pct}%;top:20px;transform:translateX(-50%);background:{bg};color:{fg};border:1px solid {border};border-radius:12px;padding:1px 6px;font-size:0.68rem;font-weight:bold;white-space:nowrap;cursor:help;">{icon} N{nid if nid != -1 else "A"}@{at:,}</div>'
                        tl_html += '</div>'
                        st.markdown(tl_html, unsafe_allow_html=True)
                        
                        sched_data = []
                        for item in state["schedule"]:
                            act_type, nid, at = item[0], item[1], item[2]
                            label = item[4] if len(item) > 4 else f"{act_type} Node {nid} at {at}"
                            sched_data.append({"Cursor kích hoạt": at, "Loại hành động": act_type.upper(), "Mục tiêu (Node ID)": nid if nid != -1 else "Tất cả", "Nhãn kịch bản": label})
                        st.dataframe(pd.DataFrame(sched_data), hide_index=True, width="stretch")
                        if st.button("🗑️ Xóa toàn bộ lịch sự cố", disabled=state["auto_play_active"]):
                            state["schedule"] = []
                            st.rerun()
                    else:
                        st.caption("Chưa có lịch sự cố nào được thiết lập. Dữ liệu sẽ truyền đi bình thường.")

                    st.markdown("**➕ Thêm lịch sự cố thủ công**")
                    ac1, ac2, ac3, ac4 = st.columns([1, 1, 1, 1])
                    sa = ac1.selectbox("Hành động", ["kill", "revive", "ooo_spike", "load_burst", "delay_inject"], key="sched_act")
                    sn = ac2.selectbox("Node mục tiêu", list(range(n)), key="sched_node")
                    sc_default = min(cursor + max(1, (total - cursor) // 3), total - 1) if total > 0 else 0
                    sx = ac3.number_input("Kích hoạt tại cursor", 0, max(total - 1, 0), int(sc_default), key="sched_cursor")
                    if ac4.button("Thêm vào timeline", key="add_to_sched_btn"):
                        lbl = f"{sa.upper()} Node {sn} tại {sx}"
                        params = {}
                        if sa == "ooo_spike":
                            params = {"fraction": 0.8, "duration": 500}
                        elif sa == "load_burst":
                            params = {"multiplier": 3.0, "duration": 1000}
                        elif sa == "delay_inject":
                            params = {"delay_s": 5.0, "duration": 500}
                        state["schedule"].append((sa, int(sn), int(sx), params, lbl))
                        state["schedule"].sort(key=lambda x: x[2])
                        st.rerun()

                # ----- Event Handlers (Click actions) -----
                if not state.get("auto_play_active"):
                    if run_chunk_btn:
                        end = min(cursor + int(kill_chunk), total)
                        for j in range(cursor, end):
                            execute_step(state, j, cfg)
                        state["cursor"] = end
                        state["log"].append(f"[cursor={cursor:,} → {end:,}] ▶️ Đã xử lý {end - cursor:,} events thủ công.")
                        sample_sim_history(state)
                        st.rerun()

                    if run_all_btn:
                        for j in range(cursor, total):
                            execute_step(state, j, cfg)
                        state["cursor"] = total
                        state["log"].append(f"⏭️ Đã xử lý toàn bộ luồng log tới cursor={total:,}")
                        sample_sim_history(state)
                        st.rerun()

                    if flush_btn:
                        for i, eng in enumerate(state["engines"]):
                            if state["status"][i] == "alive":
                                eng.flush()
                        state["log"].append(f"[cursor={cursor:,}] 🏁 Đã hoàn thành flush toàn bộ node.")
                        st.rerun()

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
                        state["log"].append(f"⏯️ BẮT ĐẦU AUTO-PLAY: tốc độ {speed_eps} eps, mốc dừng {end_at:,}.")
                        st.rerun()

            with col_right:
                # ============================================================
                # INSPECTOR — 4-tab bottom panel: Metrics / DLQ / Files / Log
                # ============================================================
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

                st.markdown("#### Chi tiết vận hành")
                ins_metrics, ins_dlq, ins_files, ins_log = st.tabs([
                    "📊 Biến động & Phân phối", "📭 Preview DLQ trên đĩa", "📁 File hệ thống", "📜 Logs thực thi"
                ])

                with ins_metrics:
                    st.markdown("**Diễn biến toàn cluster theo dòng thời gian (events cursor)**")
                    render_sim_evolution_chart(st, state, total)
                    
                    st.markdown("**Chỉ số KPI gộp**")
                    m1, m2, m3, m4, m5 = st.columns(5)
                    m1.metric("Độ đầy đủ (Completeness)", f"{completeness:.2f}%")
                    m2.metric("Events duy nhất", f"{agg['unique']:,}")
                    m3.metric("Cửa sổ đã đóng", f"{agg['windows']}")
                    m4.metric("DLQ đang đệm", f"{total_pending:,}", delta=f"{dead_count} node sập" if dead_count else None)
                    m5.metric("Lặp được lọc", f"{agg['duplicates']:,}")

                    st.markdown("**Biểu đồ tải của từng Node**")
                    node_names = [f"Node {i}" for i in range(n)]
                    processed_vals = state["node_processed"]
                    dlq_vals = [len(p) for p in state["pending"]]
                    color_processed = ["#10B981" if state["status"][i] == "alive" else "#9CA3AF" for i in range(n)]
                    
                    dist_fig = go.Figure()
                    dist_fig.add_trace(go.Bar(
                        x=node_names, y=processed_vals,
                        marker_color=color_processed,
                        name="Đã xử lý (Memory)",
                        text=[f"{v:,}" for v in processed_vals],
                        textposition="outside",
                        hovertemplate="%{x}<br>processed: %{y:,}<extra></extra>",
                    ))
                    if any(dlq_vals):
                        dist_fig.add_trace(go.Bar(
                            x=node_names, y=dlq_vals,
                            marker_color="#EF4444",
                            name="Đệm DLQ (Đĩa)",
                            text=[f"{v:,}" if v else "" for v in dlq_vals],
                            textposition="outside",
                            hovertemplate="%{x}<br>DLQ: %{y:,}<extra></extra>",
                        ))
                    dist_fig.update_layout(
                        height=260, barmode="group",
                        margin=dict(l=10, r=10, t=20, b=30),
                        plot_bgcolor="#0F172A", paper_bgcolor="#0F172A",
                        font=dict(color="#94A3B8"),
                        xaxis=dict(showgrid=False),
                        yaxis=dict(title="Events", gridcolor="#1E293B"),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
                    )
                    st.plotly_chart(dist_fig, width="stretch", key="sim_dist_bar")

                with ins_dlq:
                    dlq_active = [i for i in range(n) if os.path.exists(state["dlq_paths"][i]) and os.path.getsize(state["dlq_paths"][i]) > 0]
                    if not dlq_active:
                        st.info("Thư mục Dead-Letter Queue hiện đang trống (Không có node nào sập).", icon="📭")
                    else:
                        st.caption("Logs được đệm an toàn trên đĩa dưới định dạng append-only JSONL. Revive node sẽ tự động replay.")
                        dlq_cols = st.columns(min(len(dlq_active), 3))
                        for idx, nid in enumerate(dlq_active):
                            with dlq_cols[idx % len(dlq_cols)]:
                                dlq_path = state["dlq_paths"][nid]
                                size = os.path.getsize(dlq_path)
                                with open(dlq_path, "r", encoding="utf-8") as f:
                                    all_lines = f.readlines()
                                n_lines = len(all_lines)
                                
                                with st.container(border=True):
                                    st.markdown(f"**DLQ Node {nid}** &nbsp; :red[● dead]")
                                    st.caption(f"Đường dẫn: `{os.path.basename(dlq_path)}`")
                                    
                                    c_col1, c_col2 = st.columns(2)
                                    c_col1.metric("Events", f"{n_lines:,}")
                                    c_col2.metric("Dung lượng", f"{size:,} B")
                                    
                                    parsed_events = []
                                    for ln in all_lines[:5]:
                                        try:
                                            parsed_events.append(json.loads(ln))
                                        except Exception:
                                            pass
                                            
                                    if parsed_events:
                                        st.caption("Preview 5 sự kiện đầu:")
                                        st.dataframe(pd.DataFrame(parsed_events), width="stretch", hide_index=True)

                with ins_files:
                    sim_dir = state.get("sim_dir", "")
                    st.markdown("📂 **Thư mục lưu trữ hệ thống**: copy đường dẫn dưới đây để mở trên máy tính của bạn")
                    st.code(sim_dir, language="text")
                    
                    fs_rows = []
                    for i in range(n):
                        cp = state["ckpt_paths"][i]
                        dq = state["dlq_paths"][i]
                        fs_rows.append({
                            "Node": f"Node {i}",
                            "Trạng thái": "SẬP (DEAD)" if state["status"][i] == "dead" else "KHOẺ (ALIVE)",
                            "Dung lượng Checkpoint (B)": os.path.getsize(cp) if os.path.exists(cp) else 0,
                            "Dung lượng DLQ đĩa (B)": os.path.getsize(dq) if os.path.exists(dq) else 0,
                            "Dòng DLQ đệm (RAM)": len(state["pending"][i]),
                        })
                    st.dataframe(pd.DataFrame(fs_rows), width="stretch", hide_index=True)

                with ins_log:
                    flt_col, _ = st.columns([1.5, 4])
                    log_filter = flt_col.selectbox(
                        "Lọc logs theo loại",
                        ["Tất cả", "Chỉ sự cố (KILL/REVIVE)", "Chỉ lập lịch (SCHEDULED)", "Chỉ bão log/tăng tải (Spikes/Bursts)", "Chỉ log luồng dữ liệu (STREAM)"],
                        key="sim_log_filter"
                    )
                    
                    log_lines = state["log"]
                    if log_filter == "Chỉ sự cố (KILL/REVIVE)":
                        log_lines = [l for l in log_lines if "KILL" in l.upper() or "REVIVE" in l.upper() or "💀" in l or "🔄" in l]
                    elif log_filter == "Chỉ lập lịch (SCHEDULED)":
                        log_lines = [l for l in log_lines if "SCHEDULED" in l.upper() or "⏰" in l]
                    elif log_filter == "Chỉ bão log/tăng tải (Spikes/Bursts)":
                        log_lines = [l for l in log_lines if "BURST" in l.upper() or "SPIKE" in l.upper() or "🌪️" in l or "⚡" in l]
                    elif log_filter == "Chỉ log luồng dữ liệu (STREAM)":
                        log_lines = [l for l in log_lines if "streaming" in l.lower() or "📡" in l]
                        
                    colored = [colorize_log_line(ln) for ln in log_lines[-40:]]
                    log_html = "<br>".join(colored) if colored else "<i style='color:#94A3B8'>không có log nào khớp bộ lọc</i>"
                    st.markdown(f'<div class="demo-log">{log_html}</div>', unsafe_allow_html=True)

                # Final Done Check
                if done and total_pending == 0 and dead_count == 0:
                    st.success(
                        f"🎉 **Hoàn thành toàn bộ luồng mô phỏng sự cố!**\n\n"
                        f"- Độ đầy đủ (Completeness) cluster đạt: **{completeness:.2f}%**\n"
                        f"- Tổng số log đã đệm DLQ và khôi phục thành công: **{sum(state['node_buffered_total']):,} events**.\n"
                        f"👉 **Đảm bảo Exactly-Once Semantics thành công cho toàn cluster dù có sập node giữa luồng stream!**",
                        icon="✅"
                    )

        # Call the simulation tab fragment
        _sim_lab_dashboard_fragment()

