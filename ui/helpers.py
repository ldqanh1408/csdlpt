# -*- coding: utf-8 -*-
import os
import json
import streamlit as st
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import matplotlib.pyplot as plt

from wm import kill_node, revive_node
from wm.partition import partition_key

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
