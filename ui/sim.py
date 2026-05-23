# -*- coding: utf-8 -*-
import os
import json
import time
import numpy as np
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from wm import (WatermarkEngine, generate_logs, load_nasa_csv,
                DEFAULT_CONFIG, PRESETS,
                kill_node, revive_node, kill_all, revive_all)
from wm.partition import partition_key

from ui.helpers import (
    generate_svg_cluster,
    execute_step,
    render_cluster_health,
    render_sim_evolution_chart,
    sample_sim_history,
    colorize_log_line
)

def render_sim():
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

    cc1_m, cc2_m = st.columns(2)
    kill_wm_mode = cc1_m.selectbox(
        "Chế độ Watermark (Đóng sổ cuối luồng)",
        ["Strict Watermark (Dùng EOS Barrier)", "Heuristic Watermark (Dùng Idle Timeout)"],
        key="kill_wm_mode"
    )

    cinit, _, creset = st.columns([1, 4, 1])
    init_btn = cinit.button("🏗️ Khởi tạo & nạp kịch bản", type="primary", key="sim_init_btn")
    reset_btn = creset.button("🗑️ Reset phòng lab", key="sim_reset_btn")

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
        
        # Construct rows and append N EOS markers if Strict Watermark is selected
        rows_list = list(df_k.itertuples(index=False))
        if kill_wm_mode == "Strict Watermark (Dùng EOS Barrier)":
            eos_hosts = {}
            for i in range(kill_nodes):
                suffix = 0
                while True:
                    host_candidate = f"EOS_HOST_{i}_{suffix}"
                    if partition_key(host_candidate, kill_nodes) == i:
                        eos_hosts[i] = host_candidate
                        break
                    suffix += 1
            
            from collections import namedtuple
            EOSRow = namedtuple("EOSRow", ["event_id", "event_time", "status", "host", "endpoint"])
            
            for i in range(kill_nodes):
                eos_host = eos_hosts[i]
                eos_row = EOSRow(
                    event_id=f"EOS_MARKER_{i}",
                    event_time=df_k["event_time"].max() + 1000.0 if not df_k.empty else 1_700_000_000.0,
                    status="EOS",
                    host=eos_host,
                    endpoint=eos_host
                )
                rows_list.append(eos_row)

        st.session_state.kill_state = {
            "engines": engines,
            "ckpt_paths": ckpt_paths,
            "dlq_paths": dlq_paths,
            "sim_dir": sim_dir,
            "ckpt_dir": ckpt_dir,
            "dlq_dir": dlq_dir,
            "status": ["alive"] * kill_nodes,
            "rows": rows_list,
            "cursor": 0,
            "pending": [[] for _ in range(kill_nodes)],
            "node_processed": [0] * kill_nodes,
            "node_buffered_total": [0] * kill_nodes,
            "log": [
                f"🏗️ Khởi tạo cluster {kill_nodes} node với {len(rows_list):,} events",
                f"🎬 Kịch bản: {selected_preset}",
                f"🔒 Chế độ Watermark: {kill_wm_mode}",
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
            "burst_end_at": 0,
            "kill_wm_mode": kill_wm_mode
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
                    if state.get("kill_wm_mode") == "Heuristic Watermark (Dùng Idle Timeout)" and j_end >= total:
                        flushed_nodes = []
                        for i, eng in enumerate(state["engines"]):
                            if state["status"][i] == "alive":
                                eng.flush()
                                flushed_nodes.append(str(i))
                        state["log"].append(
                            f"[cursor={j_end:,}] ⏱️ IDLE TIMEOUT: Nhàn rỗi quá 3 giây (Hệ thống hết event) -> Tự động local flush() chốt sổ các node: {', '.join(flushed_nodes)}."
                        )
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
                              key="sim_kill_all_btn",
                              width="stretch"):
                    killed = kill_all(state)
                    if killed:
                        state["log"].append(f"[cursor={cursor:,}] 💀 Sập cluster: KILLED toàn bộ {killed} nodes.")
                        sample_sim_history(state)
                    st.rerun()
                if qa2.button("🔄 Hồi sinh toàn cluster (Revive ALL)",
                              disabled=state.get("auto_play_active"),
                              key="sim_revive_all_btn",
                              width="stretch"):
                    revived = revive_all(state, cfg)
                    if revived:
                        state["log"].append(f"[cursor={cursor:,}] 🔄 Khôi phục cluster: REVIVED toàn bộ {revived} nodes từ checkpoint & DLQ.")
                        sample_sim_history(state)
                    st.rerun()
                if qa3.button("🏁 Flush all engines",
                              disabled=state.get("auto_play_active"),
                              key="sim_flush_all_btn",
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
                              key="sim_ckpt_all_btn",
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
                        if st.button("🗑️ Xóa toàn bộ lịch sự cố", disabled=state["auto_play_active"], key="clear_timeline_btn"):
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
                        if state.get("kill_wm_mode") == "Heuristic Watermark (Dùng Idle Timeout)" and end >= total:
                            flushed_nodes = []
                            for i, eng in enumerate(state["engines"]):
                                if state["status"][i] == "alive":
                                    eng.flush()
                                    flushed_nodes.append(str(i))
                            state["log"].append(
                                f"[cursor={end:,}] ⏱️ IDLE TIMEOUT: Nhàn rỗi quá 3 giây (Hệ thống hết event) -> Tự động local flush() chốt sổ các node: {', '.join(flushed_nodes)}."
                            )
                        sample_sim_history(state)
                        st.rerun()

                    if run_all_btn:
                        for j in range(cursor, total):
                            execute_step(state, j, cfg)
                        state["cursor"] = total
                        state["log"].append(f"⏭️ Đã xử lý toàn bộ luồng log tới cursor={total:,}")
                        if state.get("kill_wm_mode") == "Heuristic Watermark (Dùng Idle Timeout)":
                            flushed_nodes = []
                            for i, eng in enumerate(state["engines"]):
                                if state["status"][i] == "alive":
                                    eng.flush()
                                    flushed_nodes.append(str(i))
                            state["log"].append(
                                f"[cursor={total:,}] ⏱️ IDLE TIMEOUT: Nhàn rỗi quá 3 giây (Hệ thống hết event) -> Tự động local flush() chốt sổ các node: {', '.join(flushed_nodes)}."
                            )
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
