# -*- coding: utf-8 -*-
"""
app.py — Dashboard đa chức năng của Distributed Watermark Tracker
------------------------------------------------------------------
Chạy: streamlit run app.py
Một lệnh duy nhất hiển thị TOÀN BỘ giao diện trình bày dự án + cấu hình +
mô phỏng dựa trên dataset, KHÔNG cần gõ thêm lệnh nào trong terminal.
"""
import streamlit as st
from ui import (
    inject_styles,
    render_overview,
    render_report,
    render_stream,
    render_sweep,
    render_demos,
    render_dist,
    render_sim
)

# CẤU HÌNH TRANG
st.set_page_config(
    page_title="Distributed Watermark Tracker · Dashboard",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded",
)

# INJECT STYLES AND SIDEBAR
inject_styles()

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

with tab_overview:
    render_overview()

with tab_report:
    render_report()

with tab_stream:
    render_stream()

with tab_sweep:
    render_sweep()

with tab_demos:
    render_demos()

with tab_dist:
    render_dist()

with tab_sim:
    render_sim()
