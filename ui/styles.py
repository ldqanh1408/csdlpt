# -*- coding: utf-8 -*-
import os
import streamlit as st

def inject_styles():
    # Global custom CSS
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

    # Sidebar Content
    with st.sidebar:
        st.image("https://img.icons8.com/clouds/150/000000/water.png", width=90)
        st.markdown("### 💧 Watermark Tracker")
        st.caption("Project 112 · Nhóm 112 · CSDL Phân tán")
        st.markdown("---")

        st.markdown("#### 📖 Tài liệu chính thức")
        st.markdown("- [README.md](file:///D:/dev/csdlpt/README.md)")
        st.markdown("- [INDEX.md (Mục lục)](file:///D:/dev/csdlpt/docs/INDEX.md)")
        st.markdown("- [DESIGN.md (Kiến trúc)](file:///D:/dev/csdlpt/docs/DESIGN.md)")
        st.markdown("- [SYSTEM_REPORT.md (Báo cáo)](file:///D:/dev/csdlpt/docs/SYSTEM_REPORT.md)")
        st.markdown("- [GLOSSARY.md (Thuật ngữ)](file:///D:/dev/csdlpt/docs/GLOSSARY.md)")
        st.markdown("- [EOS_MARKER.md (EOS Barrier)](file:///D:/dev/csdlpt/docs/EOS_MARKER.md)")
        st.markdown("- [INFRASTRUCTURE.md (So sánh hạ tầng)](file:///D:/dev/csdlpt/docs/INFRASTRUCTURE.md)")
        st.markdown("- [PERFORMANCE.md (Tối ưu hiệu năng)](file:///D:/dev/csdlpt/docs/PERFORMANCE.md)")

        st.markdown("---")
        st.markdown("#### ⏳ Tài liệu đang xem xét")
        st.markdown("- [PROPOSAL.md (Đề xuất)](file:///D:/dev/csdlpt/pending/PROPOSAL.md)")
        st.markdown("- [REPORT.md (Báo cáo cũ nháp)](file:///D:/dev/csdlpt/pending/REPORT.md)")
        st.markdown("- [ARCHITECTURE.md (Kiến trúc nháp)](file:///D:/dev/csdlpt/pending/ARCHITECTURE.md)")


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

    # Hero Header Content
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
