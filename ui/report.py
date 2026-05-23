# -*- coding: utf-8 -*-
import os
import streamlit as st

def render_report():
    report_path = "docs/SYSTEM_REPORT.md"

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
