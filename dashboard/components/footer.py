"""Footer component."""

import streamlit as st
from datetime import datetime

from config import COLORS


def render_footer():
    """Render the application footer."""
    st.markdown("---")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(
            f"<p style='color:{COLORS['muted']};font-size:0. 85rem;'>"
            f"🎓 Atlas Data Platform - Projet étudiant"
            f"</p>",
            unsafe_allow_html=True
        )

    with col2:
        st.markdown(
            f"<p style='text-align:center;color:{COLORS['muted']};font-size:0. 85rem;'>"
            f"📊 Powered by Streamlit, dbt, Prophet"
            f"</p>",
            unsafe_allow_html=True
        )

    with col3:
        st.markdown(
            f"<p style='text-align:right;color:{COLORS['muted']};font-size:0.85rem;'>"
            f"© {datetime.now().year} Harun SEZGIN"
            f"</p>",
            unsafe_allow_html=True
        )


def render_debug_info():
    """Render debug information (dev mode only)."""
    with st.expander("🔧 Debug Info"):
        st.write("**Session State:**")
        st.json(dict(st.session_state))

        st.write("**Cache Info:**")
        st.write(f"- Data cache TTL: 3600s")