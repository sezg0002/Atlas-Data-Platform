"""Footer component."""

import streamlit as st
from datetime import datetime

COLORS = {
    "muted": "#95A5A6",
}


def render_footer():
    """Render footer."""
    st.markdown("---")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(
            f"<p style='color:{COLORS['muted']};font-size:0. 85rem;'>"
            f"🎓 Atlas Data Platform</p>",
            unsafe_allow_html=True
        )

    with col2:
        st.markdown(
            f"<p style='text-align:center;color:{COLORS['muted']};font-size:0. 85rem;'>"
            f"Streamlit • dbt • Prophet</p>",
            unsafe_allow_html=True
        )

    with col3:
        st.markdown(
            f"<p style='text-align:right;color:{COLORS['muted']};font-size:0.85rem;'>"
            f"© {datetime.now().year} Harun SEZGIN</p>",
            unsafe_allow_html=True
        )