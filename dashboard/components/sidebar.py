"""Sidebar filters component."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
from typing import Tuple, Optional

from database import get_available_countries, get_data_summary, clear_cache

# Finance (global market data) is always available; the economy domain depends
# on the World Bank ingestion, so finance is surfaced first as the default view.
DEFAULTS = {
    "countries": ["FRA", "USA", "DEU"],
    "domains": ["finance", "economy"],
}

DOMAIN_LABELS = {
    "economy": "Economy",
    "finance": "Finance",
}


def render_sidebar() -> Tuple[str, Optional[str]]:
    """Render sidebar with filters.  Returns (domain, country_code)."""
    with st.sidebar:
        st. markdown(
            """
            <div style='text-align: center; padding: 0.5rem 0 1rem 0;'>
                <span style='font-size: 2rem;'>🌍</span>
                <p style='margin: 0; font-weight: bold; color: #2C3E50;'>Atlas</p>
            </div>
            """,
            unsafe_allow_html=True
        )

        st.header("🎛️ Filters")

        domain = st.selectbox(
            "Domain",
            options=DEFAULTS["domains"],
            format_func=lambda x: DOMAIN_LABELS.get(x, x.capitalize()),
            help="Select the data type"
        )

        country_code = None
        if domain == "economy":
            countries = get_available_countries()
            country_code = st.selectbox("Country", options=countries)
        else:
            st.caption("📊 Global financial data (SPY).")

        st.divider()

        # Data summary
        summary = get_data_summary()
        if summary:
            st.markdown("**📈 Summary**")
            col1, col2 = st. columns(2)
            with col1:
                st.caption(f"📁 {summary. get('total_records', 0):,}")
            with col2:
                st.caption(f"🌍 {summary.get('countries_count', 0)} countries")

        st.divider()

        # Refresh button
        if st.button("🔄 Refresh", use_container_width=True):
            clear_cache()
            st.rerun()

        st.info("💡 Run ETL if data is empty.", icon="ℹ️")

    return domain, country_code