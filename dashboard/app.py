"""
Atlas Data Platform - Main Dashboard Application

A modular Streamlit dashboard for data visualization and forecasting.
"""

import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import streamlit as st

# Local imports
from config_dash import PAGE_CONFIG
from database import check_connection, load_domain_data

from components import (
    render_header,
    render_sidebar,
    render_kpis,
    render_historical_chart,
    render_statistics,
    render_forecast_section,
    render_footer,
)
from components.header import render_connection_error, render_no_data_warning
from components.charts import render_yoy_growth_chart


def main():
    """Main application entry point."""

    # --- PAGE CONFIGURATION ---
    st.set_page_config(**PAGE_CONFIG)

    # --- HEADER ---
    render_header()

    # --- CHECK DATABASE CONNECTION ---
    if not check_connection():
        render_connection_error()
        return

    # --- SIDEBAR (returns filters) ---
    domain, country_code = render_sidebar()

    # --- LOAD DATA ---
    df = load_domain_data(domain, country_code)

    if df.empty:
        render_no_data_warning()
        return

    # --- KPIs ---
    render_kpis(df)

    st.markdown("---")

    # --- TABS FOR ORGANIZATION ---
    tab_overview, tab_analysis, tab_forecast = st.tabs([
        "📊 Vue d'ensemble",
        "📈 Analyse détaillée",
        "🔮 Prévisions"
    ])

    with tab_overview:
        # Historical chart
        render_historical_chart(df, domain, country_code)

        # Statistics
        render_statistics(df)

    with tab_analysis:
        if domain == "economy" and country_code:
            # Year-over-year growth
            render_yoy_growth_chart(df, country_code)

        # Additional analysis could go here
        st.info("💡 Plus d'analyses seront ajoutées prochainement.")

    with tab_forecast:
        if domain == "economy" and country_code:
            render_forecast_section(country_code, df)
        else:
            st.info("🔮 Les prévisions sont disponibles uniquement pour les données économiques.")

    # --- FOOTER ---
    render_footer()


if __name__ == "__main__":
    main()