"""
Atlas Data Platform - Main Dashboard Application
"""

import os
import sys

# Setup paths BEFORE any imports
sys.path.insert(0, os.path.dirname(os. path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st

from config_dash import PAGE_CONFIG
from database import check_connection, load_domain_data
from components.header import render_header, render_connection_error, render_no_data_warning
from components.sidebar import render_sidebar
from components.kpis import render_kpis
from components.charts import render_historical_chart, render_yoy_growth_chart
from components. statistics import render_statistics
from components.forecast import render_forecast_section
from components.footer import render_footer


def main():
    """Main application entry point."""

    st.set_page_config(**PAGE_CONFIG)

    render_header()

    if not check_connection():
        render_connection_error()
        return

    domain, country_code = render_sidebar()

    df = load_domain_data(domain, country_code)

    if df.empty:
        render_no_data_warning()
        return

    render_kpis(df)

    st.markdown("---")

    tab_overview, tab_analysis, tab_forecast = st.tabs([
        "📊 Overview",
        "📈 Detailed Analysis",
        "🔮 Forecasts"
    ])

    with tab_overview:
        render_historical_chart(df, domain, country_code)
        render_statistics(df)

    with tab_analysis:
        if domain == "economy" and country_code:
            render_yoy_growth_chart(df, country_code)
        else:
            st.info("💡 Select a country to view detailed analysis.")

    with tab_forecast:
        if domain == "economy" and country_code:
            render_forecast_section(country_code, df)
        else:
            st.info("🔮 Forecasts are available for economic data.")

    render_footer()


if __name__ == "__main__":
    main()