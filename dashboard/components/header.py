"""Header component."""

import streamlit as st

COLORS = {
    "dark": "#2C3E50",
    "secondary": "#5D6D7E",
}


def render_header():
    """Render the application header."""
    st.markdown(
        f"""
        <div style='text-align: center; padding: 1rem 0;'>
            <h1 style='color: {COLORS["dark"]}; margin-bottom: 0.5rem;'>
                🌍 Atlas Data Platform
            </h1>
            <p style='color: {COLORS["secondary"]}; font-size: 1.1rem;'>
                Data Engineering, Analytics & Forecast
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )


def render_connection_error():
    """Render connection error message."""
    st. error("❌ Impossible de se connecter à la base de données.")
    st.info("💡 Assurez-vous que PostgreSQL est démarré : `docker compose up -d`")
    st.stop()


def render_no_data_warning():
    """Render no data warning."""
    st.warning("⚠️ Aucune donnée disponible.  Lancez le pipeline ETL :")
    st.code("python -m etl.run_etl", language="bash")
    st. stop()