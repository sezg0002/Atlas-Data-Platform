"""KPI cards component."""

import streamlit as st
import pandas as pd


def format_number(value: float, decimals: int = 0) -> str:
    """Format number with thousand separators."""
    if pd.isna(value):
        return "N/A"
    return f"{value:,. {decimals}f}"


def format_percentage(value: float, decimals: int = 1) -> str:
    """Format percentage with sign."""
    if pd.isna(value):
        return "N/A"
    return f"{value:+.{decimals}f}%"


def render_kpis(df: pd.DataFrame):
    """Render KPI cards."""
    if df.empty:
        return

    values = df["value"]
    latest = values.iloc[-1] if len(values) > 0 else 0
    previous = values.iloc[-2] if len(values) > 1 else latest
    delta = ((latest - previous) / previous * 100) if previous != 0 else 0

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="📊 Dernière valeur",
            value=format_number(latest),
            delta=format_percentage(delta)
        )

    with col2:
        st.metric(label="📈 Moyenne", value=format_number(values.mean()))

    with col3:
        st.metric(label="⬇️ Minimum", value=format_number(values.min()))

    with col4:
        st.metric(label="⬆️ Maximum", value=format_number(values.max()))