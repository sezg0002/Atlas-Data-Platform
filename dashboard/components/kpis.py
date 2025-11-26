"""KPI cards component."""

import streamlit as st
import pandas as pd
from typing import Optional

from utils.formatters import format_number, format_percentage


def render_kpis(df: pd.DataFrame):
    """Render KPI cards."""
    if df.empty:
        return

    # Calculate metrics
    metrics = _calculate_metrics(df)

    # Render 4 columns
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="📊 Dernière valeur",
            value=format_number(metrics["latest"]),
            delta=format_percentage(metrics["delta"])
        )

    with col2:
        st.metric(
            label="📈 Moyenne",
            value=format_number(metrics["mean"])
        )

    with col3:
        st.metric(
            label="⬇️ Minimum",
            value=format_number(metrics["min"])
        )

    with col4:
        st.metric(
            label="⬆️ Maximum",
            value=format_number(metrics["max"])
        )


def _calculate_metrics(df: pd.DataFrame) -> dict:
    """Calculate KPI metrics from dataframe."""
    values = df["value"]

    latest = values.iloc[-1] if len(values) > 0 else 0
    previous = values.iloc[-2] if len(values) > 1 else latest

    delta = ((latest - previous) / previous * 100) if previous != 0 else 0

    return {
        "latest": latest,
        "mean": values.mean(),
        "min": values.min(),
        "max": values.max(),
        "delta": delta,
        "count": len(values),
    }


def render_mini_kpis(df: pd.DataFrame, cols: int = 6):
    """Render compact mini KPIs."""
    if df.empty:
        return

    metrics = _calculate_metrics(df)
    columns = st.columns(cols)

    kpi_items = [
        ("Dernier", metrics["latest"]),
        ("Moyenne", metrics["mean"]),
        ("Médiane", df["value"].median()),
        ("Écart-type", df["value"].std()),
        ("Min", metrics["min"]),
        ("Max", metrics["max"]),
    ]

    for col, (label, value) in zip(columns, kpi_items):
        with col:
            st.markdown(
                f"<div style='text-align:center;'>"
                f"<span style='color:#5D6D7E;font-size:0.8rem;'>{label}</span><br>"
                f"<span style='font-size:1.1rem;font-weight:bold;'>{format_number(value)}</span>"
                f"</div>",
                unsafe_allow_html=True
            )