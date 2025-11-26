"""Chart components."""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Optional

COLORS = {
    "primary": "#2E86DE",
    "success": "#1E8449",
    "danger": "#E74C3C",
    "muted": "#95A5A6",
}

CHART_CONFIG = {
    "plot_bgcolor": "#F9FBFD",
    "paper_bgcolor": "#F9FBFD",
    "font_color": "#2C3E50",
}


def render_historical_chart(df: pd.DataFrame, domain: str, country_code: Optional[str] = None):
    """Render historical trends chart."""
    if df.empty:
        st.warning("Pas de données à afficher.")
        return

    st.subheader("📈 Évolution historique")

    fig = px.line(
        df,
        x="date",
        y="value",
        title=f"Tendances {domain.capitalize()} - {country_code or 'Global'}",
        color_discrete_sequence=[COLORS["primary"]],
        markers=True
    )

    fig.update_layout(
        xaxis_title="Date",
        yaxis_title=df["unit"].iloc[0] if "unit" in df.columns else "Valeur",
        title_x=0.5,
        plot_bgcolor=CHART_CONFIG["plot_bgcolor"],
        paper_bgcolor=CHART_CONFIG["paper_bgcolor"],
        font=dict(color=CHART_CONFIG["font_color"]),
        hovermode="x unified"
    )

    fig.update_xaxes(rangeslider_visible=True, rangeslider_thickness=0.05)

    st.plotly_chart(fig, use_container_width=True)


def render_yoy_growth_chart(df: pd.DataFrame, country_code: str):
    """Render year-over-year growth chart."""
    if df.empty or len(df) < 2:
        return

    df_copy = df.copy()
    df_copy["year"] = pd.to_datetime(df_copy["date"]).dt.year
    df_copy["prev_value"] = df_copy["value"].shift(1)
    df_copy["yoy_growth"] = (
            (df_copy["value"] - df_copy["prev_value"]) / df_copy["prev_value"] * 100
    )
    df_copy = df_copy.dropna(subset=["yoy_growth"])

    if df_copy.empty:
        return

    st.subheader("📊 Croissance annuelle")

    df_copy["color"] = df_copy["yoy_growth"].apply(
        lambda x: COLORS["success"] if x >= 0 else COLORS["danger"]
    )

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df_copy["year"],
        y=df_copy["yoy_growth"],
        marker_color=df_copy["color"],
        text=df_copy["yoy_growth"].round(1).astype(str) + "%",
        textposition="outside"
    ))

    fig.update_layout(
        title=f"Croissance YoY - {country_code}",
        title_x=0.5,
    xaxis_title = "Année",
    yaxis_title = "Croissance (%)",
    plot_bgcolor = CHART_CONFIG["plot_bgcolor"],
    paper_bgcolor = CHART_CONFIG["paper_bgcolor"],
    )

    fig.add_hline(y=0, line_dash="dash", line_color=COLORS["muted"])

    st.plotly_chart(fig, use_container_width=True)