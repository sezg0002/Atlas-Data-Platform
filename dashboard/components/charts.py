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
        st.warning("No data to display.")
        return

    st.subheader("📈 Historical Trends")

    fig = px.line(
        df,
        x="date",
        y="value",
        title=f"{domain.capitalize()} Trends - {country_code or 'Global'}",
        color_discrete_sequence=[COLORS["primary"]],
        markers=True
    )

    fig.update_layout(
        xaxis_title="Date",
        yaxis_title=df["unit"].iloc[0] if "unit" in df.columns else "Value",
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
    if df.empty:
        st.warning("No data to display.")
        return

    st.subheader("📊 Year-over-Year Growth")

    df_yearly = df.copy()
    df_yearly["year"] = pd.to_datetime(df_yearly["date"]).dt.year
    yearly_data = df_yearly.groupby("year")["value"].mean().reset_index()
    yearly_data["yoy_growth"] = yearly_data["value"].pct_change() * 100

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=yearly_data["year"],
        y=yearly_data["yoy_growth"],
        marker_color=[COLORS["success"] if v >= 0 else COLORS["danger"] for v in yearly_data["yoy_growth"]],
        text=[f"{v:.1f}%" if pd.notna(v) else "" for v in yearly_data["yoy_growth"]],
        textposition="outside"
    ))

    fig.update_layout(
        title=f"YoY Growth - {country_code}",
        xaxis_title="Year",
        yaxis_title="Growth (%)",
        title_x=0.5,
        plot_bgcolor=CHART_CONFIG["plot_bgcolor"],
        paper_bgcolor=CHART_CONFIG["paper_bgcolor"],
        font=dict(color=CHART_CONFIG["font_color"]),
    )

    st.plotly_chart(fig, use_container_width=True)