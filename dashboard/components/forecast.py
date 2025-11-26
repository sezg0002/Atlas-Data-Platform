"""Prophet forecast section component."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go

COLORS = {
    "primary": "#2E86DE",
    "success": "#1E8449",
    "dark": "#2C3E50",
}

CHART_CONFIG = {
    "plot_bgcolor": "#F9FBFD",
    "paper_bgcolor": "#F9FBFD",
}


def render_forecast_section(country_code: str, df: pd.DataFrame):
    """Render the forecast section with Prophet."""
    st.markdown("---")
    st.subheader("🔮 Prévisions GDP (Prophet)")

    col1, col2 = st.columns([2, 1])

    with col1:
        periods = st.slider("Années à prévoir", min_value=1, max_value=10, value=5)

    with col2:
        st.info(f"📊 Prévision **{country_code}**")

    try:
        from ml.forecast_gdp import forecast_gdp

        with st.spinner("🔄 Calcul des prévisions..."):
            forecast = forecast_gdp(country_code, periods)

        _render_forecast_chart(forecast, country_code, df)
        _render_forecast_table(forecast)

    except ImportError:
        st.warning("⚠️ **Prophet non installé. ** `pip install prophet`")
    except Exception as e:
        st.error(f"❌ Erreur: {e}")


def _render_forecast_chart(forecast: pd.DataFrame, country_code: str, historical_df: pd.DataFrame):
    """Render forecast chart."""
    fig = go.Figure()

    # Historical
    fig.add_trace(go.Scatter(
        x=historical_df["date"],
        y=historical_df["value"],
        mode="lines+markers",
        name="Historique",
        line=dict(color=COLORS["primary"], width=2)
    ))

    # Forecast
    forecast_only = forecast[forecast["ds"] > historical_df["date"].max()]

    fig.add_trace(go.Scatter(
        x=forecast_only["ds"],
        y=forecast_only["yhat"],
        mode="lines+markers",
        name="Prévision",
        line=dict(color=COLORS["success"], width=2, dash="dash")
    ))

    # Confidence interval
    fig.add_trace(go.Scatter(
        x=pd.concat([forecast_only["ds"], forecast_only["ds"][::-1]]),
        y=pd.concat([forecast_only["yhat_upper"], forecast_only["yhat_lower"][::-1]]),
        fill="toself",
        fillcolor="rgba(30, 132, 73, 0.15)",
        line=dict(color="rgba(255,255,255,0)"),
        name="IC 95%",
        hoverinfo="skip"
    ))

    fig.update_layout(
        title=f"Prévision GDP - {country_code}",
        title_x=0.5,
    xaxis_title = "Année",
    yaxis_title = "GDP per capita (USD)",
    plot_bgcolor = CHART_CONFIG["plot_bgcolor"],
    paper_bgcolor = CHART_CONFIG["paper_bgcolor"],
    hovermode = "x unified",
    legend = dict(orientation="h", yanchor="bottom", y=1.02, x=1, xanchor="right")
    )

    st.plotly_chart(fig, use_container_width=True)


def _render_forecast_table(forecast: pd.DataFrame):
    """Render forecast table."""
    with st.expander("📋 Tableau des prévisions"):
        display_df = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(10).copy()
        display_df.columns = ["Année", "Prévision", "Min", "Max"]
        display_df["Année"] = display_df["Année"].dt.year

        for col in ["Prévision", "Min", "Max"]:
            display_df[col] = display_df[col].apply(lambda x: f"{x:,. 0f}")

        st.dataframe(display_df, use_container_width=True, hide_index=True)