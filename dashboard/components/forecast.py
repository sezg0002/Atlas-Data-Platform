"""Forecast section component."""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from prophet import Prophet
from typing import Tuple

COLORS = {
    "primary": "#2E86DE",
    "success": "#1E8449",
    "muted": "#95A5A6",
}

CHART_CONFIG = {
    "plot_bgcolor": "#F9FBFD",
    "paper_bgcolor": "#F9FBFD",
    "font_color": "#2C3E50",
}


def render_forecast_section(country_code: str, df: pd.DataFrame):
    """Render forecast section with Prophet."""
    st.subheader("🔮 GDP Forecast with Prophet")

    col1, col2 = st.columns([3, 1])

    with col2:
        periods = st.slider("Years to forecast", min_value=1, max_value=10, value=5)

    with st.spinner("Generating forecast..."):
        forecast, model, metrics = _generate_forecast(df, periods)

    if forecast is not None:
        _render_forecast_chart(forecast, model, country_code)
        _render_metrics(metrics)
        _render_forecast_table(forecast)


def _generate_forecast(df: pd.DataFrame, periods: int) -> Tuple:
    """Generate Prophet forecast."""
    try:
        prophet_df = df[["date", "value"]].copy()
        prophet_df.columns = ["ds", "y"]
        prophet_df["ds"] = pd.to_datetime(prophet_df["ds"])

        # Convert Decimal to float to avoid type errors
        prophet_df["y"] = prophet_df["y"].astype(float)

        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
            changepoint_prior_scale=0.05
        )
        model.fit(prophet_df)

        future = model.make_future_dataframe(periods=periods, freq="Y")
        forecast = model.predict(future)

        # Calculate metrics
        merged = prophet_df.merge(forecast[["ds", "yhat"]], on="ds")
        mae = (merged["y"] - merged["yhat"]).abs().mean()
        rmse = ((merged["y"] - merged["yhat"]) ** 2).mean() ** 0.
        5
        mape = ((merged["y"] - merged["yhat"]).abs() / merged["y"]).mean() * 100

        metrics = {"MAE": mae, "RMSE": rmse, "MAPE": f"{mape:.2f}%"}

        return forecast, model, metrics

    except Exception as e:
        st.error(f"Forecast error: {e}")
        return None, None, None


def _render_forecast_chart(forecast: pd.DataFrame, model: Prophet, country_code: str):
    """Render forecast chart."""
    fig = go.Figure()

    # Historical + Forecast
    fig.add_trace(go.Scatter(
        x=forecast["ds"],
        y=forecast["yhat"],
        mode="lines",
        name="Forecast",
        line=dict(color=COLORS["primary"])
    ))

    # Confidence interval
    fig.add_trace(go.Scatter(
        x=forecast["ds"].tolist() + forecast["ds"].tolist()[::-1],
        y=forecast["yhat_upper"].tolist() + forecast["yhat_lower"].tolist()[::-1],
        fill="toself",
        fillcolor="rgba(46, 134, 222, 0.2)",
        line=dict(color="rgba(255,255,255,0)"),
        name="Confidence Interval"
    ))

    fig.update_layout(
        title=f"GDP Forecast - {country_code}",
        xaxis_title="Date",
        yaxis_title="GDP (USD)",
        title_x=0.5,
        plot_bgcolor=CHART_CONFIG["plot_bgcolor"],
        paper_bgcolor=CHART_CONFIG["paper_bgcolor"],
        font=dict(color=CHART_CONFIG["font_color"]),
        hovermode="x unified"
    )

    st.plotly_chart(fig, use_container_width=True)


def _render_metrics(metrics: dict):
    """Render model metrics."""
    if metrics:
        st.markdown("**📊 Model Metrics**")
        col1, col2, col3 = st.columns(3)
        col1.metric("MAE", f"{metrics['MAE']:,.0f}")
        col2.metric("RMSE", f"{metrics['RMSE']:,.0f}")
        col3.metric("MAPE", metrics['MAPE'])


def _render_forecast_table(forecast: pd.DataFrame):
    """Render forecast table."""
    with st.expander("📋 Forecast Table"):
        display_df = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(10).copy()
        display_df.columns = ["Year", "Forecast", "Min", "Max"]
        display_df["Year"] = display_df["Year"].dt.year

        for col in ["Forecast", "Min", "Max"]:
            display_df[col] = display_df[col].apply(lambda x: f"{x:,.0f}")

        st.dataframe(display_df, use_container_width=True, hide_index=True)