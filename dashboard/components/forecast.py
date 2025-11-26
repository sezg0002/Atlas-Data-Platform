"""Prophet forecast section component."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from typing import Optional

from config import COLORS, CHART_CONFIG, DEFAULTS


def render_forecast_section(country_code: str, df: pd.DataFrame):
    """Render the complete forecast section with Prophet."""
    st.markdown("---")
    st.subheader("🔮 Prévisions GDP (Prophet)")

    # Forecast parameters in columns
    col_params, col_info = st.columns([2, 1])

    with col_params:
        param_col1, param_col2, param_col3 = st.columns(3)

        with param_col1:
            periods = st.slider(
                "Années à prévoir",
                min_value=1,
                max_value=10,
                value=DEFAULTS["forecast_periods"],
                help="Nombre d'années futures à prédire"
            )

        with param_col2:
            show_components = st.checkbox(
                "Composants du modèle",
                value=False,
                help="Afficher la tendance extraite"
            )

        with param_col3:
            show_metrics = st.checkbox(
                "Métriques du modèle",
                value=False,
                help="Évaluer la performance du modèle"
            )

    with col_info:
        st.info(
            f"📊 Prévision pour **{country_code}**\n\n"
            f"Horizon: **{periods} ans**",
            icon="🔮"
        )

    # Run forecast
    try:
        forecast_result = _run_forecast(country_code, periods, df)

        if forecast_result is None:
            return

        forecast, metrics, forecaster = forecast_result

        # Main forecast chart
        _render_forecast_chart(forecast, country_code, df)

        # Optional: Model metrics
        if show_metrics and metrics:
            _render_model_metrics(metrics)

        # Optional: Components
        if show_components and forecaster:
            _render_components(forecaster)

        # Forecast table
        _render_forecast_table(forecast)

    except ImportError:
        st.warning(
            "⚠️ **Prophet n'est pas installé.**\n\n"
            "Installez-le avec : `pip install prophet`"
        )
    except Exception as e:
        st.error(f"❌ Erreur de prévision : {e}")
        with st.expander("Détails de l'erreur"):
            st.exception(e)


def _run_forecast(country_code: str, periods: int, df: pd.DataFrame):
    """Run the Prophet forecast."""
    import sys
    import os
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

    try:
        from ml.forecast_gdp import GDPForecaster

        with st.spinner("🔄 Entraînement du modèle Prophet..."):
            forecaster = GDPForecaster(country_code)
            forecaster.train()
            forecast = forecaster.predict(periods)

            # Try to get metrics (may fail with limited data)
            try:
                metrics = forecaster.evaluate()
            except Exception:
                metrics = None

        return forecast, metrics, forecaster

    except ImportError:
        # Fallback: use simple forecast function
        from ml.forecast_gdp import forecast_gdp

        with st.spinner("🔄 Calcul des prévisions..."):
            forecast = forecast_gdp(country_code, periods)

        return forecast, None, None


def _render_forecast_chart(forecast: pd.DataFrame, country_code: str, historical_df: pd.DataFrame):
    """Render the main forecast chart."""
    fig = go.Figure()

    # Determine if forecast has is_forecast column
    if "is_forecast" in forecast.columns:
        hist = forecast[~forecast["is_forecast"]]
        pred = forecast[forecast["is_forecast"]]
    else:
        # Fallback: use historical data and forecast
        hist_dates = set(pd.to_datetime(historical_df["date"]).dt.date)
        forecast["is_historical"] = forecast["ds"].dt.date.isin(hist_dates)
        hist = forecast[forecast["is_historical"]]
        pred = forecast[~forecast["is_historical"]]

    # Historical fitted values
    fig.add_trace(go.Scatter(
        x=hist["ds"],
        y=hist["yhat"],
        mode="lines+markers",
        name="Valeurs ajustées",
        line=dict(color=COLORS["primary"], width=2),
        marker=dict(size=6)
    ))

    # Actual historical values
    fig.add_trace(go.Scatter(
        x=historical_df["date"],
        y=historical_df["value"],
        mode="markers",
        name="Valeurs réelles",
        marker=dict(color=COLORS["dark"], size=8, symbol="diamond")
    ))

    # Forecast values
    fig.add_trace(go.Scatter(
        x=pred["ds"],
        y=pred["yhat"],
        mode="lines+markers",
        name="Prévision",
        line=dict(color=COLORS["success"], width=2, dash="dash"),
        marker=dict(size=8)
    ))

    # Confidence interval
    fig.add_trace(go.Scatter(
        x=pd.concat([pred["ds"], pred["ds"][::-1]]),
        y=pd.concat([pred["yhat_upper"], pred["yhat_lower"][::-1]]),
        fill="toself",
        fillcolor=f"rgba(30, 132, 73, 0.15)",
        line=dict(color="rgba(255,255,255,0)"),
        name="Intervalle de confiance (95%)",
        hoverinfo="skip"
    ))

    fig.update_layout(
        title=dict(
            text=f"Prévision GDP per capita - {country_code}",
            x=0.5
        ),
        xaxis_title="Année",
        yaxis_title="GDP per capita (USD)",
        plot_bgcolor=CHART_CONFIG["plot_bgcolor"],
        paper_bgcolor=CHART_CONFIG["paper_bgcolor"],
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )

    st.plotly_chart(fig, use_container_width=True)


def _render_model_metrics(metrics: dict):
    """Render model performance metrics."""
    st.markdown("#### 📊 Performance du modèle")

    if "error" in metrics:
        st.warning(f"Évaluation impossible : {metrics['error']}")
        return

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "MAE",
            f"{metrics.get('mae', 0):,.0f}",
            help="Mean Absolute Error - Erreur moyenne absolue"
        )

    with col2:
        st.metric(
            "RMSE",
            f"{metrics.get('rmse', 0):,.0f}",
            help="Root Mean Square Error - Racine de l'erreur quadratique moyenne"
        )

    with col3:
        st.metric(
            "MAPE",
            f"{metrics.get('mape', 0):. 1f}%",
            help="Mean Absolute Percentage Error - Erreur moyenne en pourcentage"
        )

    with col4:
        st.metric(
            "Coverage",
            f"{metrics.get('coverage', 0):. 0f}%",
            help="Couverture de l'intervalle de confiance"
        )

    # Interpretation
    mape = metrics.get('mape', 100)
    if mape < 10:
        st.success("✅ Excellent modèle (MAPE < 10%)")
    elif mape < 20:
        st.info("ℹ️ Bon modèle (MAPE < 20%)")
    else:
        st.warning("⚠️ Modèle à améliorer (MAPE ≥ 20%)")


def _render_components(forecaster):
    """Render model components (trend)."""
    st.markdown("#### 📈 Composants du modèle")

    try:
        components = forecaster.get_components()

        fig = px.line(
            components,
            x="ds",
            y="trend",
            title="Tendance extraite par Prophet"
        )

        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Tendance",
            plot_bgcolor=CHART_CONFIG["plot_bgcolor"],
            paper_bgcolor=CHART_CONFIG["paper_bgcolor"],
        )

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.warning(f"Impossible d'afficher les composants : {e}")


def _render_forecast_table(forecast: pd.DataFrame):
    """Render forecast values table."""
    with st.expander("📋 Tableau des prévisions"):
        # Filter to forecast only
        if "is_forecast" in forecast.columns:
            display_df = forecast[forecast["is_forecast"]].copy()
        else:
            display_df = forecast.tail(10).copy()

        display_df = display_df[["ds", "yhat", "yhat_lower", "yhat_upper"]].copy()
        display_df.columns = ["Année", "Prévision", "Borne basse", "Borne haute"]
        display_df["Année"] = display_df["Année"].dt.year

        # Format numbers
        for col in ["Prévision", "Borne basse", "Borne haute"]:
            display_df[col] = display_df[col].apply(lambda x: f"{x:,.0f}")

        st.dataframe(display_df, use_container_width=True, hide_index=True)