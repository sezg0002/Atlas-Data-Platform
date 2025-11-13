import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine, text
from etl.config import DATABASE_URL
from ml.forecast_gdp import forecast_gdp

# --- PAGE CONFIG ---
st.set_page_config(
    page_title=" Global Data Intelligence Platform",
    page_icon="",
    layout="wide",
)

# --- HEADER ---
st.markdown(
    """
    <h1 style='text-align:center; color:#2C3E50;'> Global Data Intelligence Platform</h1>
    <p style='text-align:center; color:#5D6D7E;'> Data Engineering, Analytics & Forecast</p>
    """,
    unsafe_allow_html=True
)

# --- DATABASE ---
engine = create_engine(DATABASE_URL)

@st.cache_data(ttl=3600)
def load_domain_data(domain: str, country_code: str | None = None) -> pd.DataFrame:
    query = """
    SELECT
        dd.date,
        fi.value,
        fi.indicator_name,
        dc.country_name,
        ddom.domain_name,
        fi.unit,
        dc.country_code
    FROM fact_indicator fi
    JOIN dim_date dd ON fi.date_id = dd.date_id
    JOIN dim_country dc ON fi.country_id = dc.country_id
    JOIN dim_domain ddom ON fi.domain_id = ddom.domain_id
    WHERE ddom.domain_name = :domain
    """
    params = {"domain": domain}
    if country_code:
        query += " AND dc.country_code = :ccode"
        params["ccode"] = country_code
    query += " ORDER BY dd.date"

    with engine.connect() as conn:
        result = conn.execute(text(query), params)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    return df

# --- FILTERS ---
with st.sidebar:
    st.header(" Filters")
    domain = st.selectbox("Select Domain", ["economy", "finance"])
    if domain == "economy":
        country_code = st.selectbox("Select Country", ["FRA", "USA", "DEU"])
    else:
        country_code = None
        st.caption("Finance data is global (SPY index).")

    st.divider()
    st.info(" Tip: Run the ETL first if data is missing.", icon="ℹ")

# --- LOAD DATA ---
df = load_domain_data(domain, country_code)

if df.empty:
    st.warning("⚠ No data available yet. Please run the ETL pipeline first.")
else:
    # --- KPIs ---
    latest_value = df["value"].iloc[-1]
    mean_value = df["value"].mean()
    delta = ((df["value"].iloc[-1] - df["value"].iloc[-2]) / df["value"].iloc[-2]) * 100 if len(df) > 2 else 0

    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric(label=" Latest Value", value=f"{latest_value:,.2f}", delta=f"{delta:+.2f}%")
    kpi2.metric(label=" Average Value", value=f"{mean_value:,.2f}")
    kpi3.metric(label=" Data Points", value=len(df))

    st.markdown("---")

    # --- MAIN CHART ---
    st.subheader(" Historical Trends")
    fig = px.line(
        df,
        x="date",
        y="value",
        title=f"{domain.capitalize()} Trends",
        color_discrete_sequence=["#2E86DE"]
    )
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title=df["unit"].iloc[0] if "unit" in df.columns else "Value",
        title_x=0.5,
        plot_bgcolor="#F9FBFD",
        paper_bgcolor="#F9FBFD",
        font=dict(color="#2C3E50"),
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- RAW DATA ---
    with st.expander(" View raw data (last 20 rows)"):
        st.dataframe(df.tail(20))

    # --- FORECAST ---
    if domain == "economy":
        st.markdown("---")
        st.subheader(" GDP Forecast (Prophet)")
        try:
            forecast = forecast_gdp(country_code)
            forecast_display = forecast.set_index("ds")[["yhat", "yhat_lower", "yhat_upper"]]

            fig_forecast = px.line(
                forecast_display,
                x=forecast_display.index,
                y=["yhat", "yhat_lower", "yhat_upper"],
                labels={"value": "GDP forecast", "ds": "Year"},
                title=f"{country_code} GDP Forecast (next 5 years)",
                color_discrete_sequence=["#1E8449", "#F39C12", "#E74C3C"]
            )
            fig_forecast.update_layout(
                title_x=0.5,
                plot_bgcolor="#F9FBFD",
                paper_bgcolor="#F9FBFD",
            )
            st.plotly_chart(fig_forecast, use_container_width=True)
        except Exception as e:
            st.error(f"Forecast error: {e}")
