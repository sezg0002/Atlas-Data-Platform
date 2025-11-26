"""Database connection and queries."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from typing import Optional, List

from config_dash import DEFAULTS


@st.cache_resource
def get_engine():
    """Create database engine with connection pooling."""
    try:
        from etl.config import DATABASE_URL
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception:
        return None


def check_connection() -> bool:
    """Check if database connection is available."""
    return get_engine() is not None


@st.cache_data(ttl=DEFAULTS["cache_ttl"])
def load_domain_data(domain: str, country_code: Optional[str] = None) -> pd.DataFrame:
    """Load data from warehouse with caching."""
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()

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

    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df
    except OperationalError:
        return pd.DataFrame()


@st.cache_data(ttl=DEFAULTS["cache_ttl"])
def get_available_countries() -> List[str]:
    """Get list of available countries from database."""
    engine = get_engine()
    if engine is None:
        return DEFAULTS["countries"]

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT DISTINCT country_code FROM dim_country ORDER BY country_code")
            )
            countries = [row[0] for row in result.fetchall()]
            return countries if countries else DEFAULTS["countries"]
    except Exception:
        return DEFAULTS["countries"]


@st.cache_data(ttl=DEFAULTS["cache_ttl"])
def get_data_summary() -> dict:
    """Get summary statistics about the data."""
    engine = get_engine()
    if engine is None:
        return {}

    try:
        with engine.connect() as conn:
            total = conn.execute(text("SELECT COUNT(*) FROM fact_indicator")).scalar()
            dates = conn.execute(text("SELECT MIN(date), MAX(date) FROM dim_date")).fetchone()
            countries = conn.execute(text("SELECT COUNT(DISTINCT country_code) FROM dim_country")).scalar()

            return {
                "total_records": total,
                "min_date": dates[0] if dates else None,
                "max_date": dates[1] if dates else None,
                "countries_count": countries,
            }
    except Exception:
        return {}


def clear_cache():
    """Clear all cached data."""
    st.cache_data.clear()