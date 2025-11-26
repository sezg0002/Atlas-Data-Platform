"""Sidebar filters component."""

import streamlit as st
from typing import Tuple, Optional

from ._imports import (
    DOMAIN_LABELS,
    DEFAULTS,
    get_available_countries,
    get_data_summary,
    clear_cache,
)


def render_sidebar() -> Tuple[str, Optional[str]]:
    """
    Render sidebar with filters.

    Returns:
        Tuple[str, Optional[str]]: (domain, country_code)
            - domain: 'economy' or 'finance'
            - country_code: ISO country code (e.g., 'FRA') or None for finance
    """

    with st.sidebar:
        # Logo/Title
        st.markdown(
            """
            <div style='text-align: center; padding: 0. 5rem 0 1rem 0;'>
                <span style='font-size: 2rem;'>🌍</span>
                <p style='margin: 0; font-weight: bold; color: #2C3E50;'>Atlas</p>
            </div>
            """,
            unsafe_allow_html=True
        )

        st.header("🎛️ Filtres")

        # Domain selector
        domain = st.selectbox(
            "Domaine",
            options=DEFAULTS["domains"],
            format_func=lambda x: DOMAIN_LABELS.get(x, x.capitalize()),
            help="Sélectionnez le type de données à analyser"
        )

        # Country selector (only for economy)
        country_code = None
        if domain == "economy":
            countries = get_available_countries()
            country_code = st.selectbox(
                "Pays",
                options=countries,
                help="Sélectionnez un pays pour voir ses indicateurs économiques"
            )
        else:
            st.caption("📊 Les données financières sont globales (indice SPY).")

        st.divider()

        # Data summary section
        _render_data_summary()

        st.divider()

        # Action buttons
        _render_actions()

        st.divider()

        # Help section
        _render_help()

    return domain, country_code


def _render_data_summary():
    """Render data summary statistics in sidebar."""
    summary = get_data_summary()

    if summary:
        st.markdown("**📈 Résumé des données**")

        col1, col2 = st.columns(2)

        with col1:
            st.metric(
                label="Enregistrements",
                value=f"{summary.get('total_records', 0):,}",
                label_visibility="collapsed"
            )
            st.caption("📁 Enregistrements")

        with col2:
            st.metric(
                label="Pays",
                value=summary.get('countries_count', 0),
                label_visibility="collapsed"
            )
            st.caption("🌍 Pays")

        # Date range
        if summary.get('min_date') and summary.get('max_date'):
            min_year = summary['min_date'].strftime('%Y')
            max_year = summary['max_date'].strftime('%Y')
            st.caption(f"📅 Période: {min_year} → {max_year}")
    else:
        st.warning("Aucune donnée disponible")


def _render_actions():
    """Render action buttons in sidebar."""
    st.markdown("**⚡ Actions**")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("🔄 Rafraîchir", use_container_width=True, help="Recharger les données"):
            clear_cache()
            st.rerun()

    with col2:
        if st.button("🗑️ Vider cache", use_container_width=True, help="Vider le cache"):
            clear_cache()
            st.success("Cache vidé!")
            st.rerun()


def _render_help():
    """Render help section in sidebar."""
    with st.expander("ℹ️ Aide", expanded=False):
        st.markdown(
            """
            **Guide rapide:**

            1. 📊 **Domaine**: Choisissez entre données économiques ou financières

            2. 🌍 **Pays**: Sélectionnez un pays (économie uniquement)

            3. 📈 **Onglets**:
               - *Vue d'ensemble*: Graphiques historiques
               - *Analyse*: Croissance annuelle
               - *Prévisions*: Modèle Prophet

            4. 🔄 **Rafraîchir**: Recharge les données depuis la base

            ---

            **Commandes utiles:**
            ```bash
            # Lancer l'ETL
            python -m etl.run_etl

            # Lancer le dashboard
            streamlit run dashboard/app.py
            ```
            """
        )

        # Version info
        st.caption("v1.0.0 | Atlas Data Platform")


def render_sidebar_minimal() -> Tuple[str, Optional[str]]:
    """
    Render a minimal sidebar (alternative version).

    Returns:
        Tuple[str, Optional[str]]: (domain, country_code)
    """

    with st.sidebar:
        st.header("🎛️ Filtres")

        domain = st.radio(
            "Domaine",
            options=DEFAULTS["domains"],
            format_func=lambda x: f"{'📈' if x == 'economy' else '💹'} {DOMAIN_LABELS.get(x, x)}",
            horizontal=True
        )

        country_code = None
        if domain == "economy":
            countries = get_available_countries()
            country_code = st.selectbox("Pays", countries)

        if st.button("🔄 Rafraîchir", use_container_width=True):
            clear_cache()
            st.rerun()

    return domain, country_code