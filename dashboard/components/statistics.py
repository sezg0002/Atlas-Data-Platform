"""Statistics section component."""

import streamlit as st
import pandas as pd

from utils.formatters import format_date


def render_statistics(df: pd.DataFrame):
    """Render statistics section."""
    if df.empty:
        return

    col1, col2 = st.columns(2)

    with col1:
        _render_descriptive_stats(df)

    with col2:
        _render_data_info(df)

    # Raw data expander
    _render_raw_data(df)


def _render_descriptive_stats(df: pd.DataFrame):
    """Render descriptive statistics table."""
    st.subheader("📊 Statistiques descriptives")

    stats = df["value"].describe().to_frame().T
    stats.columns = ["N", "Moyenne", "Écart-type", "Min", "25%", "Médiane", "75%", "Max"]

    # Format numbers
    for col in stats.columns:
        if col == "N":
            stats[col] = stats[col].astype(int)
        else:
            stats[col] = stats[col].apply(lambda x: f"{x:,.2f}")

    st.dataframe(stats, use_container_width=True)


def _render_data_info(df: pd.DataFrame):
    """Render data information."""
    st.subheader("📅 Informations")

    info_data = {
        "Métrique": ["Période début", "Période fin", "Observations", "Indicateur"],
        "Valeur": [
            format_date(df["date"].min()),
            format_date(df["date"].max()),
            str(len(df)),
            df["indicator_name"].iloc[0] if "indicator_name" in df.columns else "N/A"
        ]
    }

    st.dataframe(
        pd.DataFrame(info_data).set_index("Métrique"),
        use_container_width=True
    )


def _render_raw_data(df: pd.DataFrame):
    """Render raw data with download option."""
    with st.expander("📋 Voir les données brutes"):
        # Display options
        col1, col2 = st.columns([3, 1])

        with col2:
            n_rows = st.selectbox(
                "Lignes à afficher",
                options=[10, 20, 50, 100, len(df)],
                index=1,
                format_func=lambda x: "Toutes" if x == len(df) else str(x)
            )

        # Display data
        display_df = df.tail(n_rows) if n_rows != len(df) else df
        st.dataframe(display_df, use_container_width=True)

        # Download buttons
        col_dl1, col_dl2, _ = st.columns([1, 1, 2])

        with col_dl1:
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="⬇️ CSV",
                data=csv,
                file_name="atlas_data.csv",
                mime="text/csv",
                use_container_width=True
            )

        with col_dl2:
            json_data = df.to_json(orient="records", date_format="iso")
            st.download_button(
                label="⬇️ JSON",
                data=json_data,
                file_name="atlas_data.json",
                mime="application/json",
                use_container_width=True
            )