"""Statistics section component."""

import streamlit as st
import pandas as pd


def format_date(date) -> str:
    """Format date to string."""
    if pd.isna(date):
        return "N/A"
    return date.strftime("%Y-%m-%d")


def render_statistics(df: pd.DataFrame):
    """Render statistics section."""
    if df.empty:
        return

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📊 Statistics")
        stats = df["value"].describe(). to_frame().T

        # Rename columns dynamically based on what describe() returns
        column_names = {
            "count": "N",
            "mean": "Mean",
            "std": "Std Dev",
            "min": "Min",
            "25%": "25%",
            "50%": "Median",
            "75%": "75%",
            "max": "Max"
        }
        stats.columns = [column_names. get(col, col) for col in stats.columns]
        st.dataframe(stats, use_container_width=True)

    with col2:
        st. subheader("📅 Information")
        st.write(f"**Start**: {format_date(df['date'].min())}")
        st.write(f"**End**: {format_date(df['date'].max())}")
        st.write(f"**Observations**: {len(df)}")

    # Raw data
    with st.expander("📋 Raw Data"):
        st.dataframe(df. tail(20), use_container_width=True)

        csv = df.to_csv(index=False). encode('utf-8')
        st. download_button(
            label="⬇️ Download CSV",
            data=csv,
            file_name="atlas_data.csv",
            mime="text/csv"
        )