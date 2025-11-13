
import pandas as pd
from .worldbank import fetch_worldbank_gdp_per_capita
from .yfinance_data import fetch_finance_index
from .load_to_db import upsert_dimensions_and_fact

def run_pipeline() -> None:
    econ = fetch_worldbank_gdp_per_capita()
    fin = fetch_finance_index()
    df = pd.concat([econ, fin], ignore_index=True)
    upsert_dimensions_and_fact(df)

if __name__ == "__main__":
    run_pipeline()
