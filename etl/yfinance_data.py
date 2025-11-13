
import pandas as pd
import yfinance as yf

TICKER = "SPY"

def fetch_finance_index(ticker: str = TICKER) -> pd.DataFrame:
    data = yf.download(ticker, period="5y", interval="1d", progress=False)
    if data.empty:
        raise RuntimeError(f"No data returned for ticker {ticker}")
    data = data.reset_index()
    data = data.rename(columns={"Date": "date", "Close": "value"})
    df = pd.DataFrame()
    df["date"] = pd.to_datetime(data["date"])
    df["value"] = data["value"].astype(float)
    df["country_code"] = "WLD"
    df["country_name"] = "Global"
    df["indicator_code"] = f"{ticker}_CLOSE"
    df["indicator_name"] = f"{ticker} Closing Price"
    df["unit"] = "USD"
    df["domain_name"] = "finance"
    return df[[
        "country_code",
        "country_name",
        "date",
        "indicator_code",
        "indicator_name",
        "value",
        "unit",
        "domain_name",
    ]]
