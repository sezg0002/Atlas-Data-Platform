
import requests
import pandas as pd

WORLD_BANK_URL = "https://api.worldbank.org/v2/country/{country}/indicator/NY.GDP.PCAP.CD"

COUNTRIES = ["FRA", "USA", "DEU"]
START_YEAR = 2000
END_YEAR = 2023

def fetch_worldbank_gdp_per_capita() -> pd.DataFrame:
    records = []
    for code in COUNTRIES:
        params = {
            "format": "json",
            "per_page": 1000,
            "date": f"{START_YEAR}:{END_YEAR}",
        }
        resp = requests.get(WORLD_BANK_URL.format(country=code), params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data or len(data) < 2:
            continue
        for entry in data[1]:
            value = entry.get("value")
            year = entry.get("date")
            country_name = entry.get("country", {}).get("value")
            if value is None or year is None or country_name is None:
                continue
            records.append(
                {
                    "country_code": code,
                    "country_name": country_name,
                    "date": pd.to_datetime(f"{year}-12-31"),
                    "indicator_code": "NY.GDP.PCAP.CD",
                    "indicator_name": "GDP per capita (current US$)",
                    "value": float(value),
                    "unit": "USD",
                }
            )
    df = pd.DataFrame.from_records(records)
    df["domain_name"] = "economy"
    return df
