import requests
import pandas as pd
from .logger import get_logger

logger = get_logger(__name__)

WORLD_BANK_URL = "https://api.worldbank.org/v2/country/{country}/indicator/NY.GDP. PCAP.CD"
COUNTRIES = ["FRA", "USA", "DEU"]
START_YEAR = 2000
END_YEAR = 2023


def fetch_worldbank_gdp_per_capita() -> pd.DataFrame:
    records = []
    for code in COUNTRIES:
        logger.info(f"Fetching World Bank data for {code}...")
        params = {
            "format": "json",
            "per_page": 1000,
            "date": f"{START_YEAR}:{END_YEAR}",
        }
        try:
            resp = requests.get(
                WORLD_BANK_URL.format(country=code),
                params=params,
                timeout=30
            )
            resp.raise_for_status()
            data = resp.json()

            if not data or len(data) < 2:
                logger.warning(f"No data returned for {code}")
                continue

            for entry in data[1]:
                value = entry.get("value")
                year = entry.get("date")
                country_name = entry.get("country", {}).get("value")
                if value is None or year is None or country_name is None:
                    continue
                records.append({
                    "country_code": code,
                    "country_name": country_name,
                    "date": pd.to_datetime(f"{year}-12-31"),
                    "indicator_code": "NY.GDP. PCAP.CD",
                    "indicator_name": "GDP per capita (current US$)",
                    "value": float(value),
                    "unit": "USD",
                })
            logger.info(f"✓ {code}: {len([r for r in records if r['country_code'] == code])} records")

        except requests.RequestException as e:
            logger.error(f"Failed to fetch data for {code}: {e}")
            continue

    df = pd.DataFrame.from_records(records)
    df["domain_name"] = "economy"
    logger.info(f"Total records fetched: {len(df)}")
    return df