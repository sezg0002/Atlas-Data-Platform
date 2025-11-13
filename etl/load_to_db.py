
from sqlalchemy import create_engine, text
import pandas as pd
from .config import DATABASE_URL

engine = create_engine(DATABASE_URL)

def upsert_dimensions_and_fact(df: pd.DataFrame) -> None:
    if df.empty:
        return
    df = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])

    with engine.begin() as conn:
        for d in df["domain_name"].drop_duplicates():
            conn.execute(
                text(
                    "INSERT INTO dim_domain (domain_name) VALUES (:d) "
                    "ON CONFLICT (domain_name) DO NOTHING"
                ),
                {"d": d},
            )

        countries = df[["country_code", "country_name"]].drop_duplicates()
        for _, row in countries.iterrows():
            conn.execute(
                text(
                    "INSERT INTO dim_country (country_code, country_name) "
                    "VALUES (:code, :name) "
                    "ON CONFLICT (country_code) "
                    "DO UPDATE SET country_name = EXCLUDED.country_name"
                ),
                {"code": row["country_code"], "name": row["country_name"]},
            )

        dates = df["date"].dt.date.drop_duplicates()
        for d in dates:
            conn.execute(
                text(
                    "INSERT INTO dim_date (date) VALUES (:d) "
                    "ON CONFLICT (date) DO NOTHING"
                ),
                {"d": d},
            )

        for _, row in df.iterrows():
            date_val = row["date"].date()
            domain = conn.execute(
                text("SELECT domain_id FROM dim_domain WHERE domain_name = :d"),
                {"d": row["domain_name"]},
            ).first()
            country = conn.execute(
                text("SELECT country_id FROM dim_country WHERE country_code = :c"),
                {"c": row["country_code"]},
            ).first()
            date = conn.execute(
                text("SELECT date_id FROM dim_date WHERE date = :d"),
                {"d": date_val},
            ).first()
            if not (domain and country and date):
                continue
            conn.execute(
                text(
                    "INSERT INTO fact_indicator "
                    "(domain_id, country_id, date_id, indicator_code, indicator_name, value, unit) "
                    "VALUES (:domain_id, :country_id, :date_id, :icode, :iname, :val, :unit)"
                ),
                {
                    "domain_id": domain[0],
                    "country_id": country[0],
                    "date_id": date[0],
                    "icode": row["indicator_code"],
                    "iname": row["indicator_name"],
                    "val": row["value"],
                    "unit": row["unit"],
                },
            )
