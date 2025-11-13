
import pandas as pd
from prophet import Prophet
from sqlalchemy import create_engine, text
from etl.config import DATABASE_URL

def forecast_gdp(country_code: str = "FRA", periods: int = 5) -> pd.DataFrame:
    engine = create_engine(DATABASE_URL)
    query = '''
    SELECT ddate.date, fi.value
    FROM fact_indicator fi
    JOIN dim_country dc ON fi.country_id = dc.country_id
    JOIN dim_date ddate ON fi.date_id = ddate.date_id
    JOIN dim_domain ddom ON fi.domain_id = ddom.domain_id
    WHERE dc.country_code = :ccode
      AND ddom.domain_name = 'economy'
    ORDER BY ddate.date
    '''
    with engine.connect() as conn:
        result = conn.execute(text(query), {"ccode": country_code})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    if df.empty:
        raise RuntimeError("No GDP data available for country " + country_code)

    df_prophet = df.rename(columns={"date": "ds", "value": "y"})
    model = Prophet()
    model.fit(df_prophet)
    future = model.make_future_dataframe(periods=periods, freq="Y")
    forecast = model.predict(future)
    return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]]
