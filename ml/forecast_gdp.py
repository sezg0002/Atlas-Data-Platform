import pandas as pd
import numpy as np
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
from sqlalchemy import create_engine, text
from etl.config import DATABASE_URL
from typing import Optional, Dict, Any
import logging

logging.getLogger('prophet').setLevel(logging.WARNING)
logging.getLogger('cmdstanpy').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


class GDPForecaster:
    """GDP Forecasting model using Prophet with cross-validation."""

    def __init__(self, country_code: str = "FRA"):
        self.country_code = country_code
        self.model: Optional[Prophet] = None
        self.metrics: Optional[pd.DataFrame] = None
        self.engine = create_engine(DATABASE_URL)

    def _load_data(self) -> pd.DataFrame:
        """Load GDP data from warehouse."""
        query = '''
        SELECT ddate. date, fi.value
        FROM fact_indicator fi
        JOIN dim_country dc ON fi.country_id = dc.country_id
        JOIN dim_date ddate ON fi.date_id = ddate.date_id
        JOIN dim_domain ddom ON fi.domain_id = ddom.domain_id
        WHERE dc.country_code = :ccode
          AND ddom.domain_name = 'economy'
          AND fi.value IS NOT NULL
        ORDER BY ddate.date
        '''
        with self.engine.connect() as conn:
            result = conn.execute(text(query), {"ccode": self.country_code})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        if df.empty:
            raise RuntimeError(f"No GDP data available for country {self.country_code}")

        return df.rename(columns={"date": "ds", "value": "y"})

    def train(self,
              yearly_seasonality: bool = True,
              changepoint_prior_scale: float = 0.05,
              seasonality_prior_scale: float = 10.0) -> "GDPForecaster":
        """Train the Prophet model with custom parameters."""
        df = self._load_data()

        self.model = Prophet(
            yearly_seasonality=yearly_seasonality,
            weekly_seasonality=False,  # GDP data is yearly
            daily_seasonality=False,
            changepoint_prior_scale=changepoint_prior_scale,
            seasonality_prior_scale=seasonality_prior_scale,
            interval_width=0.95  # 95% confidence interval
        )

        # Add custom seasonality if needed
        # self.model.add_seasonality(name='economic_cycle', period=7, fourier_order=3)

        self.model.fit(df)
        logger.info(f"Model trained for {self.country_code} with {len(df)} data points")

        return self

    def predict(self, periods: int = 5) -> pd.DataFrame:
        """Generate forecast for future periods."""
        if self.model is None:
            self.train()

        future = self.model.make_future_dataframe(periods=periods, freq="YE")
        forecast = self.model.predict(future)

        # Add useful columns
        forecast["country_code"] = self.country_code
        forecast["is_forecast"] = forecast["ds"] > pd.Timestamp.now()

        return forecast[[
            "ds", "yhat", "yhat_lower", "yhat_upper",
            "trend", "country_code", "is_forecast"
        ]]

    def evaluate(self, initial: str = "10 Y", period: str = "2 Y", horizon: str = "3 Y") -> Dict[str, float]:
        """Evaluate model using cross-validation."""
        if self.model is None:
            self.train()

        try:
            df_cv = cross_validation(
                self.model,
                initial=initial,
                period=period,
                horizon=horizon
            )
            self.metrics = performance_metrics(df_cv)

            return {
                "mae": self.metrics["mae"].mean(),
                "rmse": self.metrics["rmse"].mean(),
                "mape": self.metrics["mape"].mean() * 100,  # Percentage
                "coverage": self.metrics["coverage"].mean() * 100
            }
        except Exception as e:
            logger.warning(f"Cross-validation failed: {e}")
            return {"error": str(e)}

    def get_components(self) -> pd.DataFrame:
        """Get trend and seasonality components."""
        if self.model is None:
            self.train()

        future = self.model.make_future_dataframe(periods=5, freq="YE")
        forecast = self.model.predict(future)

        return forecast[["ds", "trend", "yearly"]] if "yearly" in forecast.columns else forecast[["ds", "trend"]]


def forecast_gdp(country_code: str = "FRA", periods: int = 5) -> pd.DataFrame:
    """Simple function for backward compatibility."""
    forecaster = GDPForecaster(country_code)
    return forecaster.train().predict(periods)


def forecast_multiple_countries(
        countries: list = ["FRA", "USA", "DEU"],
        periods: int = 5
) -> pd.DataFrame:
    """Forecast GDP for multiple countries."""
    forecasts = []

    for country in countries:
        try:
            forecaster = GDPForecaster(country)
            forecast = forecaster.train().predict(periods)
            forecasts.append(forecast)
        except Exception as e:
            logger.error(f"Failed to forecast for {country}: {e}")

    if not forecasts:
        raise RuntimeError("No forecasts could be generated")

    return pd.concat(forecasts, ignore_index=True)


def compare_countries(countries: list = ["FRA", "USA", "DEU"]) -> pd.DataFrame:
    """Compare forecast metrics across countries."""
    results = []

    for country in countries:
        try:
            forecaster = GDPForecaster(country)
            forecaster.train()
            metrics = forecaster.evaluate()
            metrics["country_code"] = country
            results.append(metrics)
        except Exception as e:
            logger.error(f"Evaluation failed for {country}: {e}")

    return pd.DataFrame(results)


if __name__ == "__main__":
    # Example usage
    forecaster = GDPForecaster("FRA")
    forecaster.train()

    print("Forecast:")
    print(forecaster.predict(5))

    print("\nMetrics:")
    print(forecaster.evaluate())