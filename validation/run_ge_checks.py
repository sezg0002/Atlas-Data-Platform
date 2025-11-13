
import pandas as pd
from sqlalchemy import create_engine, text
from etl.config import DATABASE_URL
import great_expectations as ge

def run_ge_validation() -> None:
    engine = create_engine(DATABASE_URL)
    query = '''
    SELECT
        fi.value,
        fi.indicator_code,
        dc.country_code,
        dd.date
    FROM fact_indicator fi
    JOIN dim_country dc ON fi.country_id = dc.country_id
    JOIN dim_date dd ON fi.date_id = dd.date_id
    '''
    with engine.connect() as conn:
        result = conn.execute(text(query))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())

    ge_df = ge.from_pandas(df)

    ge_df.expect_column_values_to_not_be_null("value")
    ge_df.expect_column_values_to_not_be_null("indicator_code")
    ge_df.expect_column_values_to_not_be_null("country_code")
    ge_df.expect_column_values_to_not_be_null("date")
    ge_df.expect_column_values_to_be_greater_than("value", 0)

    results = ge_df.validate()
    if not results["success"]:
        raise AssertionError("Great Expectations validation failed")
    print("Great Expectations validation passed successfully.")

if __name__ == "__main__":
    run_ge_validation()
