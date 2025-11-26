import pandas as pd
from sqlalchemy import create_engine, text
from etl.config import DATABASE_URL
import great_expectations as ge
from great_expectations.core import ExpectationSuite
from typing import Dict, List, Any
import json
from datetime import datetime


class DataQualityValidator:
    """Enhanced data quality validation with Great Expectations."""

    def __init__(self):
        self.engine = create_engine(DATABASE_URL)
        self.results: List[Dict[str, Any]] = []

    def _load_fact_data(self) -> pd.DataFrame:
        """Load fact table with dimensions."""
        query = '''
        SELECT
            fi.indicator_id,
            fi.value,
            fi. indicator_code,
            fi. indicator_name,
            fi.unit,
            dc. country_code,
            dc.country_name,
            dd.date,
            ddom.domain_name
        FROM fact_indicator fi
        JOIN dim_country dc ON fi.country_id = dc.country_id
        JOIN dim_date dd ON fi.date_id = dd.date_id
        JOIN dim_domain ddom ON fi.domain_id = ddom.domain_id
        '''
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            return pd.DataFrame(result.fetchall(), columns=result.keys())

    def _load_dimension_data(self, table: str) -> pd.DataFrame:
        """Load a dimension table."""
        with self.engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM {table}"))
            return pd.DataFrame(result.fetchall(), columns=result.keys())

    def validate_fact_indicator(self) -> Dict[str, Any]:
        """Validate fact_indicator table."""
        df = self._load_fact_data()
        ge_df = ge.from_pandas(df)

        expectations = [
            # Null checks
            ("value", "expect_column_values_to_not_be_null", {}),
            ("indicator_code", "expect_column_values_to_not_be_null", {}),
            ("country_code", "expect_column_values_to_not_be_null", {}),
            ("date", "expect_column_values_to_not_be_null", {}),

            # Value constraints
            ("value", "expect_column_values_to_be_between", {"min_value": 0, "max_value": 500000}),

            # Valid values
            ("domain_name", "expect_column_values_to_be_in_set", {"value_set": ["economy", "finance"]}),
            ("country_code", "expect_column_values_to_be_in_set", {"value_set": ["FRA", "USA", "DEU", "GLOBAL"]}),
            ("unit", "expect_column_values_to_be_in_set", {"value_set": ["USD", "INDEX"]}),

            # Type checks
            ("value", "expect_column_values_to_be_of_type", {"type_": "float64"}),

            # Uniqueness (composite key simulation)
            ("indicator_id", "expect_column_values_to_be_unique", {}),
        ]

        results = {"table": "fact_indicator", "checks": [], "passed": True}

        for column, expectation, kwargs in expectations:
            method = getattr(ge_df, expectation)
            result = method(column, **kwargs)

            check_result = {
                "column": column,
                "expectation": expectation,
                "success": result.success,
                "kwargs": kwargs
            }

            if not result.success:
                results["passed"] = False
                check_result["details"] = str(result.result)

            results["checks"].append(check_result)

        return results

    def validate_dim_country(self) -> Dict[str, Any]:
        """Validate dim_country table."""
        df = self._load_dimension_data("dim_country")
        ge_df = ge.from_pandas(df)

        results = {"table": "dim_country", "checks": [], "passed": True}

        # Primary key uniqueness
        result = ge_df.expect_column_values_to_be_unique("country_id")
        results["checks"].append({
            "column": "country_id",
            "expectation": "unique",
            "success": result.success
        })
        if not result.success:
            results["passed"] = False

        # Country code format (3 letters)
        result = ge_df.expect_column_values_to_match_regex("country_code", r"^[A-Z]{3,6}$")
        results["checks"].append({
            "column": "country_code",
            "expectation": "regex_match",
            "success": result.success
        })
        if not result.success:
            results["passed"] = False

        # Not null
        for col in ["country_code", "country_name"]:
            result = ge_df.expect_column_values_to_not_be_null(col)
            results["checks"].append({
                "column": col,
                "expectation": "not_null",
                "success": result.success
            })
            if not result.success:
                results["passed"] = False

        return results

    def validate_dim_date(self) -> Dict[str, Any]:
        """Validate dim_date table."""
        df = self._load_dimension_data("dim_date")
        ge_df = ge.from_pandas(df)

        results = {"table": "dim_date", "checks": [], "passed": True}

        # Primary key uniqueness
        result = ge_df.expect_column_values_to_be_unique("date_id")
        results["checks"].append({
            "column": "date_id",
            "expectation": "unique",
            "success": result.success
        })

        # Date uniqueness
        result = ge_df.expect_column_values_to_be_unique("date")
        results["checks"].append({
            "column": "date",
            "expectation": "unique",
            "success": result.success
        })

        return results

    def validate_data_freshness(self, max_days: int = 365) -> Dict[str, Any]:
        """Check if data is fresh enough."""
        query = "SELECT MAX(date) as latest_date FROM dim_date"

        with self.engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()

        if result and result[0]:
            latest_date = pd.to_datetime(result[0])
            days_old = (pd.Timestamp.now() - latest_date).days

            return {
                "check": "data_freshness",
                "latest_date": str(latest_date.date()),
                "days_old": days_old,
                "max_allowed_days": max_days,
                "passed": days_old <= max_days
            }

        return {"check": "data_freshness", "passed": False, "error": "No dates found"}

    def validate_referential_integrity(self) -> Dict[str, Any]:
        """Check referential integrity between tables."""
        checks = []

        # Check fact -> dim_country
        query = '''
        SELECT COUNT(*) as orphans
        FROM fact_indicator fi
        LEFT JOIN dim_country dc ON fi.country_id = dc.country_id
        WHERE dc.country_id IS NULL
        '''
        with self.engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()
            orphans = result[0] if result else 0
            checks.append({
                "relationship": "fact_indicator -> dim_country",
                "orphan_records": orphans,
                "passed": orphans == 0
            })

        # Check fact -> dim_date
        query = '''
        SELECT COUNT(*) as orphans
        FROM fact_indicator fi
        LEFT JOIN dim_date dd ON fi.date_id = dd.date_id
        WHERE dd. date_id IS NULL
        '''
        with self.engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()
            orphans = result[0] if result else 0
            checks.append({
                "relationship": "fact_indicator -> dim_date",
                "orphan_records": orphans,
                "passed": orphans == 0
            })

        return {
            "check": "referential_integrity",
            "checks": checks,
            "passed": all(c["passed"] for c in checks)
        }

    def run_all_validations(self) -> Dict[str, Any]:
        """Run all validation checks."""
        print("🔍 Running data quality validations.. .\n")

        all_results = {
            "timestamp": datetime.now().isoformat(),
            "validations": [],
            "summary": {"total": 0, "passed": 0, "failed": 0}
        }

        # Run all checks
        validations = [
            ("Fact Indicator", self.validate_fact_indicator),
            ("Dim Country", self.validate_dim_country),
            ("Dim Date", self.validate_dim_date),
            ("Data Freshness", self.validate_data_freshness),
            ("Referential Integrity", self.validate_referential_integrity),
        ]

        for name, func in validations:
            try:
                result = func()
                result["validation_name"] = name
                all_results["validations"].append(result)

                passed = result.get("passed", False)
                all_results["summary"]["total"] += 1
                if passed:
                    all_results["summary"]["passed"] += 1
                    print(f"  ✅ {name}: PASSED")
                else:
                    all_results["summary"]["failed"] += 1
                    print(f"  ❌ {name}: FAILED")

            except Exception as e:
                print(f"  ⚠️ {name}: ERROR - {e}")
                all_results["validations"].append({
                    "validation_name": name,
                    "passed": False,
                    "error": str(e)
                })
                all_results["summary"]["failed"] += 1

        # Summary
        print(f"\n📊 Summary: {all_results['summary']['passed']}/{all_results['summary']['total']} validations passed")

        all_results["overall_success"] = all_results["summary"]["failed"] == 0

        return all_results

    def save_results(self, results: Dict[str, Any], filepath: str = "validation_results.json"):
        """Save validation results to file."""
        with open(filepath, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\n💾 Results saved to {filepath}")


def run_ge_validation() -> None:
    """Main entry point for validation."""
    validator = DataQualityValidator()
    results = validator.run_all_validations()

    if not results["overall_success"]:
        raise AssertionError("Great Expectations validation failed")

    print("\n✅ All Great Expectations validations passed successfully!")


if __name__ == "__main__":
    run_ge_validation()