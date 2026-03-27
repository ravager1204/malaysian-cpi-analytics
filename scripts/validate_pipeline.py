"""
Validate that the end-to-end warehouse pipeline produced expected artifacts.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from sqlalchemy import create_engine

from config.settings import get_settings


def main() -> int:
    settings = get_settings()
    engine = create_engine(settings.database.sqlalchemy_url)

    checks = {
        "raw.cpi_data": "SELECT COUNT(*) AS row_count FROM raw.cpi_data",
        "raw.categories": "SELECT COUNT(*) AS row_count FROM raw.categories",
        "staging.cpi_monthly": "SELECT COUNT(*) AS row_count FROM staging.cpi_monthly",
        "mart.inflation_by_state": "SELECT COUNT(*) AS row_count FROM mart.inflation_by_state",
        "mart.inflation_by_category": (
            "SELECT COUNT(*) AS row_count FROM mart.inflation_by_category"
        ),
        "mart.state_comparison": "SELECT COUNT(*) AS row_count FROM mart.state_comparison",
    }

    print("Pipeline validation summary")
    print("=" * 70)
    for label, query in checks.items():
        row_count = pd.read_sql(query, engine).iloc[0]["row_count"]
        print(f"{label}: {row_count:,} rows")

    latest_date = pd.read_sql(
        "SELECT MAX(date) AS latest_date FROM mart.inflation_by_state",
        engine,
    ).iloc[0]["latest_date"]
    print(f"Latest mart date: {latest_date}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
