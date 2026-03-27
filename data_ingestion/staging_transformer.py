"""
Staging Data Transformer
Transform raw data into clean staging tables
"""
import logging

import pandas as pd
from sqlalchemy import create_engine

from config.settings import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StagingTransformer:
    """Transform raw data to staging layer."""

    def __init__(self):
        settings = get_settings()
        self.engine = create_engine(settings.database.sqlalchemy_url)
        logger.info("Staging transformer initialized")

    def transform_categories(self) -> int:
        """Transform category lookup to staging."""
        logger.info("Transforming categories")

        df = pd.read_sql(
            """
            SELECT
                division,
                desc_en as category_name_en,
                desc_bm as category_name_bm,
                digits as category_level
            FROM raw.categories
            WHERE digits = 2
            """,
            self.engine,
        )

        df.to_sql(
            "categories",
            self.engine,
            schema="staging",
            if_exists="replace",
            index=False,
        )

        logger.info("Loaded %s categories to staging", len(df))
        return len(df)

    def transform_cpi_monthly(self) -> int:
        """Transform CPI data with category names."""
        logger.info("Transforming CPI monthly data")

        df = pd.read_sql(
            """
            SELECT
                c.state,
                c.date,
                c.division,
                COALESCE(cat.desc_en, 'Overall') as category_name,
                c.index as index_value
            FROM raw.cpi_data c
            LEFT JOIN raw.categories cat
                ON c.division = cat.division
                AND cat.digits = 2
            ORDER BY c.date, c.state, c.division
            """,
            self.engine,
        )

        logger.info("Processing %s records", f"{len(df):,}")

        df.to_sql(
            "cpi_monthly",
            self.engine,
            schema="staging",
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=5000,
        )

        logger.info("Loaded %s records to staging.cpi_monthly", f"{len(df):,}")
        return len(df)

    def validate_staging(self) -> bool:
        """Validate staging data quality."""
        logger.info("Validating staging data")

        raw_count = pd.read_sql(
            "SELECT COUNT(*) as cnt FROM raw.cpi_data",
            self.engine,
        ).iloc[0]["cnt"]

        stg_count = pd.read_sql(
            "SELECT COUNT(*) as cnt FROM staging.cpi_monthly",
            self.engine,
        ).iloc[0]["cnt"]

        if raw_count != stg_count:
            logger.warning(
                "Row count mismatch: raw=%s, staging=%s",
                raw_count,
                stg_count,
            )
        else:
            logger.info("Row counts match: %s", f"{stg_count:,}")

        nulls = pd.read_sql(
            """
            SELECT
                COUNT(*) FILTER (WHERE state IS NULL) as null_state,
                COUNT(*) FILTER (WHERE date IS NULL) as null_date,
                COUNT(*) FILTER (WHERE index_value IS NULL) as null_index
            FROM staging.cpi_monthly
            """,
            self.engine,
        )

        if nulls.sum().sum() > 0:
            logger.warning("Found nulls: %s", nulls.to_dict(orient="records")[0])
        else:
            logger.info("No nulls in critical columns")

        return True

    def run_all(self) -> bool:
        """Run complete staging transformation."""
        logger.info("=" * 70)
        logger.info("STARTING STAGING TRANSFORMATION")
        logger.info("=" * 70)

        try:
            cat_count = self.transform_categories()
            cpi_count = self.transform_cpi_monthly()
            self.validate_staging()

            logger.info("\n%s", "=" * 70)
            logger.info("STAGING TRANSFORMATION COMPLETE")
            logger.info("=" * 70)
            logger.info("Categories: %s", cat_count)
            logger.info("CPI Records: %s", f"{cpi_count:,}")
            logger.info("=" * 70)
            return True

        except Exception as exc:
            logger.error("Transformation failed: %s", exc, exc_info=True)
            return False


if __name__ == "__main__":
    transformer = StagingTransformer()
    transformer.run_all()
