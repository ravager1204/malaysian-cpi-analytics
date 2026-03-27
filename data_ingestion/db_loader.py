"""
PostgreSQL Database Loader
Loads data into PostgreSQL with proper error handling
"""
import logging

import pandas as pd
from sqlalchemy import create_engine, text

from config.settings import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgreSQLLoader:
    """Load data to PostgreSQL database."""

    def __init__(self):
        """Initialize database connection."""
        settings = get_settings()
        self.engine = create_engine(settings.database.sqlalchemy_url)
        logger.info("Database connection initialized")

    def load_to_raw(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "replace",
    ) -> int:
        """
        Load data to raw schema.

        Args:
            df: DataFrame to load
            table_name: Name of table
            if_exists: 'replace', 'append', or 'fail'

        Returns:
            Number of records loaded
        """
        try:
            logger.info("Loading %s records to raw.%s", f"{len(df):,}", table_name)

            df.to_sql(
                table_name,
                self.engine,
                schema="raw",
                if_exists=if_exists,
                index=False,
                method="multi",
                chunksize=1000,
            )

            self._log_load(table_name, len(df), "SUCCESS")
            logger.info("Loaded %s records to raw.%s", f"{len(df):,}", table_name)
            return len(df)

        except Exception as exc:
            logger.error("Load failed: %s", exc)
            self._log_load(table_name, 0, "FAILED", str(exc))
            raise

    def _log_load(
        self,
        table_name: str,
        records: int,
        status: str,
        error: str | None = None,
    ) -> None:
        """Log load to metadata table."""
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO raw.load_metadata
                        (table_name, records_loaded, load_status, error_message)
                        VALUES (:table, :records, :status, :error)
                        """
                    ),
                    {
                        "table": table_name,
                        "records": records,
                        "status": status,
                        "error": error,
                    },
                )
                conn.commit()
        except Exception as exc:
            logger.warning("Could not log to metadata: %s", exc)

    def get_load_history(self) -> pd.DataFrame:
        """Get load history from metadata table."""
        return pd.read_sql(
            "SELECT * FROM raw.load_metadata ORDER BY load_timestamp DESC LIMIT 10",
            self.engine,
        )
