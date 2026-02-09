"""
PostgreSQL Database Loader
Loads data into PostgreSQL with proper error handling
"""
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgreSQLLoader:
    """Load data to PostgreSQL database"""
    
    def __init__(self):
        """Initialize database connection"""
        self.engine = create_engine(
            f'postgresql://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@'
            f'{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}'
        )
        logger.info("✅ Database connection initialized")
    
    def load_to_raw(self, df: pd.DataFrame, table_name: str, 
                    if_exists: str = 'replace') -> int:
        """
        Load data to raw schema
        
        Args:
            df: DataFrame to load
            table_name: Name of table
            if_exists: 'replace', 'append', or 'fail'
            
        Returns:
            Number of records loaded
        """
        try:
            logger.info(f"Loading {len(df):,} records to raw.{table_name}...")
            
            df.to_sql(
                table_name,
                self.engine,
                schema='raw',
                if_exists=if_exists,
                index=False,
                method='multi',
                chunksize=1000
            )
            
            # Log to metadata table
            self._log_load(table_name, len(df), 'SUCCESS')
            
            logger.info(f"✅ Loaded {len(df):,} records to raw.{table_name}")
            return len(df)
            
        except Exception as e:
            logger.error(f"❌ Load failed: {e}")
            self._log_load(table_name, 0, 'FAILED', str(e))
            raise
    
    def _log_load(self, table_name: str, records: int, 
                  status: str, error: Optional[str] = None):
        """Log load to metadata table"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO raw.load_metadata 
                    (table_name, records_loaded, load_status, error_message)
                    VALUES (:table, :records, :status, :error)
                """), {
                    'table': table_name,
                    'records': records,
                    'status': status,
                    'error': error
                })
                conn.commit()
        except Exception as e:
            logger.warning(f"Could not log to metadata: {e}")
    
    def get_load_history(self) -> pd.DataFrame:
        """Get load history from metadata table"""
        return pd.read_sql(
            "SELECT * FROM raw.load_metadata ORDER BY load_timestamp DESC LIMIT 10",
            self.engine
        )