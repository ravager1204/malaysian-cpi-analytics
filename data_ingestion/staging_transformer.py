"""
Staging Data Transformer
Transform raw data into clean staging tables
"""
import pandas as pd
import logging
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StagingTransformer:
    """Transform raw data to staging layer"""
    
    def __init__(self):
        self.engine = create_engine(
            f'postgresql://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@'
            f'{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}'
        )
        logger.info("✅ Staging transformer initialized")
    
    def transform_categories(self):
        """Transform category lookup to staging"""
        logger.info("Transforming categories...")
        
        # Get 2-digit categories only
        df = pd.read_sql("""
            SELECT 
                division,
                desc_en as category_name_en,
                desc_bm as category_name_bm,
                digits as category_level
            FROM raw.categories
            WHERE digits = 2
        """, self.engine)
        
        # Load to staging
        df.to_sql(
            'categories',
            self.engine,
            schema='staging',
            if_exists='replace',
            index=False
        )
        
        logger.info(f"✅ Loaded {len(df)} categories to staging")
        return len(df)
    
    def transform_cpi_monthly(self):
        """Transform CPI data with category names"""
        logger.info("Transforming CPI monthly data...")
        
        # Join raw CPI with categories
        df = pd.read_sql("""
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
        """, self.engine)
        
        logger.info(f"Processing {len(df):,} records...")
        
        # Load to staging in chunks
        df.to_sql(
            'cpi_monthly',
            self.engine,
            schema='staging',
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=5000
        )
        
        logger.info(f"✅ Loaded {len(df):,} records to staging.cpi_monthly")
        return len(df)
    
    def validate_staging(self):
        """Validate staging data quality"""
        logger.info("Validating staging data...")
        
        # Check counts
        raw_count = pd.read_sql(
            "SELECT COUNT(*) as cnt FROM raw.cpi_data", 
            self.engine
        ).iloc[0]['cnt']
        
        stg_count = pd.read_sql(
            "SELECT COUNT(*) as cnt FROM staging.cpi_monthly", 
            self.engine
        ).iloc[0]['cnt']
        
        if raw_count != stg_count:
            logger.warning(f"⚠️ Row count mismatch: raw={raw_count}, staging={stg_count}")
        else:
            logger.info(f"✅ Row counts match: {stg_count:,}")
        
        # Check for nulls
        nulls = pd.read_sql("""
            SELECT 
                COUNT(*) FILTER (WHERE state IS NULL) as null_state,
                COUNT(*) FILTER (WHERE date IS NULL) as null_date,
                COUNT(*) FILTER (WHERE index_value IS NULL) as null_index
            FROM staging.cpi_monthly
        """, self.engine)
        
        if nulls.sum().sum() > 0:
            logger.warning(f"⚠️ Found nulls: {nulls}")
        else:
            logger.info("✅ No nulls in critical columns")
        
        return True
    
    def run_all(self):
        """Run complete staging transformation"""
        logger.info("=" * 70)
        logger.info("STARTING STAGING TRANSFORMATION")
        logger.info("=" * 70)
        
        try:
            # Transform categories
            cat_count = self.transform_categories()
            
            # Transform CPI data
            cpi_count = self.transform_cpi_monthly()
            
            # Validate
            self.validate_staging()
            
            logger.info("\n" + "=" * 70)
            logger.info("✅ STAGING TRANSFORMATION COMPLETE")
            logger.info("=" * 70)
            logger.info(f"Categories: {cat_count}")
            logger.info(f"CPI Records: {cpi_count:,}")
            logger.info("=" * 70)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Transformation failed: {e}", exc_info=True)
            return False


if __name__ == "__main__":
    transformer = StagingTransformer()
    transformer.run_all()