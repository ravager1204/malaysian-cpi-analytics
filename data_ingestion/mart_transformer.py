"""
Mart Data Transformer
Calculate inflation metrics and build analytical tables
"""
import pandas as pd
import logging
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MartTransformer:
    """Build mart layer with business metrics"""
    
    def __init__(self):
        self.engine = create_engine(
            f'postgresql://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@'
            f'{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}'
        )
        logger.info("✅ Mart transformer initialized")
    
    def build_inflation_by_state(self):
        """Calculate state-level inflation metrics"""
        logger.info("Building inflation by state...")
        
        # Get overall CPI by state with window functions for MoM and YoY
        query = """
        WITH cpi_with_lag AS (
            SELECT 
                state,
                date,
                index_value,
                LAG(index_value, 1) OVER (PARTITION BY state ORDER BY date) as prev_month_index,
                LAG(index_value, 12) OVER (PARTITION BY state ORDER BY date) as prev_year_index
            FROM staging.cpi_monthly
            WHERE division = 'overall'
        )
        SELECT 
            state,
            date,
            index_value,
            CASE 
                WHEN prev_month_index IS NOT NULL 
                THEN ((index_value / prev_month_index) - 1) * 100
                ELSE NULL 
            END as mom_change,
            CASE 
                WHEN prev_year_index IS NOT NULL 
                THEN ((index_value / prev_year_index) - 1) * 100
                ELSE NULL 
            END as yoy_change,
            CASE 
                WHEN prev_year_index IS NOT NULL 
                THEN ((index_value / prev_year_index) - 1) * 100
                ELSE NULL 
            END as inflation_rate
        FROM cpi_with_lag
        ORDER BY state, date
        """
        
        df = pd.read_sql(query, self.engine)
        logger.info(f"Calculated inflation for {len(df):,} state-month combinations")
        
        # Load to mart
        df.to_sql(
            'inflation_by_state',
            self.engine,
            schema='mart',
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=5000
        )
        
        logger.info(f"✅ Loaded {len(df):,} records to mart.inflation_by_state")
        return len(df)
    
    def build_inflation_by_category(self):
        """Calculate category-level inflation (national average)"""
        logger.info("Building inflation by category...")
        
        query = """
        WITH category_avg AS (
            SELECT 
                date,
                division,
                category_name,
                AVG(index_value) as avg_index
            FROM staging.cpi_monthly
            WHERE division != 'overall'
            GROUP BY date, division, category_name
        ),
        category_with_lag AS (
            SELECT 
                date,
                division,
                category_name,
                avg_index,
                LAG(avg_index, 1) OVER (PARTITION BY division ORDER BY date) as prev_month_index,
                LAG(avg_index, 12) OVER (PARTITION BY division ORDER BY date) as prev_year_index
            FROM category_avg
        )
        SELECT 
            date,
            division,
            category_name,
            avg_index,
            CASE 
                WHEN prev_month_index IS NOT NULL 
                THEN ((avg_index / prev_month_index) - 1) * 100
                ELSE NULL 
            END as mom_change,
            CASE 
                WHEN prev_year_index IS NOT NULL 
                THEN ((avg_index / prev_year_index) - 1) * 100
                ELSE NULL 
            END as yoy_change
        FROM category_with_lag
        ORDER BY date, division
        """
        
        df = pd.read_sql(query, self.engine)
        logger.info(f"Calculated inflation for {len(df):,} category-month combinations")
        
        # Load to mart
        df.to_sql(
            'inflation_by_category',
            self.engine,
            schema='mart',
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=5000
        )
        
        logger.info(f"✅ Loaded {len(df):,} records to mart.inflation_by_category")
        return len(df)
    
    def build_state_comparison(self):
        """Build latest month state comparison"""
        logger.info("Building state comparison...")
        
        query = """
        WITH latest_date AS (
            SELECT MAX(date) as max_date
            FROM staging.cpi_monthly
        ),
        latest_data AS (
            SELECT 
                c.state,
                c.date,
                c.division,
                c.index_value
            FROM staging.cpi_monthly c
            CROSS JOIN latest_date ld
            WHERE c.date = ld.max_date
        ),
        pivoted AS (
            SELECT 
                state,
                date as latest_date,
                MAX(CASE WHEN division = 'overall' THEN index_value END) as overall_cpi,
                MAX(CASE WHEN division = '01' THEN index_value END) as food_cpi,
                MAX(CASE WHEN division = '04' THEN index_value END) as housing_cpi,
                MAX(CASE WHEN division = '07' THEN index_value END) as transport_cpi
            FROM latest_data
            GROUP BY state, date
        ),
        with_ranks AS (
            SELECT 
                p.*,
                RANK() OVER (ORDER BY overall_cpi DESC) as rank_overall,
                s.region
            FROM pivoted p
            LEFT JOIN staging.states s ON p.state = s.state_name
        ),
        with_comparison AS (
            SELECT 
                wr.*,
                ((wr.overall_cpi / (SELECT MIN(overall_cpi) FROM with_ranks)) - 1) * 100 as pct_vs_cheapest
            FROM with_ranks wr
        )
        SELECT * FROM with_comparison
        ORDER BY overall_cpi DESC
        """
        
        df = pd.read_sql(query, self.engine)
        logger.info(f"Calculated comparison for {len(df)} states")
        
        # Load to mart
        df.to_sql(
            'state_comparison',
            self.engine,
            schema='mart',
            if_exists='replace',
            index=False
        )
        
        logger.info(f"✅ Loaded {len(df)} records to mart.state_comparison")
        return len(df)
    
    def run_all(self):
        """Run complete mart transformation"""
        logger.info("=" * 70)
        logger.info("STARTING MART TRANSFORMATION")
        logger.info("=" * 70)
        
        try:
            # Build mart tables
            state_count = self.build_inflation_by_state()
            category_count = self.build_inflation_by_category()
            comparison_count = self.build_state_comparison()
            
            logger.info("\n" + "=" * 70)
            logger.info("✅ MART TRANSFORMATION COMPLETE")
            logger.info("=" * 70)
            logger.info(f"Inflation by State: {state_count:,} records")
            logger.info(f"Inflation by Category: {category_count:,} records")
            logger.info(f"State Comparison: {comparison_count} states")
            logger.info("=" * 70)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Transformation failed: {e}", exc_info=True)
            return False


if __name__ == "__main__":
    transformer = MartTransformer()
    transformer.run_all()