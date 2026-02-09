"""
Main CPI Data Extraction Script
Production script to extract, validate, and load CPI data
"""
import sys
from pathlib import Path
from datetime import datetime
import logging

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_ingestion.dosm_client import DOSMClient
from data_ingestion.cpi_extractor import CPIExtractor
from data_ingestion.db_loader import PostgreSQLLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/extraction_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def main():
    """Main extraction pipeline"""
    
    logger.info("=" * 70)
    logger.info("STARTING CPI DATA EXTRACTION PIPELINE")
    logger.info("=" * 70)
    
    try:
        # Step 1: Initialize components
        logger.info("\n1️⃣  Initializing components...")
        client = DOSMClient()
        extractor = CPIExtractor(client)
        loader = PostgreSQLLoader()
        
        # Step 2: Extract data
        logger.info("\n2️⃣  Extracting CPI data from DOSM...")
        df = extractor.extract_full(
            save_path=Path('data/raw/cpi_latest.parquet')
        )
        
        # Step 3: Load to database
        logger.info("\n3️⃣  Loading data to PostgreSQL...")
        records = loader.load_to_raw(df, 'cpi_data', if_exists='replace')
        
        # Step 4: Extract categories
        logger.info("\n4️⃣  Extracting category lookup...")
        df_categories = client.get_categories()
        df_categories.to_parquet('data/raw/categories.parquet', index=False)
        loader.load_to_raw(df_categories, 'categories', if_exists='replace')
        
        # Success summary
        logger.info("\n" + "=" * 70)
        logger.info("✅ PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("=" * 70)
        logger.info(f"📊 CPI Records: {records:,}")
        logger.info(f"📂 Categories: {len(df_categories):,}")
        logger.info(f"💾 Files saved to: data/raw/")
        logger.info(f"🗄️  Database: cpi_analytics (raw schema)")
        logger.info("=" * 70)
        
        return 0
        
    except Exception as e:
        logger.error(f"\n❌ PIPELINE FAILED: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)