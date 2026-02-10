"""
Complete CPI Data Pipeline
Run the entire extraction → transformation → S3 pipeline
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_ingestion.dosm_client import DOSMClient
from data_ingestion.cpi_extractor import CPIExtractor
from data_ingestion.db_loader import PostgreSQLLoader
from data_ingestion.staging_transformer import StagingTransformer
from data_ingestion.mart_transformer import MartTransformer
from data_ingestion.s3_uploader import S3Uploader
import logging
from datetime import datetime

# Setup logging
log_file = f'logs/pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def main():
    """Run complete pipeline"""
    
    logger.info("=" * 80)
    logger.info("MALAYSIAN CPI ANALYTICS - FULL PIPELINE")
    logger.info("=" * 80)
    
    try:
        # Step 1: Extract
        logger.info("\n🔽 STEP 1: EXTRACTING DATA FROM DOSM")
        logger.info("-" * 80)
        
        client = DOSMClient()
        extractor = CPIExtractor(client)
        
        df_cpi = extractor.extract_full(
            save_path=Path('data/raw/cpi_latest.parquet')
        )
        
        df_categories = client.get_categories()
        df_categories.to_parquet('data/raw/categories.parquet', index=False)
        logger.info(f"✅ Extracted {len(df_cpi):,} CPI records and {len(df_categories)} categories")
        
        # Step 2: Load to Database
        logger.info("\n💾 STEP 2: LOADING TO POSTGRESQL")
        logger.info("-" * 80)
        
        loader = PostgreSQLLoader()
        loader.load_to_raw(df_cpi, 'cpi_data', if_exists='replace')
        loader.load_to_raw(df_categories, 'categories', if_exists='replace')
        logger.info("✅ Data loaded to raw schema")
        
        # Step 3: Staging Transformation
        logger.info("\n🔄 STEP 3: STAGING TRANSFORMATION")
        logger.info("-" * 80)
        
        staging = StagingTransformer()
        staging.run_all()
        logger.info("✅ Staging transformation complete")
        
        # Step 4: Mart Transformation
        logger.info("\n📊 STEP 4: MART TRANSFORMATION")
        logger.info("-" * 80)
        
        mart = MartTransformer()
        mart.run_all()
        logger.info("✅ Mart transformation complete")
        
        # Step 5: Upload to S3
        logger.info("\n☁️  STEP 5: UPLOADING TO AWS S3")
        logger.info("-" * 80)
        
        uploader = S3Uploader()
        results = uploader.upload_data_backup()
        logger.info(f"✅ Uploaded {len(results['uploaded'])} files to S3")
        
        # Success!
        logger.info("\n" + "=" * 80)
        logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        logger.info(f"📊 CPI Records: {len(df_cpi):,}")
        logger.info(f"📂 Categories: {len(df_categories)}")
        logger.info(f"☁️  S3 Files: {len(results['uploaded'])}")
        logger.info(f"📝 Log File: {log_file}")
        logger.info("=" * 80)
        
        return 0
        
    except Exception as e:
        logger.error(f"\n❌ PIPELINE FAILED: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)