"""
Complete CPI Data Pipeline
Run the entire extraction -> transformation -> S3 pipeline
"""
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import get_settings
from data_ingestion.cpi_extractor import CPIExtractor
from data_ingestion.db_loader import PostgreSQLLoader
from data_ingestion.dosm_client import DOSMClient
from data_ingestion.mart_transformer import MartTransformer
from data_ingestion.s3_uploader import S3Uploader
from data_ingestion.staging_transformer import StagingTransformer

settings = get_settings()
settings.ensure_runtime_dirs()
log_file = settings.logs_dir / f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def main() -> int:
    """Run complete pipeline."""
    logger.info("=" * 80)
    logger.info("MALAYSIAN CPI ANALYTICS - FULL PIPELINE")
    logger.info("=" * 80)

    try:
        logger.info("STEP 1: EXTRACTING DATA FROM DOSM")
        logger.info("-" * 80)

        client = DOSMClient()
        extractor = CPIExtractor(client)
        df_cpi = extractor.extract_full(save_path=settings.raw_data_dir / "cpi_latest.parquet")

        df_categories = client.get_categories()
        df_categories.to_parquet(settings.raw_data_dir / "categories.parquet", index=False)
        logger.info(
            "Extracted %s CPI records and %s categories",
            f"{len(df_cpi):,}",
            len(df_categories),
        )

        logger.info("STEP 2: LOADING TO POSTGRESQL")
        logger.info("-" * 80)

        loader = PostgreSQLLoader()
        loader.load_to_raw(df_cpi, "cpi_data", if_exists="replace")
        loader.load_to_raw(df_categories, "categories", if_exists="replace")
        logger.info("Data loaded to raw schema")

        logger.info("STEP 3: STAGING TRANSFORMATION")
        logger.info("-" * 80)
        StagingTransformer().run_all()

        logger.info("STEP 4: MART TRANSFORMATION")
        logger.info("-" * 80)
        MartTransformer().run_all()

        logger.info("STEP 5: UPLOADING TO AWS S3")
        logger.info("-" * 80)
        results = S3Uploader().upload_data_backup()
        if results.get("skipped"):
            logger.info("S3 upload skipped")
        else:
            logger.info("Uploaded %s files to S3", len(results["uploaded"]))

        logger.info("=" * 80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info("CPI Records: %s", f"{len(df_cpi):,}")
        logger.info("Categories: %s", len(df_categories))
        logger.info("S3 Files: %s", len(results["uploaded"]))
        logger.info("Log File: %s", log_file)
        logger.info("=" * 80)
        return 0

    except Exception as exc:
        logger.error("PIPELINE FAILED: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
