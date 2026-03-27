"""
Main CPI Data Extraction Script
Production script to extract, validate, and load CPI data
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

settings = get_settings()
settings.ensure_runtime_dirs()
extraction_log = settings.logs_dir / f"extraction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(extraction_log),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def main() -> int:
    """Main extraction pipeline."""
    logger.info("=" * 70)
    logger.info("STARTING CPI DATA EXTRACTION PIPELINE")
    logger.info("=" * 70)

    try:
        logger.info("Initializing components")
        client = DOSMClient()
        extractor = CPIExtractor(client)
        loader = PostgreSQLLoader()

        logger.info("Extracting CPI data from DOSM")
        df = extractor.extract_full(save_path=settings.raw_data_dir / "cpi_latest.parquet")

        logger.info("Loading data to PostgreSQL")
        records = loader.load_to_raw(df, "cpi_data", if_exists="replace")

        logger.info("Extracting category lookup")
        df_categories = client.get_categories()
        df_categories.to_parquet(settings.raw_data_dir / "categories.parquet", index=False)
        loader.load_to_raw(df_categories, "categories", if_exists="replace")

        logger.info("=" * 70)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)
        logger.info("CPI Records: %s", f"{records:,}")
        logger.info("Categories: %s", f"{len(df_categories):,}")
        logger.info("Files saved to: %s", settings.raw_data_dir)
        logger.info("Database: %s", settings.database.name)
        logger.info("=" * 70)
        return 0

    except Exception as exc:
        logger.error("PIPELINE FAILED: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
