"""
CPI Data Pipeline DAG
Automated daily extraction, transformation, and upload to S3
"""
import sys
from datetime import timedelta
from pathlib import Path

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from airflow import DAG

project_path = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_path))

from config.settings import get_settings  # noqa: E402
from data_ingestion.cpi_extractor import CPIExtractor  # noqa: E402
from data_ingestion.db_loader import PostgreSQLLoader  # noqa: E402
from data_ingestion.dosm_client import DOSMClient  # noqa: E402
from data_ingestion.mart_transformer import MartTransformer  # noqa: E402
from data_ingestion.s3_uploader import S3Uploader  # noqa: E402
from data_ingestion.staging_transformer import StagingTransformer  # noqa: E402

settings = get_settings()
settings.ensure_runtime_dirs()

default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def extract_cpi_data(**context):
    """Extract CPI data from DOSM."""
    print("Extracting CPI data...")
    client = DOSMClient()
    extractor = CPIExtractor(client)
    df = extractor.extract_full(save_path=settings.raw_data_dir / "cpi_latest.parquet")
    print(f"Extracted {len(df):,} records")
    return len(df)


def extract_categories(**context):
    """Extract category lookup."""
    print("Extracting categories...")
    client = DOSMClient()
    df = client.get_categories()
    df.to_parquet(settings.raw_data_dir / "categories.parquet", index=False)
    print(f"Extracted {len(df)} categories")
    return len(df)


def load_to_database(**context):
    """Load raw data to PostgreSQL."""
    print("Loading to database...")
    import pandas as pd

    loader = PostgreSQLLoader()
    df_cpi = pd.read_parquet(settings.raw_data_dir / "cpi_latest.parquet")
    cpi_count = loader.load_to_raw(df_cpi, "cpi_data", if_exists="replace")

    df_cat = pd.read_parquet(settings.raw_data_dir / "categories.parquet")
    cat_count = loader.load_to_raw(df_cat, "categories", if_exists="replace")

    print(f"Loaded {cpi_count:,} CPI records and {cat_count} categories")
    return {"cpi": cpi_count, "categories": cat_count}


def transform_staging(**context):
    """Transform to staging layer."""
    print("Transforming to staging...")
    transformer = StagingTransformer()
    transformer.run_all()
    print("Staging transformation complete")


def transform_mart(**context):
    """Transform to mart layer."""
    print("Transforming to mart...")
    transformer = MartTransformer()
    transformer.run_all()
    print("Mart transformation complete")


def upload_to_s3(**context):
    """Upload to S3."""
    print("Uploading to S3...")
    uploader = S3Uploader()
    results = uploader.upload_data_backup()
    print(f"Uploaded {len(results['uploaded'])} files to S3")
    return results


with DAG(
    "cpi_data_pipeline",
    default_args=default_args,
    description="Malaysian CPI data extraction and transformation pipeline",
    schedule_interval="0 9 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["cpi", "data-engineering", "etl"],
) as dag:
    task_extract_cpi = PythonOperator(
        task_id="extract_cpi_data",
        python_callable=extract_cpi_data,
    )

    task_extract_categories = PythonOperator(
        task_id="extract_categories",
        python_callable=extract_categories,
    )

    task_load_db = PythonOperator(
        task_id="load_to_database",
        python_callable=load_to_database,
    )

    task_staging = PythonOperator(
        task_id="transform_staging",
        python_callable=transform_staging,
    )

    task_mart = PythonOperator(
        task_id="transform_mart",
        python_callable=transform_mart,
    )

    task_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    (
        [task_extract_cpi, task_extract_categories]
        >> task_load_db
        >> task_staging
        >> task_mart
        >> task_s3
    )
