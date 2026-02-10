"""
CPI Data Pipeline DAG
Automated daily extraction, transformation, and upload to S3
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add project path
project_path = Path('D:/VS Code Projects/malaysian-cpi-analytics')
sys.path.insert(0, str(project_path))

from data_ingestion.dosm_client import DOSMClient
from data_ingestion.cpi_extractor import CPIExtractor
from data_ingestion.db_loader import PostgreSQLLoader
from data_ingestion.staging_transformer import StagingTransformer
from data_ingestion.mart_transformer import MartTransformer
from data_ingestion.s3_uploader import S3Uploader


# Default arguments
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


# Task functions
def extract_cpi_data(**context):
    """Extract CPI data from DOSM"""
    print("Extracting CPI data...")
    client = DOSMClient()
    extractor = CPIExtractor(client)
    
    df = extractor.extract_full(
        save_path=project_path / 'data/raw/cpi_latest.parquet'
    )
    
    print(f"✅ Extracted {len(df):,} records")
    return len(df)


def extract_categories(**context):
    """Extract category lookup"""
    print("Extracting categories...")
    client = DOSMClient()
    df = client.get_categories()
    df.to_parquet(project_path / 'data/raw/categories.parquet', index=False)
    
    print(f"✅ Extracted {len(df)} categories")
    return len(df)


def load_to_database(**context):
    """Load raw data to PostgreSQL"""
    print("Loading to database...")
    import pandas as pd
    
    loader = PostgreSQLLoader()
    
    # Load CPI data
    df_cpi = pd.read_parquet(project_path / 'data/raw/cpi_latest.parquet')
    cpi_count = loader.load_to_raw(df_cpi, 'cpi_data', if_exists='replace')
    
    # Load categories
    df_cat = pd.read_parquet(project_path / 'data/raw/categories.parquet')
    cat_count = loader.load_to_raw(df_cat, 'categories', if_exists='replace')
    
    print(f"✅ Loaded {cpi_count:,} CPI records and {cat_count} categories")
    return {'cpi': cpi_count, 'categories': cat_count}


def transform_staging(**context):
    """Transform to staging layer"""
    print("Transforming to staging...")
    transformer = StagingTransformer()
    transformer.run_all()
    print("✅ Staging transformation complete")


def transform_mart(**context):
    """Transform to mart layer"""
    print("Transforming to mart...")
    transformer = MartTransformer()
    transformer.run_all()
    print("✅ Mart transformation complete")


def upload_to_s3(**context):
    """Upload to S3"""
    print("Uploading to S3...")
    uploader = S3Uploader()
    results = uploader.upload_data_backup()
    
    print(f"✅ Uploaded {len(results['uploaded'])} files to S3")
    return results


# Define DAG
with DAG(
    'cpi_data_pipeline',
    default_args=default_args,
    description='Malaysian CPI data extraction and transformation pipeline',
    schedule_interval='0 9 * * *',  # Daily at 9 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['cpi', 'data-engineering', 'etl'],
) as dag:
    
    # Task 1: Extract CPI data
    task_extract_cpi = PythonOperator(
        task_id='extract_cpi_data',
        python_callable=extract_cpi_data,
    )
    
    # Task 2: Extract categories
    task_extract_categories = PythonOperator(
        task_id='extract_categories',
        python_callable=extract_categories,
    )
    
    # Task 3: Load to database
    task_load_db = PythonOperator(
        task_id='load_to_database',
        python_callable=load_to_database,
    )
    
    # Task 4: Transform to staging
    task_staging = PythonOperator(
        task_id='transform_staging',
        python_callable=transform_staging,
    )
    
    # Task 5: Transform to mart
    task_mart = PythonOperator(
        task_id='transform_mart',
        python_callable=transform_mart,
    )
    
    # Task 6: Upload to S3
    task_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
    )
    
    # Define task dependencies
    [task_extract_cpi, task_extract_categories] >> task_load_db >> task_staging >> task_mart >> task_s3