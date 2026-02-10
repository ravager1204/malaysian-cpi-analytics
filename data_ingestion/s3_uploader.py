"""
S3 Uploader
Upload data files to AWS S3 for backup and cloud storage
"""
import boto3
from botocore.exceptions import ClientError
import logging
from pathlib import Path
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3Uploader:
    """Upload files to AWS S3"""
    
    def __init__(self):
        """Initialize S3 client"""
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
        logger.info(f"✅ S3 client initialized for bucket: {self.bucket_name}")
    
    def upload_file(self, local_path: Path, s3_key: str) -> bool:
        """
        Upload a single file to S3
        
        Args:
            local_path: Path to local file
            s3_key: S3 object key (path in bucket)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Uploading {local_path} to s3://{self.bucket_name}/{s3_key}")
            
            self.s3_client.upload_file(
                str(local_path),
                self.bucket_name,
                s3_key
            )
            
            logger.info(f"✅ Uploaded successfully")
            return True
            
        except ClientError as e:
            logger.error(f"❌ Upload failed: {e}")
            return False
    
    def upload_data_backup(self, date_partition: str = None) -> dict:
        """
        Upload all raw data files to S3 with date partitioning
        
        Args:
            date_partition: Date string (YYYY-MM-DD), defaults to today
            
        Returns:
            Dictionary with upload results
        """
        if not date_partition:
            date_partition = datetime.now().strftime('%Y-%m-%d')
        
        logger.info("=" * 70)
        logger.info(f"STARTING S3 BACKUP FOR {date_partition}")
        logger.info("=" * 70)
        
        results = {
            'date': date_partition,
            'uploaded': [],
            'failed': []
        }
        
        # Define files to upload
        files_to_upload = [
            {
                'local': Path('data/raw/cpi_latest.parquet'),
                's3_key': f'raw/cpi/date={date_partition}/cpi_data.parquet'
            },
            {
                'local': Path('data/raw/categories.parquet'),
                's3_key': f'raw/categories/date={date_partition}/categories.parquet'
            }
        ]
        
        # Upload each file
        for file_info in files_to_upload:
            local_path = file_info['local']
            s3_key = file_info['s3_key']
            
            if not local_path.exists():
                logger.warning(f"⚠️  File not found: {local_path}")
                results['failed'].append(str(local_path))
                continue
            
            success = self.upload_file(local_path, s3_key)
            
            if success:
                results['uploaded'].append(s3_key)
            else:
                results['failed'].append(str(local_path))
        
        # Summary
        logger.info("\n" + "=" * 70)
        logger.info("BACKUP SUMMARY")
        logger.info("=" * 70)
        logger.info(f"✅ Uploaded: {len(results['uploaded'])} files")
        logger.info(f"❌ Failed: {len(results['failed'])} files")
        
        if results['uploaded']:
            logger.info("\nUploaded files:")
            for key in results['uploaded']:
                logger.info(f"  - s3://{self.bucket_name}/{key}")
        
        logger.info("=" * 70)
        
        return results
    
    def list_bucket_contents(self, prefix: str = '') -> list:
        """List objects in S3 bucket"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' in response:
                objects = [obj['Key'] for obj in response['Contents']]
                logger.info(f"Found {len(objects)} objects in s3://{self.bucket_name}/{prefix}")
                return objects
            else:
                logger.info(f"No objects found in s3://{self.bucket_name}/{prefix}")
                return []
                
        except ClientError as e:
            logger.error(f"❌ Failed to list bucket: {e}")
            return []


if __name__ == "__main__":
    uploader = S3Uploader()
    uploader.upload_data_backup()