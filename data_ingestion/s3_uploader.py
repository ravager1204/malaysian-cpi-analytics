"""
S3 Uploader
Upload data files to AWS S3 for backup and cloud storage
"""
import logging
from datetime import datetime
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from config.settings import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3Uploader:
    """Upload files to AWS S3."""

    def __init__(self):
        """Initialize S3 client."""
        settings = get_settings()
        self.settings = settings
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=settings.aws.access_key_id,
            aws_secret_access_key=settings.aws.secret_access_key,
            region_name=settings.aws.region,
        )
        self.bucket_name = settings.aws.bucket_name
        logger.info("S3 client initialized for bucket: %s", self.bucket_name)

    def upload_file(self, local_path: Path, s3_key: str) -> bool:
        """Upload a single file to S3."""
        try:
            logger.info("Uploading %s to s3://%s/%s", local_path, self.bucket_name, s3_key)
            self.s3_client.upload_file(str(local_path), self.bucket_name, s3_key)
            logger.info("Uploaded successfully")
            return True
        except ClientError as exc:
            logger.error("Upload failed: %s", exc)
            return False

    def upload_data_backup(self, date_partition: str | None = None) -> dict:
        """Upload all raw data files to S3 with date partitioning."""
        if not self.settings.aws.enable_upload:
            logger.info("S3 upload disabled by configuration; skipping backup step")
            return {"date": date_partition, "uploaded": [], "failed": [], "skipped": True}

        if not date_partition:
            date_partition = datetime.now().strftime("%Y-%m-%d")

        logger.info("=" * 70)
        logger.info("STARTING S3 BACKUP FOR %s", date_partition)
        logger.info("=" * 70)

        results = {"date": date_partition, "uploaded": [], "failed": []}

        files_to_upload = [
            {
                "local": self.settings.raw_data_dir / "cpi_latest.parquet",
                "s3_key": f"raw/cpi/date={date_partition}/cpi_data.parquet",
            },
            {
                "local": self.settings.raw_data_dir / "categories.parquet",
                "s3_key": f"raw/categories/date={date_partition}/categories.parquet",
            },
        ]

        for file_info in files_to_upload:
            local_path = file_info["local"]
            s3_key = file_info["s3_key"]

            if not local_path.exists():
                logger.warning("File not found: %s", local_path)
                results["failed"].append(str(local_path))
                continue

            success = self.upload_file(local_path, s3_key)
            if success:
                results["uploaded"].append(s3_key)
            else:
                results["failed"].append(str(local_path))

        logger.info("\n%s", "=" * 70)
        logger.info("BACKUP SUMMARY")
        logger.info("=" * 70)
        logger.info("Uploaded: %s files", len(results["uploaded"]))
        logger.info("Failed: %s files", len(results["failed"]))

        if results["uploaded"]:
            logger.info("Uploaded files:")
            for key in results["uploaded"]:
                logger.info("  - s3://%s/%s", self.bucket_name, key)

        logger.info("=" * 70)
        return results

    def list_bucket_contents(self, prefix: str = "") -> list[str]:
        """List objects in S3 bucket."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
            )
            if "Contents" in response:
                objects = [obj["Key"] for obj in response["Contents"]]
                logger.info(
                    "Found %s objects in s3://%s/%s",
                    len(objects),
                    self.bucket_name,
                    prefix,
                )
                return objects

            logger.info("No objects found in s3://%s/%s", self.bucket_name, prefix)
            return []
        except ClientError as exc:
            logger.error("Failed to list bucket: %s", exc)
            return []


if __name__ == "__main__":
    uploader = S3Uploader()
    uploader.upload_data_backup()
