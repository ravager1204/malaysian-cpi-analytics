import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import uuid4

from data_ingestion.s3_uploader import S3Uploader


def test_upload_data_backup_collects_success_and_failures():
    temp_root = Path("tests") / "_artifacts" / "s3" / str(uuid4())
    raw_dir = temp_root / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "cpi_latest.parquet").write_text("dummy", encoding="utf-8")

    fake_client = MagicMock()

    with patch("data_ingestion.s3_uploader.boto3.client", return_value=fake_client):
        uploader = S3Uploader()
        aws_settings = uploader.settings.aws.__class__(
            access_key_id=uploader.settings.aws.access_key_id,
            secret_access_key=uploader.settings.aws.secret_access_key,
            region=uploader.settings.aws.region,
            bucket_name=uploader.settings.aws.bucket_name,
            enable_upload=True,
        )
        uploader.settings = uploader.settings.__class__(
            project_root=temp_root,
            raw_data_dir=raw_dir,
            processed_data_dir=temp_root / "processed",
            outputs_dir=temp_root / "outputs",
            logs_dir=temp_root / "logs",
            environment="test",
            project_name="test-project",
            database=uploader.settings.database,
            aws=aws_settings,
        )
        results = uploader.upload_data_backup(date_partition="2026-03-26")

    assert len(results["uploaded"]) == 1
    assert len(results["failed"]) == 1
    fake_client.upload_file.assert_called_once()

    shutil.rmtree(temp_root)
