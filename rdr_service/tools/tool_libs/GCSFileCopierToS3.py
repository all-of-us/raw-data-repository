import os
import datetime
import logging

import boto3
from google.cloud import storage

from rdr_service import config


class GCSFileCopierToS3:
    def __init__(self, gcs_bucket: str, s3_bucket: str):
        self.gcs_bucket_name = gcs_bucket
        self.s3_bucket = s3_bucket

        self.gcs_client = storage.Client()
        self.gcs_bucket = self.gcs_client.bucket(self.gcs_bucket_name)

        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=config.getSettingJson(config.AWS_ACCESS_KEY_ID)[0],
            aws_secret_access_key=config.getSettingJson(config.AWS_SECRET_ACCESS_KEY)[0],
            region_name=config.getSettingJson(config.AWS_REGION_NAME)[0]
        )

    @staticmethod
    def get_date_prefix(directory: str) -> str:
        """Return the GCS prefix for today's date."""
        today_date = datetime.datetime.now().strftime("%Y-%m-%d")
        return os.path.join(directory, f"run_date={today_date}")

    def list_gcs_files(self, directory: str) -> list:
        """Yield all file blobs for today's run_date folder."""
        prefix = self.get_date_prefix(directory)
        blobs = self.gcs_bucket.list_blobs(prefix=prefix)
        return [blob for blob in blobs if not blob.name.endswith('/')]

    def copy_file_to_s3(self, key: str, file_body: bytes) -> None:
        """Upload a file directly to S3."""
        self.s3_client.put_object(
            Bucket=self.s3_bucket,
            Key=key,
            Body=file_body
        )

    def process_directory(self, gcs_directory: str, ppsc_directory: str) -> None:
        """Process a directory: list files, download, upload."""
        prefix = self.get_date_prefix(gcs_directory)
        blobs = self.list_gcs_files(gcs_directory)
        if not blobs:
            raise FileNotFoundError(f"{prefix} not found")

        for blob in blobs:
            logging.info(f"Downloading GCS file: {blob.name}")
            content = blob.download_as_string()
            s3_key = f"{ppsc_directory}/{os.path.basename(prefix)}/{os.path.basename(blob.name)}"
            logging.info(f"Uploading to S3: s3://{self.s3_bucket}/{s3_key}")
            self.copy_file_to_s3(s3_key, content)
            logging.info(f"Successfully copied {blob.name} to {s3_key}")

    def run(self, directories: dict[str]) -> None:
        for gcs_dir, ppsc_dir in directories.items():
            self.process_directory(gcs_dir, ppsc_dir)
