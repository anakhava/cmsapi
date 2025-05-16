import os
import csv
import logging
from pathlib import Path
import boto3
from botocore.exceptions import ClientError
from utils import setup_logging, download_file

class S3Storage:
    def __init__(self, bucket_name: str):
        """
        Initialize S3 storage handler
        
        Args:
            bucket_name: Name of the S3 bucket
        """
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name
        self.logger = setup_logging('s3_storage')
    
    def exists(self, s3_key: str) -> bool:
        """
        Check if a file exists in S3
        
        Args:
            s3_key: S3 object key
            
        Returns:
            True if file exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise
    
    def upload_file(self, local_path: Path, s3_key: str) -> bool:
        """
        Upload a file to S3
        
        Args:
            local_path: Local path to the file
            s3_key: S3 object key
            
        Returns:
            True if upload successful, False otherwise
        """
        try:
            self.s3_client.upload_file(str(local_path), self.bucket_name, s3_key)
            return True
        except Exception as e:
            self.logger.error(f"Error uploading to S3: {e}")
            return False

class NDCDatabaseDownloader:
    def __init__(self, csv_path="ndc_database_urls.csv", output_dir="/Users/arianakhavan/Documents/reference_data", s3_bucket=None):
        self.csv_path = csv_path
        self.output_dir = Path(output_dir)
        self.logger = logging.getLogger(__name__)
        self.s3_bucket = s3_bucket
        self.s3_storage = S3Storage(s3_bucket) if s3_bucket else None
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def read_csv(self):
        """Read the CSV file containing download URLs."""
        try:
            with open(self.csv_path, 'r') as f:
                reader = csv.DictReader(f)
                return list(reader)
        except Exception as e:
            self.logger.error(f"Error reading CSV file: {str(e)}")
            return []

    def get_s3_key(self, file_name: str) -> str:
        """
        Generate S3 key for a file
        
        Args:
            file_name: Name of the file
            
        Returns:
            S3 object key
        """
        # Create S3 key structure: ndc-database/filename
        return f"ndc-database/{file_name}"

    def download_files(self):
        """Download all files from the CSV and optionally upload to S3."""
        files = self.read_csv()
        if not files:
            self.logger.error("No files found in CSV")
            return
        
        total_files = len(files)
        successful_downloads = 0
        failed_downloads = 0
        skipped_downloads = 0
        
        self.logger.info(f"Starting download of {total_files} files...")
        
        for file_info in files:
            file_name = file_info['File Name']
            download_url = file_info['Download URL']
            output_path = self.output_dir / file_name
            
            # Check if file already exists locally
            if output_path.exists():
                self.logger.info(f"File already exists locally: {file_name}")
                if self.s3_bucket:
                    s3_key = self.get_s3_key(file_name)
                    if self.s3_storage.exists(s3_key):
                        self.logger.info(f"File already exists in S3: {s3_key}")
                        skipped_downloads += 1
                        continue
                    else:
                        self.logger.info(f"Uploading existing file to S3: {s3_key}")
                        if self.s3_storage.upload_file(output_path, s3_key):
                            successful_downloads += 1
                            self.logger.info(f"Successfully uploaded to S3: {s3_key}")
                        else:
                            failed_downloads += 1
                            self.logger.error(f"Failed to upload to S3: {s3_key}")
                else:
                    skipped_downloads += 1
                continue
            
            try:
                self.logger.info(f"Downloading {file_name}...")
                if download_file(download_url, output_path, logger=self.logger):
                    successful_downloads += 1
                    self.logger.info(f"Successfully downloaded {file_name}")
                    
                    # Upload to S3 if configured
                    if self.s3_bucket:
                        s3_key = self.get_s3_key(file_name)
                        self.logger.info(f"Uploading to S3: {s3_key}")
                        if self.s3_storage.upload_file(output_path, s3_key):
                            self.logger.info(f"Successfully uploaded to S3: {s3_key}")
                        else:
                            self.logger.error(f"Failed to upload to S3: {s3_key}")
                            failed_downloads += 1
                else:
                    failed_downloads += 1
                    self.logger.error(f"Failed to download {file_name}")
            except Exception as e:
                failed_downloads += 1
                self.logger.error(f"Error downloading {file_name}: {str(e)}")
        
        # Print summary
        self.logger.info("\n=== Download Summary ===")
        self.logger.info(f"Total files: {total_files}")
        self.logger.info(f"Successfully downloaded: {successful_downloads}")
        self.logger.info(f"Failed downloads: {failed_downloads}")
        self.logger.info(f"Skipped (already exist): {skipped_downloads}")

def main():
    logger = setup_logging('ndc_downloader')
    # Configure S3 bucket name if needed
    S3_BUCKET = 'downloaded-data'  # Set to None to disable S3 upload
    logger.info(f"Initializing NDC Database Downloader with S3 bucket: {S3_BUCKET}")
    downloader = NDCDatabaseDownloader(s3_bucket=S3_BUCKET)
    logger.info("Starting download process...")
    downloader.download_files()
    logger.info("Download process completed.")

if __name__ == "__main__":
    main() 