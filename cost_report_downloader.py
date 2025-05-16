"""
Download cost report files from URLs listed in cost_reports.csv and upload to S3.
"""
import csv
import time
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from utils import setup_logging, download_file, clean_url, get_file_extension

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

class CostReportDownloader:
    def __init__(self, csv_path: str, s3_bucket: str):
        """
        Initialize the downloader
        
        Args:
            csv_path: Path to the cost_reports.csv file
            s3_bucket: Name of the S3 bucket
        """
        self.logger = setup_logging('cost_report_downloader')
        self.csv_path = Path(csv_path)
        self.s3_storage = S3Storage(s3_bucket)
        self.stats = {
            'total': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0
        }
        
    def load_reports(self) -> List[Dict]:
        """
        Load cost report information from CSV
        
        Returns:
            List of dictionaries containing report information
        """
        try:
            df = pd.read_csv(self.csv_path)
            self.logger.info(f"Loaded {len(df)} reports from {self.csv_path}")
            return df.to_dict('records')
        except Exception as e:
            self.logger.error(f"Error loading CSV file: {e}")
            return []
    
    def get_s3_key(self, report: Dict) -> str:
        """
        Generate S3 key for a report
        
        Args:
            report: Dictionary containing report information
            
        Returns:
            S3 object key
        """
        # Create S3 key structure: cost-reports/facility_type/year/filename
        facility_type = report['facility_type']
        year = str(report['year'])
        
        # Get file extension from URL or default to .zip
        ext = get_file_extension(report['download_url'])
        
        # Use file_name from CSV if available, otherwise generate one
        if report['file_name']:
            filename = f"{report['file_name']}{ext}"
        else:
            filename = f"{report['facility_type']}_{report['year']}{ext}"
        
        return f"cost-reports/{facility_type}/{year}/{filename}"
    
    def download_reports(self, reports: List[Dict], delay: float = 1.0) -> None:
        """
        Download cost report files and upload to S3
        
        Args:
            reports: List of report dictionaries
            delay: Delay between downloads in seconds
        """
        self.stats['total'] = len(reports)
        
        for i, report in enumerate(reports, 1):
            self.logger.info(f"\nProcessing report {i}/{len(reports)}:")
            self.logger.info(f"  Year: {report['year']}")
            self.logger.info(f"  Type: {report['facility_type']}")
            
            # Skip if no download URL
            if not report['download_url']:
                self.logger.warning("  No download URL available, skipping")
                self.stats['skipped'] += 1
                continue
            
            # Clean URL
            url = clean_url(report['download_url'])
            
            # Get S3 key
            s3_key = self.get_s3_key(report)
            
            # Skip if file already exists in S3
            if self.s3_storage.exists(s3_key):
                self.logger.info(f"  File already exists in S3: {s3_key}")
                self.stats['skipped'] += 1
                continue
            
            # Create temporary local file
            temp_path = Path(f"temp_{report['facility_type']}_{report['year']}{get_file_extension(url)}")
            
            try:
                # Download file
                self.logger.info(f"  Downloading to temporary file: {temp_path}")
                if not download_file(url, temp_path, logger=self.logger):
                    self.logger.error("  Download failed")
                    self.stats['failed'] += 1
                    continue
                
                # Upload to S3
                self.logger.info(f"  Uploading to S3: {s3_key}")
                if self.s3_storage.upload_file(temp_path, s3_key):
                    self.logger.info("  Upload successful")
                    self.stats['successful'] += 1
                else:
                    self.logger.error("  Upload failed")
                    self.stats['failed'] += 1
                
            finally:
                # Clean up temporary file
                if temp_path.exists():
                    temp_path.unlink()
            
            # Add delay between downloads
            if i < len(reports):
                time.sleep(delay)
    
    def print_summary(self) -> None:
        """Print summary of download operations"""
        self.logger.info("\n=== Download Summary ===")
        self.logger.info(f"Total reports: {self.stats['total']}")
        self.logger.info(f"Successfully downloaded: {self.stats['successful']}")
        self.logger.info(f"Failed downloads: {self.stats['failed']}")
        self.logger.info(f"Skipped (already exist): {self.stats['skipped']}")

def main():
    """Main function to run the downloader"""
    # Configuration
    CSV_PATH = 'cost_reports.csv'
    S3_BUCKET = 'downloaded-data'
    DOWNLOAD_DELAY = 1.0  # seconds between downloads
    
    # Create downloader instance
    downloader = CostReportDownloader(CSV_PATH, S3_BUCKET)
    
    # Load reports
    reports = downloader.load_reports()
    if not reports:
        return
    
    # Download reports
    downloader.download_reports(reports, delay=DOWNLOAD_DELAY)
    
    # Print summary
    downloader.print_summary()

if __name__ == "__main__":
    main() 