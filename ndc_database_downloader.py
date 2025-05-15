import os
import csv
import logging
from pathlib import Path
from utils import setup_logging, download_file

class NDCDatabaseDownloader:
    def __init__(self, csv_path="ndc_database_urls.csv", output_dir="/Users/arianakhavan/Documents/reference_data"):
        self.csv_path = csv_path
        self.output_dir = Path(output_dir)
        self.logger = logging.getLogger(__name__)
        
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

    def download_files(self):
        """Download all files from the CSV."""
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
            
            # Check if file already exists
            if output_path.exists():
                self.logger.info(f"File already exists: {file_name}")
                skipped_downloads += 1
                continue
            
            try:
                self.logger.info(f"Downloading {file_name}...")
                if download_file(download_url, output_path, logger=self.logger):
                    successful_downloads += 1
                    self.logger.info(f"Successfully downloaded {file_name}")
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
    downloader = NDCDatabaseDownloader()
    downloader.download_files()

if __name__ == "__main__":
    main() 