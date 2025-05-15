"""
Download cost report files from URLs listed in cost_reports.csv.
"""
import csv
import time
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
from utils import setup_logging, download_file, clean_url, get_file_extension

class CostReportDownloader:
    def __init__(self, csv_path: str, output_dir: str):
        """
        Initialize the downloader
        
        Args:
            csv_path: Path to the cost_reports.csv file
            output_dir: Base directory for downloaded files
        """
        self.logger = setup_logging('cost_report_downloader')
        self.csv_path = Path(csv_path)
        self.output_dir = Path(output_dir)
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
    
    def get_output_path(self, report: Dict) -> Path:
        """
        Generate output path for a report
        
        Args:
            report: Dictionary containing report information
            
        Returns:
            Path object for the output file
        """
        # Create directory structure: output_dir/facility_type/year/
        facility_dir = self.output_dir / report['facility_type']
        year_dir = facility_dir / str(report['year'])
        
        # Get file extension from URL or default to .zip
        ext = get_file_extension(report['download_url'])
        
        # Use file_name from CSV if available, otherwise generate one
        if report['file_name']:
            filename = f"{report['file_name']}{ext}"
        else:
            filename = f"{report['facility_type']}_{report['year']}{ext}"
        
        return year_dir / filename
    
    def download_reports(self, reports: List[Dict], delay: float = 1.0) -> None:
        """
        Download cost report files
        
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
            
            # Get output path
            output_path = self.get_output_path(report)
            
            # Skip if file already exists
            if output_path.exists():
                self.logger.info(f"  File already exists: {output_path}")
                self.stats['skipped'] += 1
                continue
            
            # Download file
            self.logger.info(f"  Downloading to: {output_path}")
            if download_file(url, output_path, logger=self.logger):
                self.logger.info("  Download successful")
                self.stats['successful'] += 1
            else:
                self.logger.error("  Download failed")
                self.stats['failed'] += 1
            
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
    OUTPUT_DIR = '/Users/arianakhavan/Documents/reference_data'
    DOWNLOAD_DELAY = 1.0  # seconds between downloads
    
    # Create downloader instance
    downloader = CostReportDownloader(CSV_PATH, OUTPUT_DIR)
    
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