import os
import logging
import requests
import csv
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from utils import setup_logging, get_page_content

class NDCDatabaseScraper:
    def __init__(self):
        self.base_url = "https://www.fda.gov/drugs/drug-approvals-and-databases/national-drug-code-directory"
        self.logger = logging.getLogger(__name__)
        
        # Files to scrape with their expected names
        self.files_to_scrape = [
            {
                "name": "ndctext.zip",
                "description": "NDC Database File (text version)"
            },
            {
                "name": "ndcunfinished.zip",
                "description": "NDC Unfinished Drugs Database"
            },
            {
                "name": "ndccompounded.zip",
                "description": "NDC Compounded Drugs Database"
            },
            {
                "name": "ndcexcluded.zip",
                "description": "NDC Excluded Drugs Database"
            }
        ]

    def get_download_links(self):
        """Get download links for the specified NDC database files only."""
        # Filenames to capture
        target_filenames = {
            'ndctext.zip',
            'ndc_unfinished.zip',
            'compounders_ndc_directory.zip',
            'ndc_excluded.zip',
        }
        try:
            html = get_page_content(self.base_url, logger=self.logger)
            if not html:
                self.logger.error("Failed to fetch page content.")
                return {}
            soup = BeautifulSoup(html, 'html.parser')
            links = soup.find_all('a')
            download_links = {}
            for link in links:
                href = link.get('href', '')
                if href and any(name in href for name in target_filenames):
                    # Use the filename as the key
                    for name in target_filenames:
                        if name in href:
                            download_links[name] = {
                                'url': href if href.startswith('http') else urljoin(self.base_url, href),
                                'description': link.get_text(strip=True)
                            }
            return download_links
        except Exception as e:
            self.logger.error(f"Error fetching download links: {str(e)}")
            return {}

    def save_to_csv(self, download_links):
        """Save download links to a CSV file."""
        csv_file = "ndc_database_urls.csv"
        try:
            with open(csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['File Name', 'Description', 'Download URL'])
                for name, info in download_links.items():
                    writer.writerow([name, info['description'], info['url']])
            self.logger.info(f"Successfully saved URLs to {csv_file}")
            return True
        except Exception as e:
            self.logger.error(f"Error saving to CSV: {str(e)}")
            return False

    def scrape(self):
        """Scrape download links and save to CSV."""
        download_links = self.get_download_links()
        
        if not download_links:
            self.logger.error("No download links found")
            return False
        
        return self.save_to_csv(download_links)

def main():
    logger = setup_logging('ndc_scraper')
    scraper = NDCDatabaseScraper()
    scraper.scrape()

if __name__ == "__main__":
    main() 