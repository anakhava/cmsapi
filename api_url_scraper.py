"""
Scrapes CMS dataset pages to extract API URLs and dataset metadata.
"""
from dataclasses import dataclass
import re
from typing import Optional, List
from bs4 import BeautifulSoup
import requests
import time
import csv

@dataclass
class DatasetInfo:
    """Holds information about a CMS dataset"""
    title: str
    description: str
    api_url: Optional[str]
    uuid: Optional[str]
    dataset_url: str

def get_dataset_urls(offset: int = 0) -> List[str]:
    """
    Gets dataset URLs from the CMS search results page
    
    Args:
        offset: Offset for search results (increments by 10)
        
    Returns:
        List of dataset URLs
    """
    print(f"\n=== Processing Search Results (offset {offset}) ===")
    search_url = f"https://data.cms.gov/search?offset={offset}"
    response = requests.get(search_url)
    
    # Debug: Print response status and headers
    print(f"\nResponse Status Code: {response.status_code}")
    print("\nResponse Headers:")
    for key, value in response.headers.items():
        print(f"{key}: {value}")
    
    # Debug: Print raw HTML
    print("\nRaw HTML received:")
    print(response.text[:1000])  # First 1000 chars
    print("...[truncated]...")
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Debug: Print all link elements found
    print("\nAll <a> tags found:")
    all_links = soup.find_all('a')
    for link in all_links:
        print(f"\nLink: {link}")
        print(f"Classes: {link.get('class', [])}")
        print(f"Href: {link.get('href')}")
        print(f"Text: {link.text.strip()}")
    
    # Find all dataset links in search results
    dataset_links = soup.find_all('a', class_='DatasetResult__title-container__title-link')
    print(f"\nDataset links found with class 'DatasetResult__title-container__title-link': {len(dataset_links)}")
    
    dataset_urls = []
    for link in dataset_links:
        if link.get('href'):
            full_url = f"https://data.cms.gov{link['href']}"
            dataset_urls.append(full_url)
            print(f"Found dataset: {link.text.strip()}")
    
    print(f"Found {len(dataset_urls)} datasets on page {offset}")
    return dataset_urls

def extract_dataset_info(dataset_url: str) -> Optional[DatasetInfo]:
    """
    Extracts dataset information from a CMS dataset page.
    
    Args:
        dataset_url: URL of the CMS dataset page
        
    Returns:
        DatasetInfo object containing extracted information or None if error
    """
    try:
        print(f"\nNavigating to dataset: {dataset_url}")
        response = requests.get(dataset_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract title
        title = soup.find('h1').text.strip()
        print(f"Dataset Title: {title}")
        
        # Extract description 
        desc_div = soup.find('div', class_='DatasetPage__summary-field-summary-container')
        description = desc_div.text.strip() if desc_div else ''
        
        # Extract UUID from meta tags
        uuid = None
        og_url = soup.find('meta', property='og:url')
        if og_url:
            uuid_match = re.search(r'dataset/([^/]+)', og_url['content'])
            if uuid_match:
                uuid = uuid_match.group(1)
                print(f"UUID found: {uuid}")
            else:
                print("No UUID found in meta tags")
                
        # Extract API URL from modal
        api_url = None
        url_input = soup.find('input', class_='AccessApiModal__urlInput')
        if url_input:
            api_url = url_input.get('value')
            print("✓ Access API button found")
        else:
            print("✗ No Access API button found")
                
        return DatasetInfo(
            title=title,
            description=description,
            api_url=api_url,
            uuid=uuid,
            dataset_url=dataset_url
        )
    except Exception as e:
        print(f"Error processing {dataset_url}: {str(e)}")
        return None

def save_to_csv(datasets: List[DatasetInfo], filename: str = 'cms_datasets.csv'):
    """Saves dataset information to CSV file"""
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Title', 'Description', 'API URL', 'UUID', 'Dataset URL'])
        for dataset in datasets:
            writer.writerow([
                dataset.title,
                dataset.description,
                dataset.api_url,
                dataset.uuid,
                dataset.dataset_url
            ])

def main():
    """Main entry point"""
    all_datasets = []
    offset = 0
    
    print("Starting CMS Dataset API URL Scraper...")
    
    while True:
        dataset_urls = get_dataset_urls(offset)
        
        if not dataset_urls:
            print("\nNo more datasets found. Finished processing all pages.")
            break
            
        for url in dataset_urls:
            info = extract_dataset_info(url)
            if info and info.api_url:  # Only keep datasets with API URLs
                all_datasets.append(info)
                print(f"Added to results - has API access")
            elif info:
                print(f"Skipped - no API access")
            time.sleep(1)  # Be nice to the server
            
        offset += 10  # Increment by 10 instead of page number
        
    print(f"\n=== Summary ===")
    print(f"Found {len(all_datasets)} datasets with API URLs")
    save_to_csv(all_datasets)
    print(f"Results saved to cms_datasets.csv")

if __name__ == "__main__":
    main() 