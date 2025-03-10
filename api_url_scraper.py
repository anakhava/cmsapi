"""
Scrapes CMS dataset pages to extract API URLs and dataset metadata.
"""
from dataclasses import dataclass
import re
import sys
import traceback
from typing import Optional, List
import time
import csv

# Add Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

# Enable unbuffered output
sys.stdout = open(sys.stdout.fileno(), mode='w', buffering=1)

@dataclass
class DatasetInfo:
    """Stores information about a CMS dataset"""
    title: str
    description: str
    api_url: Optional[str]
    uuid: Optional[str]
    dataset_url: str

def get_dataset_urls(offset: int = 0) -> List[str]:
    """
    Gets dataset URLs from the CMS search results page using Selenium
    
    Args:
        offset: Offset for search results (increments by 10)
        
    Returns:
        List of dataset URLs
    """
    print(f"\n=== Processing Search Results (offset {offset}) ===")
    driver = None
    
    try:
        # Set up Selenium
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        
        print("Starting browser...")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        # Navigate to search page with offset
        search_url = f"https://data.cms.gov/search?offset={offset}"
        print(f"Navigating to: {search_url}")
        driver.get(search_url)
        
        # Wait for page to load
        print("Waiting for page to load...")
        time.sleep(5)
        print(f"Current page title: {driver.title}")
        
        # Find dataset links
        print("Looking for dataset links...")
        dataset_urls = []
        
        # Try to find elements with the target class
        elements = driver.find_elements(By.CSS_SELECTOR, "a.DatasetResult__title-container__title-link")
        print(f"Found {len(elements)} dataset links")
        
        if len(elements) == 0:
            print("No elements found with primary selector. Trying alternative selectors...")
            # Try alternative selectors
            elements = driver.find_elements(By.XPATH, "//div[contains(@class, 'DatasetResult__title-container')]//a")
            print(f"Found {len(elements)} dataset links with alternative selector")
        
        for element in elements:
            href = element.get_attribute("href")
            title = element.text.strip()
            
            if href:
                dataset_urls.append(href)
                print(f"Found dataset: {title}")
        
        print(f"Found {len(dataset_urls)} datasets on page {offset}")
        return dataset_urls
        
    except Exception as e:
        print(f"Error during scraping: {str(e)}")
        print(traceback.format_exc())
        return []
    
    finally:
        if driver:
            print("Closing browser...")
            driver.quit()

def extract_api_url_and_uuid(driver) -> tuple:
    """
    Extracts API URL and UUID from a dataset page
    
    Args:
        driver: Selenium WebDriver instance
        
    Returns:
        Tuple of (api_url, uuid)
    """
    api_url = None
    uuid = None
    
    try:
        # Try to find any button containing "API" text
        print("Looking for 'Access API' button...")
        buttons = driver.find_elements(By.XPATH, "//button")
        print(f"Found {len(buttons)} buttons on the page")
        
        api_button = None
        for btn in buttons:
            try:
                text = btn.text.lower()
                if "api" in text:
                    api_button = btn
                    print(f"Found API button with text: '{text}'")
                    break
            except:
                continue
        
        if api_button:
            # Scroll the button into view
            print("Scrolling to API button...")
            driver.execute_script("arguments[0].scrollIntoView(true);", api_button)
            time.sleep(1)  # Wait for scroll to complete
            
            # Try to ensure the button is clickable
            print("Clicking API button...")
            try:
                # Try direct click first
                api_button.click()
            except Exception as e:
                print(f"Direct click failed: {e}")
                # Try JavaScript click as fallback
                driver.execute_script("arguments[0].click();", api_button)
                
            print("Waiting for modal to appear...")
            time.sleep(3)
            
            # Look for the input field
            input_fields = driver.find_elements(By.TAG_NAME, "input")
            print(f"Found {len(input_fields)} input fields")
            
            for inp in input_fields:
                try:
                    class_name = inp.get_attribute("class")
                    value = inp.get_attribute("value")
                    
                    if class_name and "AccessApiModal__urlInput" in class_name:
                        api_url = value
                        print(f"Found API URL in modal: {api_url}")
                        break
                        
                    # Fallback: look for any input with a data-api URL
                    if value and "data-api" in value:
                        api_url = value
                        print(f"Found API URL in input value: {api_url}")
                except:
                    continue
            
            # Extract UUID from API URL
            if api_url:
                uuid_match = re.search(r'dataset/([a-f0-9-]+)/data', api_url)
                if uuid_match:
                    uuid = uuid_match.group(1)
                    print(f"Extracted UUID from API URL: {uuid}")
        else:
            print("No API button found")
    
    except Exception as e:
        print(f"Error extracting API URL: {str(e)}")
    
    return api_url, uuid

def extract_dataset_info(dataset_url: str) -> Optional[DatasetInfo]:
    """
    Extracts dataset information from a CMS dataset page.
    
    Args:
        dataset_url: URL of the CMS dataset page
        
    Returns:
        DatasetInfo object containing extracted information or None if error
    """
    driver = None
    try:
        print(f"\nNavigating to dataset: {dataset_url}")
        
        # Use Selenium to load the dataset page
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.get(dataset_url)
        
        # Wait for page to load
        print("Waiting for page to load...")
        time.sleep(5)
        print(f"Current page title: {driver.title}")
        
        # Extract title
        try:
            title_element = driver.find_element(By.TAG_NAME, "h1")
            title = title_element.text.strip()
            print(f"Dataset Title: {title}")
        except Exception as e:
            print(f"Error extracting title: {e}")
            title = "Unknown Title"
        
        # Extract description
        try:
            desc_div = driver.find_element(By.CSS_SELECTOR, "div.DatasetPage__summary-field-summary-container")
            description = desc_div.text.strip()
        except Exception as e:
            print(f"Error extracting description: {e}")
            description = ""
        
        # Extract API URL and UUID
        api_url, uuid = extract_api_url_and_uuid(driver)
        
        # If UUID wasn't found in API URL, try to extract from dataset URL
        if not uuid:
            uuid_match = re.search(r'dataset/([a-f0-9-]+)', dataset_url)
            if uuid_match:
                uuid = uuid_match.group(1)
                print(f"UUID found in dataset URL: {uuid}")
            else:
                print("No UUID found in URL")
        
        return DatasetInfo(
            title=title,
            description=description,
            api_url=api_url,
            uuid=uuid,
            dataset_url=dataset_url
        )
    except Exception as e:
        print(f"Error processing {dataset_url}: {str(e)}")
        print(traceback.format_exc())
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

def save_to_csv(datasets: List[DatasetInfo]) -> None:
    """
    Saves dataset information to a CSV file
    
    Args:
        datasets: List of DatasetInfo objects
    """
    with open('cms_datasets.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['title', 'description', 'api_url', 'uuid', 'dataset_url']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for dataset in datasets:
            writer.writerow({
                'title': dataset.title,
                'description': dataset.description,
                'api_url': dataset.api_url or '',
                'uuid': dataset.uuid or '',
                'dataset_url': dataset.dataset_url
            })
    
    print(f"Results saved to cms_datasets.csv")

def main():
    """Main function to run the scraper"""
    print("Starting CMS API URL scraper...")
    
    try:
        all_datasets = []
        offset = 0
        max_pages = 5  # Limit to 5 pages for testing
        
        for page in range(max_pages):
            print(f"\n=== Processing page {page+1} ===")
            dataset_urls = get_dataset_urls(offset)
            
            if not dataset_urls:
                print(f"No more datasets found. Stopping at page {page+1}.")
                break
            
            for url in dataset_urls:
                dataset_info = extract_dataset_info(url)
                if dataset_info:
                    all_datasets.append(dataset_info)
                    if dataset_info.api_url:
                        print(f"✓ API URL found for {dataset_info.title}")
                    else:
                        print(f"✗ No API URL for {dataset_info.title}")
            
            offset += 10
            
        print(f"\n=== Summary ===")
        print(f"Found {len(all_datasets)} datasets")
        api_count = sum(1 for d in all_datasets if d.api_url)
        print(f"Found {api_count} datasets with API URLs")
        
        if all_datasets:
            save_to_csv(all_datasets)
        else:
            print("No datasets found to save.")
    
    except Exception as e:
        print(f"Error in main function: {e}")
        print(traceback.format_exc())

if __name__ == "__main__":
    main() 