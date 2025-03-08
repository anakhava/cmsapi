from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import json
import time

def scrape_cms_data_links():
    """
    Scraper for CMS data search page using Selenium to handle JavaScript
    """
    print("=== Starting CMS Dataset Scraper ===")
    options = Options()
    options.add_argument("--headless")  # Run in headless mode (no browser UI)
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    print("Starting browser...")
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        print("Browser started successfully")
    except Exception as e:
        print(f"Failed to start browser: {e}")
        return []
    
    try:
        print("Navigating to CMS data search page...")
        driver.get("https://data.cms.gov/search")
        
        # Wait for the page to load - adjust timeout as needed
        print("Waiting for page to load...")
        time.sleep(5)  # Simple wait for page to load
        
        # Save the page source for debugging
        with open("cms_selenium_page.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        print("Page source saved to cms_selenium_page.html")
        
        # Try to find elements with the target class
        print("Looking for dataset links...")
        try:
            # Wait for elements to be present (up to 10 seconds)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a.DatasetResult__title-container__title-link.btn.btn-link"))
            )
        except Exception as e:
            print(f"Wait timed out: {e}")
            # Try alternative selectors
            print("Trying alternative selectors...")
        
        # Get all links
        links = []
        elements = driver.find_elements(By.CSS_SELECTOR, "a.DatasetResult__title-container__title-link.btn.btn-link")
        print(f"Found {len(elements)} elements with target class")
        
        if len(elements) == 0:
            print("No elements found with target class. Trying to find dataset links with alternative methods...")
            print("All anchor elements on the page:")
            all_anchors = driver.find_elements(By.TAG_NAME, "a")
            print(f"Total anchors: {len(all_anchors)}")
            
            # Print first 5 anchors for debugging
            for i, a in enumerate(all_anchors[:5]):
                print(f"{i+1}. Class: {a.get_attribute('class')}, Href: {a.get_attribute('href')}, Text: {a.text[:50]}")
        
        # Process the elements found
        for element in elements:
            href = element.get_attribute("href")
            title = element.text.strip()
            
            if href:
                links.append({
                    "title": title,
                    "url": href
                })
        
        print(f"Processed {len(links)} valid links")
        
        # Output the results
        if len(links) == 0:
            print("No links found with the target class.")
        else:
            print("Dataset Links:")
            for index, link in enumerate(links, 1):
                print(f"{index}. {link['title']}")
                print(f"   URL: {link['url']}")
        
        return links
        
    except Exception as e:
        print(f"Error during scraping: {type(e).__name__}: {e}")
        return []
    
    finally:
        print("Closing browser...")
        driver.quit()

if __name__ == "__main__":
    links = scrape_cms_data_links()
    
    if links:
        with open("cms-dataset-links.json", "w", encoding="utf-8") as f:
            json.dump(links, f, indent=2)
        print(f"Results saved to cms-dataset-links.json")
    else:
        print("No links were scraped, so no file was created.")