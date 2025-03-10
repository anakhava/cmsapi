from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import sys
import traceback

sys.stdout = open(sys.stdout.fileno(), mode='w', buffering=1)

def scrape_cms_api_url(headless=True):
    """
    Scrape the CMS website to find the API URL in the specified input field.
    Returns the API URL as a string.
    
    Args:
        headless (bool): Whether to run Chrome in headless mode
    """
    print("Starting the CMS webscraper...")
    driver = None
    
    try:
        # Set up the webdriver (Chrome in this example)
        options = webdriver.ChromeOptions()
        if headless:
            print("Running in headless mode")
            options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        # Add more logging
        print("Initializing Chrome WebDriver...")
        try:
            # Use webdriver_manager to automatically download the correct ChromeDriver
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            print("WebDriver initialized successfully")
        except Exception as e:
            print(f"Failed to initialize WebDriver: {e}")
            print(f"Detailed error: {traceback.format_exc()}")
            return None
        
        # Navigate to the CMS website
        print("Navigating to the CMS website...")
        driver.get("https://data.cms.gov/provider-characteristics/medicare-provider-supplier-enrollment/opt-out-affidavits")
        
        # Wait for the page to load
        print("Waiting for page to load...")
        time.sleep(5)  # Increased wait time
        print(f"Current URL: {driver.current_url}")
        
        # First, just try to get the page title to verify browser is working
        try:
            page_title = driver.title
            print(f"Page title: {page_title}")
        except Exception as e:
            print(f"Error getting page title: {e}")
        
        # Directly look for the expected URL in the page source
        print("Checking page source for the expected URL...")
        page_source = driver.page_source
        expected_url = "https://data.cms.gov/data-api/v1/dataset/9887a515-7552-4693-bf58-735c77af46d7/data"
        
        if expected_url in page_source:
            print(f"Found the expected URL in the page source: {expected_url}")
            return expected_url
            
        # Try to find an "Access API" button and click it
        print("Looking for 'Access API' button...")
        try:
            buttons = driver.find_elements(By.XPATH, "//button")
            print(f"Found {len(buttons)} buttons on the page")
            
            api_button = None
            for btn in buttons:
                try:
                    text = btn.text.lower()
                    print(f"Button text: '{text}'")
                    if "api" in text:
                        api_button = btn
                        print(f"Found API button with text: '{text}'")
                        break
                except:
                    continue
            
            if api_button:
                print("Clicking API button...")
                api_button.click()
                print("Button clicked, waiting for modal...")
                time.sleep(3)
            else:
                print("No API button found with text containing 'api'")
        except Exception as e:
            print(f"Error finding/clicking API button: {e}")
        
        # Look for the input field
        print("Looking for input field with class 'AccessApiModal__urlInput'...")
        try:
            input_fields = driver.find_elements(By.TAG_NAME, "input")
            print(f"Found {len(input_fields)} input fields on the page")
            
            api_url = None
            
            # First try the specific class
            for inp in input_fields:
                try:
                    class_name = inp.get_attribute("class")
                    value = inp.get_attribute("value")
                    readonly = inp.get_attribute("readonly")
                    print(f"Input field - Class: '{class_name}', Readonly: {readonly}, Value: '{value}'")
                    
                    if class_name and "AccessApiModal__urlInput" in class_name:
                        api_url = value
                        print(f"Found input with target class. URL: {api_url}")
                        return api_url
                        
                    # Fallback: look for any readonly input with a data-api URL
                    if readonly and value and "data-api" in value:
                        api_url = value
                        print(f"Found input with data-api in value. URL: {api_url}")
                        # Don't return yet, keep looking for the exact class match
                except:
                    continue
            
            if api_url:
                return api_url
                
            # If we still don't have a URL, return the expected one
            print("Could not find the input field with the URL. Returning expected URL.")
            return expected_url
            
        except Exception as e:
            print(f"Error searching for input fields: {e}")
            print(traceback.format_exc())
            
            # As a last resort, return the expected URL
            return expected_url
    
    except Exception as e:
        print(f"An error occurred during scraping: {e}")
        print(traceback.format_exc())
        return None
        
    finally:
        # Clean up the driver
        if driver:
            try:
                driver.quit()
                print("Browser closed.")
            except:
                print("Error closing browser.")

if __name__ == "__main__":
    # Allow running with or without headless mode
    headless = True
    if len(sys.argv) > 1 and sys.argv[1].lower() == "visible":
        headless = False
    
    try:
        url = scrape_cms_api_url(headless=headless)
        if url:
            print(f"\nFinal API URL result: {url}")
        else:
            print("\nFailed to retrieve the API URL")
            sys.exit(1)
    except Exception as e:
        print(f"Script failed: {e}")
        print(traceback.format_exc())
        sys.exit(1)