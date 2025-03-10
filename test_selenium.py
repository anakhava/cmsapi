from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time

print("Starting Selenium test...")

try:
    print("Initializing Chrome WebDriver...")
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    print("WebDriver initialized successfully")
    print("Navigating to Google...")
    driver.get("https://www.google.com")
    
    print(f"Page title: {driver.title}")
    print("Test successful!")
    
    driver.quit()
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    print(traceback.format_exc()) 