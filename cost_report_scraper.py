"""
Scrapes CMS cost report pages to extract download links for cost report data.
Uses direct HTML parsing for more reliable extraction.
"""
import re
import sys
import traceback
import time
import csv
import requests
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Enable unbuffered output
sys.stdout = open(sys.stdout.fileno(), mode='w', buffering=1)

def get_page_content(url, max_retries=3):
    """
    Get HTML content from a URL with retry logic
    
    Args:
        url: URL to retrieve
        max_retries: Maximum number of retry attempts
        
    Returns:
        HTML content as string or None if failed
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    for attempt in range(max_retries):
        try:
            print(f"Fetching URL: {url} (Attempt {attempt+1}/{max_retries})")
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"Error fetching URL (Attempt {attempt+1}): {e}")
            if attempt < max_retries - 1:
                wait_time = 2 * (attempt + 1)  # Exponential backoff
                print(f"Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            else:
                print(f"Failed to fetch URL after {max_retries} attempts")
                return None

def find_cost_reports_in_main_page(html_content, base_url):
    """
    Parse the main cost reports page and extract links to report pages
    
    Args:
        html_content: HTML content of the main page
        base_url: Base URL for resolving relative links
        
    Returns:
        List of dictionaries with year, facility_type, and report_url
    """
    reports = []
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find the main table containing the cost reports
    table = soup.find('table')
    if not table:
        print("No main table found, trying alternative selectors...")
        return find_reports_alternative_method(soup, base_url)
    
    # Process each row in the table
    rows = table.find_all('tr')
    print(f"Found {len(rows)} rows in the main table")
    
    for row in rows:
        try:
            cells = row.find_all('td')
            if len(cells) >= 2:
                # Get the year and link from the first cell
                year_cell = cells[0]
                link = year_cell.find('a')
                if not link:
                    continue
                    
                year_text = link.get_text().strip()
                link_url = link.get('href')
                
                # Extract year
                year_match = re.search(r'(\d{4})', year_text)
                if not year_match:
                    continue
                year = year_match.group(1)
                
                # Extract facility type from the second cell
                facility_cell = cells[1]
                facility_text = facility_cell.get_text().strip()
                
                # Clean up facility type text
                facility_type = clean_facility_type(facility_text)
                
                # If we couldn't determine facility type from text, try URL
                if facility_type == "UNKNOWN":
                    facility_type = extract_facility_type_from_url(link_url)
                
                # Construct full URL
                report_url = urljoin(base_url, link_url)
                
                report_info = {
                    'year': year,
                    'facility_type': facility_type,
                    'report_url': report_url
                }
                
                reports.append(report_info)
                print(f"Found report: Year {year}, Type {facility_type}, URL: {report_url}")
                
        except Exception as e:
            print(f"Error processing row: {e}")
            continue
    
    return reports

def clean_facility_type(text):
    """
    Clean up facility type text by removing unwanted parts and standardizing format
    """
    # Remove "Facility Type" and any extra whitespace
    text = re.sub(r'Facility Type\s*', '', text, flags=re.IGNORECASE)
    text = text.strip()
    
    # Standardize common variations
    text = text.upper()
    text = re.sub(r'HOSP(ITAL)?-?\d*', 'HOSPITAL', text)
    text = re.sub(r'SNF-?\d*', 'SNF', text)
    text = re.sub(r'HHA-?\d*', 'HHA', text)
    
    # If text is empty or just contains numbers, return UNKNOWN
    if not text or text.replace('-', '').isdigit():
        return "UNKNOWN"
        
    return text

def extract_facility_type_from_url(url):
    """
    Extract facility type from URL using improved pattern matching
    """
    patterns = [
        (r'/(snf|hha|hosp|hospital)(?:-?\d*)?/', 'HOSPITAL', 'HOSP'),
        (r'/(snf|hha|hosp|hospital)(?:-?\d*)?$', 'HOSPITAL', 'HOSP'),
        (r'/(snf|hha|hosp|hospital)(?:-?\d*)?\.', 'HOSPITAL', 'HOSP')
    ]
    
    for pattern, replacement, match in patterns:
        facility_match = re.search(pattern, url, re.IGNORECASE)
        if facility_match:
            facility_type = facility_match.group(1).upper()
            if facility_type == match:
                return replacement
            return facility_type
            
    return "UNKNOWN"

def find_reports_alternative_method(soup, base_url):
    """
    Alternative method to find reports when main table is not found
    """
    reports = []
    
    # Look for any links that might contain cost report information
    links = soup.find_all('a', href=True)
    for link in links:
        try:
            href = link['href']
            text = link.get_text().strip()
            
            # Look for year in the link text
            year_match = re.search(r'(\d{4})', text)
            if not year_match:
                continue
            year = year_match.group(1)
            
            facility_type = extract_facility_type_from_url(href)
            
            if facility_type != "UNKNOWN":
                report_url = urljoin(base_url, href)
                report_info = {
                    'year': year,
                    'facility_type': facility_type,
                    'report_url': report_url
                }
                reports.append(report_info)
                print(f"Found report (alt method): Year {year}, Type {facility_type}")
                
        except Exception as e:
            print(f"Error processing link with alt method: {e}")
            continue
            
    return reports

def handle_pagination(base_url, max_pages=20):
    """
    Handle pagination on the main page
    
    Args:
        base_url: Base URL for the main page
        max_pages: Maximum number of pages to process
        
    Returns:
        List of dictionaries with year, facility_type, and report_url
    """
    all_reports = []
    processed_urls = set()
    
    # Start with the base URL
    current_url = base_url
    page = 1
    
    while page <= max_pages:
        print(f"\n=== Processing Page {page} ===")
        
        # Get the current page content
        html_content = get_page_content(current_url)
        if not html_content:
            print(f"Failed to get content for page {page}, stopping pagination.")
            break
            
        # Find report links on this page
        reports = find_cost_reports_in_main_page(html_content, base_url)
        print(f"Found {len(reports)} reports on page {page}")
        
        # Add new reports to the list (avoid duplicates)
        for report in reports:
            if report['report_url'] not in processed_urls:
                all_reports.append(report)
                processed_urls.add(report['report_url'])
        
        # Look for the "next page" link
        soup = BeautifulSoup(html_content, 'html.parser')
        next_link = soup.find('a', class_='next')
        
        if next_link and next_link.get('href'):
            # Update the URL for the next page
            current_url = urljoin(base_url, next_link.get('href'))
            print(f"Moving to next page: {current_url}")
            page += 1
            
            # Pause briefly between page requests
            time.sleep(2)
        else:
            print("No next page found, ending pagination")
            break
    
    return all_reports

def find_download_links(html_content, base_url, year, facility_type):
    """
    Parse a report page and extract download links
    
    Args:
        html_content: HTML content of the report page
        base_url: Base URL for resolving relative links
        year: Year of the cost report
        facility_type: Type of facility
        
    Returns:
        List of dictionaries with download information
    """
    downloads = []
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Try multiple strategies to find download links
    download_links = []
    
    # Strategy 1: Look in the main content area
    main_content = soup.find('div', class_='field--name-body')
    if main_content:
        download_links.extend(main_content.find_all('a', href=True))
    
    # Strategy 2: Look for list items with field__item class (common in newer pages)
    field_items = soup.find_all('li', class_='field__item')
    for item in field_items:
        link = item.find('a', href=True)
        if link:
            download_links.append(link)
    
    # Strategy 3: Look for links in any div with field__item class
    field_items_div = soup.find_all('div', class_='field__item')
    for item in field_items_div:
        link = item.find('a', href=True)
        if link:
            download_links.append(link)
    
    # Strategy 4: Fallback to searching the entire page
    if not download_links:
        download_links = soup.find_all('a', href=True)
    
    print(f"Found {len(download_links)} potential download links on the page")
    
    # Process each link
    for link in download_links:
        try:
            # Get the href, handling both single and double quotes
            href = link.get('href', '').strip()
            if not href:
                continue
                
            # Skip if not a download link
            if not is_download_link(href, year, facility_type):
                continue
                
            # Get file name from link text or href
            file_name = link.get_text().strip()
            if not file_name or file_name.startswith('http'):
                file_name = href.split('/')[-1]
                
            # Clean up file name and URL
            file_name = clean_file_name(file_name)
            download_url = clean_url(href, base_url)
            
            # Skip if URL cleaning failed
            if not download_url:
                continue
            
            # Generate a better file name if needed
            if not file_name or file_name.startswith('http'):
                file_name = generate_file_name(facility_type, year, download_url)
            
            download_info = {
                'year': year,
                'facility_type': facility_type,
                'file_name': file_name,
                'download_url': download_url
            }
            
            print(f"Found download link: {file_name} -> {download_url}")
            downloads.append(download_info)
            
        except Exception as e:
            print(f"Error processing download link: {e}")
            continue
            
    return downloads

def clean_url(url, base_url):
    """
    Clean and validate a URL
    """
    try:
        # Remove leading/trailing whitespace
        url = url.strip()
        
        # Handle single-quoted URLs that might have spaces
        url = url.strip("'").strip()
        
        # Ensure proper URL format
        if not url.startswith(('http://', 'https://')):
            url = urljoin(base_url, url)
        
        # Force HTTPS
        url = url.replace('http://', 'https://')
        
        # Normalize case for consistency
        url = url.replace('/FILES/', '/files/').replace('/HCRIS/', '/hcris/')
        
        # Validate URL format
        if not is_valid_download_url(url):
            return None
            
        return url
    except Exception as e:
        print(f"Error cleaning URL {url}: {e}")
        return None

def is_download_link(href, year, facility_type):
    """
    Check if a link is likely to be a download link for the specific year and facility type
    """
    href = href.lower()
    year_str = str(year)
    facility_patterns = {
        'HOSPITAL': ['hosp', 'hospital'],
        'SNF': ['snf'],
        'HHA': ['hha']
    }
    
    # Check for common download file extensions
    if not any(href.endswith(ext) for ext in ['.zip', '.csv', '.xlsx', '.xls']):
        return False
    
    # Check for year in URL
    if year_str not in href and f"fy{year_str[-2:]}" not in href:
        return False
    
    # Check for facility type in URL
    facility_matches = facility_patterns.get(facility_type, [])
    if not any(pattern in href for pattern in facility_matches):
        return False
    
    # Check for common download patterns in the URL
    download_patterns = [
        r'/downloads/',
        r'/files/',
        r'/data/',
        r'/hcris/',
        r'cost-report',
        r'costreport'
    ]
    
    return any(re.search(pattern, href, re.IGNORECASE) for pattern in download_patterns)

def is_valid_download_url(url):
    """
    Verify that a download URL is valid
    """
    url = url.lower()
    
    # Must be HTTPS
    if not url.startswith('https://'):
        return False
    
    # Must be from CMS domain
    if 'cms.gov' not in url:
        return False
    
    # Must contain at least one of these patterns
    required_patterns = [
        ('downloads', 'files'),  # Either downloads or files
        'hcris'                 # Must have hcris
    ]
    
    # Check first group (downloads or files)
    has_download_or_files = any(pattern in url for pattern in required_patterns[0])
    
    # Check second requirement (hcris)
    has_hcris = required_patterns[1] in url
    
    return has_download_or_files and has_hcris

def generate_file_name(facility_type, year, url):
    """
    Generate a standardized file name based on facility type, year, and URL
    """
    # Extract base name from URL
    base_name = url.split('/')[-1]
    
    # Remove file extension
    base_name = re.sub(r'\.(zip|csv|xlsx|xls)$', '', base_name, flags=re.IGNORECASE)
    
    # If the base name is already good, use it
    if re.match(rf'^{facility_type}.*{year}', base_name, re.IGNORECASE):
        return base_name
    
    # Otherwise generate a new name
    return f"{facility_type}-{year} DATA FILES"

def clean_file_name(file_name):
    """
    Clean up file name by removing unwanted characters and standardizing format
    """
    # Remove any HTML tags
    file_name = re.sub(r'<[^>]+>', '', file_name)
    
    # Remove extra whitespace
    file_name = ' '.join(file_name.split())
    
    # Remove any non-printable characters
    file_name = ''.join(char for char in file_name if char.isprintable())
    
    # Remove any URLs
    file_name = re.sub(r'https?://[^\s]+', '', file_name)
    
    # Remove any file extensions
    file_name = re.sub(r'\.(zip|csv|xlsx|xls)$', '', file_name, flags=re.IGNORECASE)
    
    return file_name.strip()

def remove_duplicates_from_csv(csv_file):
    """
    Remove duplicate records from the CSV file while keeping the most complete record for each unique combination
    of year, facility type, and download URL.
    
    Args:
        csv_file: Path to the CSV file to clean
    """
    print("\nRemoving duplicate records from CSV...")
    
    # Read all records
    records = []
    with open(csv_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        records = list(reader)
    
    # Create a dictionary to store unique records
    unique_records = {}
    
    # Process each record
    for record in records:
        # Create a key based on year, facility type, and download URL
        key = (record['year'], record['facility_type'], record['download_url'])
        
        # If this is a new unique record or has a better file name, keep it
        if key not in unique_records or (
            record['file_name'] and 
            (not unique_records[key]['file_name'] or 
             len(record['file_name']) > len(unique_records[key]['file_name']))
        ):
            unique_records[key] = record
    
    # Write back unique records
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['year', 'facility_type', 'file_name', 'download_url'])
        writer.writeheader()
        writer.writerows(unique_records.values())
    
    removed_count = len(records) - len(unique_records)
    print(f"Removed {removed_count} duplicate records")
    print(f"Final record count: {len(unique_records)}")

def scrape_cost_reports(output_file='cost_reports.csv'):
    """
    Main function to scrape cost reports
    
    Args:
        output_file: Path to output CSV file
    """
    base_url = "https://www.cms.gov/data-research/statistics-trends-and-reports/cost-reports/cost-reports-fiscal-year"
    
    # Create and open output file
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['year', 'facility_type', 'file_name', 'download_url']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Handle pagination and get all report links
        cost_reports = handle_pagination(base_url)
        print(f"Found a total of {len(cost_reports)} unique cost report links across all pages")
        
        # Process each cost report page
        for i, report in enumerate(cost_reports, 1):
            print(f"\nProcessing report {i} of {len(cost_reports)}:")
            print(f"  Year: {report['year']}")
            print(f"  Type: {report['facility_type']}")
            print(f"  URL: {report['report_url']}")
            
            # Get the report page content
            report_html = get_page_content(report['report_url'])
            if not report_html:
                print(f"Failed to get content for {report['report_url']}, logging with empty download info.")
                # Log the report with empty download info
                writer.writerow({
                    'year': report['year'],
                    'facility_type': report['facility_type'],
                    'file_name': '',
                    'download_url': ''
                })
                print(f"✓ Saved to CSV with empty download info")
                continue
                
            # Find download links on the report page
            downloads = find_download_links(
                report_html, 
                base_url, 
                report['year'],
                report['facility_type']
            )
            
            # If no downloads found, still log the report with empty download info
            if not downloads:
                print(f"No download links found for {report['report_url']}, logging with empty download info.")
                writer.writerow({
                    'year': report['year'],
                    'facility_type': report['facility_type'],
                    'file_name': '',
                    'download_url': ''
                })
                print(f"✓ Saved to CSV with empty download info")
            else:
                # Write download links to CSV
                for download in downloads:
                    writer.writerow(download)
                    print(f"✓ Saved to CSV: {download['file_name']}")
                
            # Pause between requests to avoid overloading the server
            if i < len(cost_reports):
                time.sleep(2)
    
    # Remove duplicates from the final CSV file
    remove_duplicates_from_csv(output_file)
                
    print(f"\n=== Summary ===")
    print(f"Results saved to {output_file}")

if __name__ == "__main__":
    print("Starting CMS Cost Report scraper...")
    try:
        scrape_cost_reports()
    except Exception as e:
        print(f"Error in main function: {e}")
        print(traceback.format_exc()) 