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
    
    # Find rows in the table that contain links
    # Look specifically for the div structure containing the links
    link_containers = soup.find_all('div', class_='js-form-item')
    print(f"Found {len(link_containers)} potential link containers")
    
    # Process each link container
    for container in link_containers:
        try:
            # Find the anchor tag
            link = container.find('a')
            if link:
                link_url = link.get('href')
                year_text = link.get_text().strip()
                
                # Try to determine year from link text or URL
                if year_text.isdigit():
                    year = year_text
                else:
                    # Try to extract year from URL
                    year_match = re.search(r'(\d{4})', year_text)
                    if year_match:
                        year = year_match.group(1)
                    else:
                        # If we can't find a year, use the text as is
                        year = year_text
                
                # Try to determine facility type from URL
                facility_match = re.search(r'/(snf|hha|hosp|hospital)-', link_url, re.IGNORECASE)
                if facility_match:
                    facility_type = facility_match.group(1).upper()
                    if facility_type == 'HOSP':
                        facility_type = 'HOSPITAL'
                else:
                    # If we can't determine facility type, check the parent row
                    parent_row = container.find_parent('tr')
                    if parent_row:
                        facility_cells = parent_row.find_all('td')
                        if len(facility_cells) >= 2:
                            facility_type = facility_cells[1].get_text().strip()
                        else:
                            facility_type = "UNKNOWN"
                    else:
                        facility_type = "UNKNOWN"
                
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
            print(f"Error processing link container: {e}")
            continue
    
    if not reports:
        print("No reports found using primary method. Trying alternative selectors...")
        # Look for plain table rows with links as a fallback
        rows = soup.find_all('tr')
        print(f"Found {len(rows)} table rows to process")
        
        for row in rows:
            try:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    link = cells[0].find('a')
                    if link:
                        year_text = link.get_text().strip()
                        year_match = re.search(r'(\d{4})', year_text)
                        if year_match:
                            year = year_match.group(1)
                            facility_type = cells[1].get_text().strip()
                            report_url = urljoin(base_url, link.get('href'))
                            
                            report_info = {
                                'year': year,
                                'facility_type': facility_type,
                                'report_url': report_url
                            }
                            reports.append(report_info)
                            print(f"Found report (alt method): Year {year}, Type {facility_type}")
            except Exception as e:
                print(f"Error processing row with alt method: {e}")
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
    
    # Look for links with zip, csv, or xlsx extensions
    download_links = soup.find_all('a', href=lambda href: href and (
        href.endswith('.zip') or href.endswith('.csv') or href.endswith('.xlsx')
    ))
    
    print(f"Found {len(download_links)} download links on the page")
    
    for link in download_links:
        try:
            file_name = link.get_text().strip()
            if not file_name:
                # If link text is empty, use the href
                file_name = link['href'].split('/')[-1]
                
            download_url = urljoin(base_url, link['href'])
            
            download_info = {
                'year': year,
                'facility_type': facility_type,
                'file_name': file_name,
                'download_url': download_url
            }
            
            print(f"Found download link: {file_name}")
            downloads.append(download_info)
            
        except Exception as e:
            print(f"Error processing download link: {e}")
            continue
            
    return downloads

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
                
    print(f"\n=== Summary ===")
    print(f"Results saved to {output_file}")
                

if __name__ == "__main__":
    print("Starting CMS Cost Report scraper...")
    try:
        scrape_cost_reports()
    except Exception as e:
        print(f"Error in main function: {e}")
        print(traceback.format_exc()) 