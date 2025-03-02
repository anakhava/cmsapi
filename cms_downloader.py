import requests
import sys
import json
from pathlib import Path

def download_cms_data(uuid):
    """
    Download data from CMS API using provided UUID
    """
    # Corrected Base API URL
    base_url = "https://data.cms.gov/data-api/v1/dataset"
    
    try:
        # First get the dataset metadata
        metadata_url = f"{base_url}/{uuid}/data"
        metadata_response = requests.get(metadata_url)
        metadata_response.raise_for_status()
        
        # Print response for debugging
        print(f"Response status code: {metadata_response.status_code}")
        print(f"Response content: {metadata_response.text[:200]}...")
        
        try:
            data = metadata_response.json()
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON response: {e}")
            print("Full response:", metadata_response.text)
            sys.exit(1)
        
        # Create output filename based on UUID
        output_filename = f"cms_data_{uuid}.csv"
        
        # Convert JSON data to CSV and save
        if data:
            with open(output_filename, 'w', newline='') as f:
                if isinstance(data, list) and len(data) > 0:
                    import csv
                    writer = csv.DictWriter(f, fieldnames=data[0].keys())
                    writer.writeheader()
                    writer.writerows(data)
                    print(f"Successfully downloaded data to: {output_filename}")
                else:
                    print("No data found in the response")
                    sys.exit(1)
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading data: {e}")
        print(f"Response status code: {e.response.status_code if hasattr(e, 'response') else 'N/A'}")
        print(f"Response content: {e.response.text if hasattr(e, 'response') else 'N/A'}")
        sys.exit(1)

def main():
    if len(sys.argv) != 2:
        print("Usage: python cms_downloader.py <uuid>")
        sys.exit(1)
    
    uuid = sys.argv[1]
    download_cms_data(uuid)

if __name__ == "__main__":
    main() 