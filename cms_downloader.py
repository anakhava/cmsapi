import requests
import sys
import json
import csv
import pandas as pd
from pathlib import Path

def download_cms_data(uuid):
    """
    Download data from CMS API using provided UUID
    """
    # Define output directory
    output_dir = Path("/Users/arianakhavan/Documents/reference_data")
    
    # Create the directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
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
            return False
        
        # Create output filename in the specified directory
        output_filename = output_dir / f"cms_data_{uuid}.csv"
        
        # Convert JSON data to CSV and save
        if data:
            with open(output_filename, 'w', newline='') as f:
                if isinstance(data, list) and len(data) > 0:
                    import csv
                    writer = csv.DictWriter(f, fieldnames=data[0].keys())
                    writer.writeheader()
                    writer.writerows(data)
                    print(f"Successfully downloaded data to: {output_filename}")
                    return True
                else:
                    print("No data found in the response")
                    return False
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading data: {e}")
        print(f"Response status code: {e.response.status_code if hasattr(e, 'response') else 'N/A'}")
        print(f"Response content: {e.response.text if hasattr(e, 'response') else 'N/A'}")
        return False

def process_uuid_file(csv_path):
    """
    Read UUIDs from a CSV file and download data for each
    """
    try:
        # Read the CSV file using pandas
        df = pd.read_csv(csv_path)
        
        if 'uuid' not in df.columns:
            print("Error: CSV file must contain a column named 'uuid'")
            sys.exit(1)
        
        total_uuids = len(df['uuid'])
        successful_downloads = 0
        failed_downloads = 0
        
        print(f"Found {total_uuids} UUIDs to process")
        
        # Process each UUID
        for index, uuid in enumerate(df['uuid'], 1):
            print(f"\nProcessing UUID {index} of {total_uuids}: {uuid}")
            if download_cms_data(uuid):
                successful_downloads += 1
            else:
                failed_downloads += 1
        
        # Print summary
        print(f"\nDownload Summary:")
        print(f"Total UUIDs processed: {total_uuids}")
        print(f"Successful downloads: {successful_downloads}")
        print(f"Failed downloads: {failed_downloads}")
        
    except FileNotFoundError:
        print(f"Error: Could not find CSV file at {csv_path}")
        sys.exit(1)
    except pd.errors.EmptyDataError:
        print(f"Error: The CSV file is empty")
        sys.exit(1)
    except Exception as e:
        print(f"Error processing CSV file: {e}")
        sys.exit(1)

def main():
    if len(sys.argv) != 2:
        print("Usage: python cms_downloader.py <path_to_csv>")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    process_uuid_file(csv_path)

if __name__ == "__main__":
    main() 