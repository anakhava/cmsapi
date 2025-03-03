import requests
import sys
import json
import csv
import pandas as pd
from pathlib import Path
from datetime import datetime
import uuid as uuid_lib

def create_log_entry(uuid, status_code, response_content, success, error_message=None):
    """
    Create a structured log entry
    """
    return {
        "timestamp": datetime.now().isoformat(),
        "uuid": uuid,
        "status_code": status_code,
        "response_content": response_content,
        "success": success,
        "error_message": error_message
    }

def download_cms_data(uuid, log_entries):
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
            # Log successful API response
            log_entries.append(create_log_entry(
                uuid,
                metadata_response.status_code,
                metadata_response.text[:1000],  # First 1000 chars of response
                True
            ))
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON response: {e}")
            print("Full response:", metadata_response.text)
            # Log JSON parse error
            log_entries.append(create_log_entry(
                uuid,
                metadata_response.status_code,
                metadata_response.text[:1000],
                False,
                f"JSON Parse Error: {str(e)}"
            ))
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
        error_response = e.response if hasattr(e, 'response') else None
        # Log request error
        log_entries.append(create_log_entry(
            uuid,
            error_response.status_code if error_response else 'N/A',
            error_response.text if error_response else 'N/A',
            False,
            str(e)
        ))
        return False

def process_uuid_file(csv_path):
    """
    Read UUIDs from a CSV file and download data for each
    """
    try:
        # Create logs directory
        logs_dir = Path("/Users/arianakhavan/Documents/reference_data/logs")
        logs_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate run ID and create log filename
        run_id = str(uuid_lib.uuid4())[:8]
        current_date = datetime.now().strftime("%Y_%m_%d")
        timestamp = datetime.now().strftime("%H_%M_%S")
        log_filename = logs_dir / f"logs_run_{run_id}_{current_date}_{timestamp}.json"
        
        # Initialize log entries list
        log_entries = []
        
        # Read the CSV file using pandas
        df = pd.read_csv(csv_path)
        
        if 'uuid' not in df.columns:
            print("Error: CSV file must contain a column named 'uuid'")
            sys.exit(1)
        
        total_uuids = len(df['uuid'])
        successful_downloads = 0
        failed_downloads = 0
        
        print(f"Found {total_uuids} UUIDs to process")
        print(f"Logging to: {log_filename}")
        
        # Process each UUID
        for index, uuid in enumerate(df['uuid'], 1):
            print(f"\nProcessing UUID {index} of {total_uuids}: {uuid}")
            if download_cms_data(uuid, log_entries):
                successful_downloads += 1
            else:
                failed_downloads += 1
        
        # Save logs to file
        with open(log_filename, 'w') as f:
            json.dump(log_entries, f, indent=2)
        
        # Print summary
        print(f"\nDownload Summary:")
        print(f"Total UUIDs processed: {total_uuids}")
        print(f"Successful downloads: {successful_downloads}")
        print(f"Failed downloads: {failed_downloads}")
        print(f"Logs saved to: {log_filename}")
        
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