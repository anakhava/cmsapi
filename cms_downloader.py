import requests
import sys
import json
import csv
import pandas as pd
from pathlib import Path
from datetime import datetime
import uuid as uuid_lib
import atexit
import signal

# Global variables for log handling
log_file = None
log_entries = []

def write_logs_to_file():
    """
    Write current log entries to the log file
    """
    global log_file, log_entries
    if log_file and log_entries:
        try:
            # Read existing logs if file exists and has content
            existing_logs = []
            if log_file.exists() and log_file.stat().st_size > 0:
                with open(log_file, 'r') as f:
                    existing_logs = json.load(f)
                
            # Combine existing logs with new entries
            all_logs = existing_logs + log_entries
            
            # Write all logs back to file
            with open(log_file, 'w') as f:
                json.dump(all_logs, f, indent=2)
                
            # Clear the log entries after writing
            log_entries = []
        except Exception as e:
            print(f"Error writing logs to file: {e}")

def signal_handler(sig, frame):
    """
    Handle keyboard interrupts and other signals
    """
    print("\nProgram interrupted! Writing logs before exit...")
    write_logs_to_file()
    print("Logs saved. Exiting.")
    sys.exit(0)

def create_log_entry(uuid, status_code, response_content, success, dataset_info, error_message=None):
    """
    Create a structured log entry and add it to the global log entries list
    """
    global log_entries
    
    entry = {
        "timestamp": datetime.now().isoformat(),
        "uuid": uuid,
        "dataset_name": dataset_info.get('dataset_name', ''),
        "dataset_description": dataset_info.get('dataset_description', ''),
        "dataset_notes": dataset_info.get('dataset_notes', ''),
        "status_code": status_code,
        "response_content": response_content,
        "success": success,
        "error_message": error_message
    }
    
    log_entries.append(entry)
    
    # Write logs to file after every 5 entries or on errors
    if len(log_entries) >= 5 or not success:
        write_logs_to_file()
        
    return entry

def download_cms_data(uuid, dataset_info):
    """
    Download data from CMS API using provided UUID and dataset info with pagination support
    Streams results directly to CSV without storing everything in memory
    """
    # Define output directory
    output_dir = Path("/Users/arianakhavan/Documents/reference_data")
    
    # Create the directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Corrected Base API URL
    base_url = "https://data.cms.gov/data-api/v1/dataset"
    
    try:
        # Handle NaN or empty dataset names
        dataset_name = dataset_info.get('dataset_name', '')
        if pd.isna(dataset_name) or dataset_name == '':
            dataset_name = f"Unknown_Dataset_{uuid[:8]}"
        else:
            dataset_name = str(dataset_name)
            
        safe_dataset_name = dataset_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
        output_filename = output_dir / f"{safe_dataset_name}_{uuid}.csv"
        
        # Initialize variables for pagination
        offset = 0
        limit = 1000
        more_data = True
        total_records = 0
        csv_writer = None
        csv_file = None
        
        print(f"Downloading data for {dataset_name}...")
        
        # Loop to handle pagination
        while more_data:
            # Construct URL with pagination parameters
            metadata_url = f"{base_url}/{uuid}/data?offset={offset}&limit={limit}"
            
            # Only print offset info for first page or every 10 pages
            if offset == 0 or offset % 10000 == 0:
                print(f"Fetching data at offset {offset}...")
            
            metadata_response = requests.get(metadata_url)
            metadata_response.raise_for_status()
            
            try:
                page_data = metadata_response.json()
                
                if isinstance(page_data, list):
                    page_count = len(page_data)
                    
                    # Initialize CSV writer on first batch
                    if csv_file is None and page_count > 0:
                        csv_file = open(output_filename, 'w', newline='')
                        csv_writer = csv.DictWriter(csv_file, fieldnames=page_data[0].keys())
                        csv_writer.writeheader()
                    
                    # Stream this batch to CSV directly
                    if csv_writer and page_count > 0:
                        csv_writer.writerows(page_data)
                        total_records += page_count
                        
                        # Update progress every 10,000 records
                        if total_records % 10000 == 0 or page_count < limit:
                            print(f"Written {total_records} records so far...")
                    
                    # If we got fewer records than the limit, we've reached the end
                    if page_count < limit:
                        more_data = False
                    else:
                        # Move to the next page
                        offset += limit
                else:
                    # Handle non-list response (single object)
                    if csv_file is None:
                        csv_file = open(output_filename, 'w', newline='')
                        if isinstance(page_data, dict):
                            csv_writer = csv.DictWriter(csv_file, fieldnames=page_data.keys())
                            csv_writer.writeheader()
                            csv_writer.writerow(page_data)
                            total_records = 1
                    more_data = False
                    
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON response: {e}")
                create_log_entry(
                    uuid,
                    metadata_response.status_code,
                    metadata_response.text[:1000],
                    False,
                    dataset_info,
                    f"JSON Parse Error: {str(e)}"
                )
                # Close file if open
                if csv_file:
                    csv_file.close()
                return False
        
        # Close the CSV file
        if csv_file:
            csv_file.close()
            print(f"Successfully downloaded {total_records} records to: {output_filename}")
        
        # Log successful API response
        create_log_entry(
            uuid,
            200,
            f"Successfully retrieved {total_records} records",
            True,
            dataset_info
        )
        
        return True if total_records > 0 else False
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading data: {e}")
        error_response = e.response if hasattr(e, 'response') else None
        create_log_entry(
            uuid,
            error_response.status_code if error_response else 'N/A',
            error_response.text[:1000] if error_response else 'N/A',
            False,
            dataset_info,
            str(e)
        )
        # Close file if open
        if 'csv_file' in locals() and csv_file:
            csv_file.close()
        return False

def process_cms_datasets_file(csv_path="cms_datasets.csv"):
    """
    Read dataset information from cms_datasets.csv and download data for each
    """
    global log_file
    
    try:
        # Create logs directory
        logs_dir = Path("/Users/arianakhavan/Documents/reference_data/logs")
        logs_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate run ID and create log filename
        run_id = str(uuid_lib.uuid4())[:8]
        current_date = datetime.now().strftime("%Y_%m_%d")
        timestamp = datetime.now().strftime("%H_%M_%S")
        log_file = logs_dir / f"logs_run_{run_id}_{current_date}_{timestamp}.json"
        
        print(f"Logging to: {log_file}")
        
        # Initialize the log file with an empty array
        with open(log_file, 'w') as f:
            json.dump([], f)
        
        # Read the CSV file using pandas
        df = pd.read_csv(csv_path)
        
        # Check for required columns
        required_columns = ['title', 'description', 'api_url', 'uuid', 'dataset_url']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Error: CSV file must contain these columns: {', '.join(required_columns)}")
            print(f"Missing columns: {', '.join(missing_columns)}")
            sys.exit(1)
        
        # Filter out rows with empty UUIDs
        df = df[df['uuid'].notna() & (df['uuid'] != '')]
        
        total_datasets = len(df)
        successful_downloads = 0
        failed_downloads = 0
        
        print(f"Found {total_datasets} datasets with valid UUIDs to process")
        
        # Process each row
        for index, row in df.iterrows():
            print(f"\nProcessing dataset {index + 1} of {total_datasets}")
            print(f"UUID: {row['uuid']}")
            print(f"Dataset: {row['title']}")
            print(f"Description: {row['description'][:100]}...")  # Show first 100 chars
            
            # Create dataset info dictionary
            dataset_info = {
                'dataset_name': row['title'],
                'dataset_description': row['description'],
                'dataset_notes': row.get('dataset_url', '')  # Use dataset_url as notes
            }
            
            if download_cms_data(row['uuid'], dataset_info):
                successful_downloads += 1
            else:
                failed_downloads += 1
        
        # Ensure all remaining logs are written
        write_logs_to_file()
        
        # Print summary
        print(f"\nDownload Summary:")
        print(f"Total datasets processed: {total_datasets}")
        print(f"Successful downloads: {successful_downloads}")
        print(f"Failed downloads: {failed_downloads}")
        print(f"Logs saved to: {log_file}")
        
    except FileNotFoundError:
        print(f"Error: Could not find CSV file at {csv_path}")
        sys.exit(1)
    except pd.errors.EmptyDataError:
        print(f"Error: The CSV file is empty")
        sys.exit(1)
    except Exception as e:
        print(f"Error processing CSV file: {e}")
        # Make sure logs are written even on exception
        write_logs_to_file()
        sys.exit(1)

def main():
    # Register signal handlers for graceful exit
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Termination signal
    
    # Register exit handler to ensure logs are written
    atexit.register(write_logs_to_file)
    
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    else:
        csv_path = "cms_datasets.csv"  # Default to cms_datasets.csv if no argument provided
    
    print(f"Using dataset file: {csv_path}")
    process_cms_datasets_file(csv_path)

if __name__ == "__main__":
    main() 