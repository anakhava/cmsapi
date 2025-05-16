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
import threading
import queue
import time
import concurrent.futures
from functools import partial
import boto3
from botocore.exceptions import ClientError

# Global variables for log handling
log_file = None
log_entries = []

# Global variables for user input
input_queue = queue.Queue()
skip_current_dataset = False

class S3Storage:
    def __init__(self, bucket_name: str):
        """
        Initialize S3 storage handler
        
        Args:
            bucket_name: Name of the S3 bucket
        """
        self.s3_client = boto3.client('s3')
        self.bucket_name = bucket_name
    
    def exists(self, s3_key: str) -> bool:
        """
        Check if a file exists in S3
        
        Args:
            s3_key: S3 object key
            
        Returns:
            True if file exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise
    
    def upload_file(self, local_path: Path, s3_key: str) -> bool:
        """
        Upload a file to S3
        
        Args:
            local_path: Local path to the file
            s3_key: S3 object key
            
        Returns:
            True if upload successful, False otherwise
        """
        try:
            self.s3_client.upload_file(str(local_path), self.bucket_name, s3_key)
            return True
        except Exception as e:
            print(f"Error uploading to S3: {e}")
            return False

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
    
    # Add row count to the log entry if it exists
    if 'total_rows' in dataset_info:
        entry['total_rows'] = dataset_info['total_rows']
    
    log_entries.append(entry)
    
    # Write logs to file after every 5 entries or on errors
    if len(log_entries) >= 5 or not success:
        write_logs_to_file()
        
    return entry

def input_listener():
    """
    Background thread that listens for user input to skip current dataset
    """
    global input_queue
    while True:
        user_input = input()
        input_queue.put(user_input)

def check_for_skip_command():
    """
    Check if user has entered a command to skip the current dataset
    """
    global input_queue, skip_current_dataset
    
    try:
        # Check if there's any input in the queue (non-blocking)
        while not input_queue.empty():
            user_input = input_queue.get_nowait()
            # If user enters 's' or 'skip', set the flag to skip current dataset
            if user_input.lower() in ['s', 'skip']:
                print("\n*** User requested to skip current dataset ***")
                skip_current_dataset = True
                return True
    except queue.Empty:
        pass
    
    return False

def get_dataset_row_count(uuid):
    """
    Call the count endpoint to get the total number of rows in a dataset
    
    Args:
        uuid: The UUID of the dataset
        
    Returns:
        int: The number of rows in the dataset, or None if the request fails
    """
    base_url = "https://data.cms.gov/data-api/v1/dataset"
    # Use the correct endpoint URL pattern based on the sample response
    count_url = f"{base_url}/{uuid}/data-viewer/stats"
    
    try:
        print(f"Fetching row count from: {count_url}")
        response = requests.get(count_url, timeout=30)
        response.raise_for_status()
        
        # Check if we got a valid response with content
        if response.text.strip():
            try:
                count_data = response.json()
                
                # Check if the response has the expected structure
                if 'data' in count_data and 'found_rows' in count_data['data']:
                    row_count = count_data['data']['found_rows']
                    return row_count
                else:
                    print(f"Warning: Unexpected response format from count endpoint: {count_data}")
            except json.JSONDecodeError as json_err:
                print(f"JSON parse error: {json_err}")
                print(f"Response content: {response.text[:100]}...")
        else:
            print(f"Warning: Empty response received from count endpoint")
            
    except requests.exceptions.RequestException as e:
        print(f"Error getting row count for dataset {uuid}: {e}")
    
    return None

def download_cms_data(uuid, dataset_info, s3_storage=None):
    """
    Download data from CMS API using provided UUID and dataset info with pagination support
    Streams results directly to CSV without storing everything in memory
    
    Args:
        uuid: Dataset UUID
        dataset_info: Dictionary containing dataset information
        s3_storage: Optional S3Storage instance for uploading to S3
    """
    global skip_current_dataset
    
    # Create thread-local log entries list if not exists
    thread_local = threading.local()
    if not hasattr(thread_local, 'log_entries'):
        thread_local.log_entries = []
    
    # Create a thread-specific skip flag
    thread_skip = False
    
    # Use a unique prefix for thread output
    thread_id = threading.get_ident() % 10000
    prefix = f"[T-{thread_id}]"
    
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
        batch_size = 5000  # Increased to maximum allowed batch size of 5000
        more_data = True
        total_records = 0
        csv_writer = None
        csv_file = None
        
        # Get the expected row count
        expected_rows = dataset_info.get('total_rows')
        if expected_rows is not None:
            print(f"{prefix} Downloading data for {dataset_name} ({expected_rows:,} rows)...")
        else:
            print(f"{prefix} Downloading data for {dataset_name} (row count unknown)...")
        
        # Loop to handle pagination
        while more_data and not thread_skip and not skip_current_dataset:
            # Check if global skip flag is set (affects all threads)
            if check_for_skip_command():
                if csv_file:
                    csv_file.close()
                print(f"{prefix} Skipping dataset: {dataset_name}")
                thread_local.log_entries.append(create_log_entry_thread_safe(
                    uuid,
                    'SKIPPED',
                    f"User manually skipped after retrieving {total_records} rows.",
                    False,
                    dataset_info,
                    "User manually interrupted download"
                ))
                return False
                
            # Rest of the function remains the same, but add the thread prefix to all print statements
            metadata_url = f"{base_url}/{uuid}/data?size={batch_size}&offset={offset}"
            
            # Only print offset info for first page or every 2 pages (10,000 records)
            if offset == 0 or offset % 10000 == 0:
                if expected_rows and expected_rows > 0:
                    progress_pct = (total_records / expected_rows) * 100
                    print(f"{prefix} Fetching data at offset {offset}... ({total_records:,}/{expected_rows:,} rows, {progress_pct:.1f}%)")
                else:
                    print(f"{prefix} Fetching data at offset {offset}...")
            
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
                        if total_records % 10000 == 0 or page_count < batch_size:
                            if expected_rows and expected_rows > 0:
                                progress_pct = (total_records / expected_rows) * 100
                                print(f"{prefix} Written {total_records:,} of {expected_rows:,} rows so far... ({progress_pct:.1f}%)")
                            else:
                                print(f"{prefix} Written {total_records:,} rows so far...")
                    
                    # If we got fewer records than the batch size, we've reached the end
                    if page_count < batch_size:
                        more_data = False
                    else:
                        # Move to the next page
                        offset += batch_size
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
                print(f"{prefix} Failed to parse JSON response: {e}")
                thread_local.log_entries.append(create_log_entry_thread_safe(
                    uuid,
                    metadata_response.status_code,
                    metadata_response.text[:1000],
                    False,
                    dataset_info,
                    f"JSON Parse Error: {str(e)}"
                ))
                # Close file if open
                if csv_file:
                    csv_file.close()
                return False
        
        # Close the CSV file
        if csv_file:
            csv_file.close()
            if expected_rows and expected_rows > 0:
                print(f"{prefix} Successfully downloaded {total_records:,} of {expected_rows:,} rows to: {output_filename}")
                
                # Check if we got all the rows we expected
                if total_records < expected_rows:
                    print(f"{prefix} Warning: Downloaded fewer rows ({total_records:,}) than expected ({expected_rows:,})")
            else:
                print(f"{prefix} Successfully downloaded {total_records:,} rows to: {output_filename}")
        
        # Log successful API response
        thread_local.log_entries.append(create_log_entry_thread_safe(
            uuid,
            200,
            f"Successfully retrieved {total_records:,} rows",
            True,
            dataset_info
        ))
        
        # After download is complete, upload to S3 if s3_storage is provided
        if s3_storage and output_filename.exists():
            s3_key = f"cms-datasets/{safe_dataset_name}_{uuid}.csv"
            
            # Check if file already exists in S3
            if s3_storage.exists(s3_key):
                print(f"{prefix} File already exists in S3: {s3_key}")
            else:
                print(f"{prefix} Uploading to S3: {s3_key}")
                if s3_storage.upload_file(output_filename, s3_key):
                    print(f"{prefix} Upload successful")
                else:
                    print(f"{prefix} Upload failed")
        
        return True
            
    except Exception as e:
        print(f"{prefix} Error processing dataset {dataset_name}: {e}")
        return False

# Create a thread-safe version of the log entry function
def create_log_entry_thread_safe(uuid, status_code, response_content, success, dataset_info, error_message=None):
    """
    Create a structured log entry but don't add it to the global list
    Returns the entry for thread-local storage
    """
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
    
    # Add row count to the log entry if it exists
    if 'total_rows' in dataset_info:
        entry['total_rows'] = dataset_info['total_rows']
    
    return entry

def process_cms_datasets_file(csv_path="cms_datasets.csv", s3_bucket=None):
    """
    Process the CMS datasets CSV file and download each dataset
    
    Args:
        csv_path: Path to the CSV file containing dataset information
        s3_bucket: Optional S3 bucket name for uploading datasets
    """
    # Initialize S3 storage if bucket is provided
    s3_storage = S3Storage(s3_bucket) if s3_bucket else None
    
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
        skipped_datasets = 0
        
        print(f"Found {total_datasets} datasets with valid UUIDs to process")
        print("Type 's' or 'skip' at any time to skip the current dataset and move to the next one")
        
        # Prepare datasets by getting their row count first
        datasets_with_info = []
        for i, (index, row) in enumerate(df.iterrows()):
            print(f"\nChecking dataset {i + 1} of {total_datasets}: {row['title']}")
            # Get the row count for this dataset
            row_count = get_dataset_row_count(row['uuid'])
            if row_count is not None:
                print(f"Dataset contains {row_count:,} rows")
            else:
                print("Could not determine row count for this dataset")
            
            # Create dataset info dictionary
            dataset_info = {
                'dataset_name': row['title'],
                'dataset_description': row['description'],
                'dataset_notes': row.get('dataset_url', ''),
                'total_rows': row_count
            }
            datasets_with_info.append((row['uuid'], dataset_info))
        
        # Split datasets into large (>1M rows) and small
        large_datasets = []
        small_datasets = []
        for uuid, info in datasets_with_info:
            if info.get('total_rows', 0) and info['total_rows'] > 1000000:
                large_datasets.append((uuid, info))
            else:
                small_datasets.append((uuid, info))
        
        print(f"\nFound {len(large_datasets)} large datasets and {len(small_datasets)} smaller datasets")
        
        # Process small datasets sequentially
        for uuid, dataset_info in small_datasets:
            print(f"\nProcessing smaller dataset: {dataset_info['dataset_name']}")
            if download_cms_data(uuid, dataset_info, s3_storage):
                successful_downloads += 1
            else:
                failed_downloads += 1
                if len(log_entries) > 0 and log_entries[-1].get('status_code') == 'SKIPPED':
                    skipped_datasets += 1
        
        # Process large datasets in parallel with a controlled number of workers
        if large_datasets:
            print(f"\nProcessing {len(large_datasets)} large datasets in parallel...")
            max_workers = min(3, len(large_datasets))  # Limit to 3 parallel downloads
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(download_cms_data, uuid, dataset_info, s3_storage) for uuid, dataset_info in large_datasets]
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            successful_downloads += 1
                        else:
                            failed_downloads += 1
                    except Exception as e:
                        print(f"Error in parallel download: {e}")
                        failed_downloads += 1
        
        # Ensure all remaining logs are written
        write_logs_to_file()
        
        # Print summary
        print(f"\nDownload Summary:")
        print(f"Total datasets processed: {total_datasets}")
        print(f"Successful downloads: {successful_downloads}")
        print(f"Failed downloads: {failed_downloads}")
        print(f"  of which manually skipped: {skipped_datasets}")
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
    """Main function to run the downloader"""
    # Configuration
    CSV_PATH = 'cms_datasets.csv'
    S3_BUCKET = 'downloaded-data'  # Set your S3 bucket name here
    
    # Register signal handlers for graceful exit
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start input listener thread
    input_thread = threading.Thread(target=input_listener, daemon=True)
    input_thread.start()
    
    # Process datasets
    process_cms_datasets_file(CSV_PATH, S3_BUCKET)

if __name__ == "__main__":
    main()