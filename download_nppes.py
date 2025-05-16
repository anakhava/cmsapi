import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import json
import uuid
import boto3
from botocore.exceptions import ClientError
from pathlib import Path
from tqdm import tqdm
import tempfile

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
    
    def upload_file(self, file_path: Path, s3_key: str, total_size: int):
        with tqdm(total=total_size, unit='B', unit_scale=True, desc="Uploading to S3") as pbar:
            def upload_callback(bytes_transferred):
                pbar.update(bytes_transferred - pbar.n)
            self.s3_client.upload_file(
                str(file_path),
                self.bucket_name,
                s3_key,
                Callback=upload_callback
            )

def create_log_entry(status_code, response_content, success, error_message=None):
    """
    Create a structured log entry
    """
    entry = {
        "timestamp": datetime.now().isoformat(),
        "uuid": str(uuid.uuid4()),
        "dataset_name": "NPPES Data Dissemination V.2",
        "dataset_description": "National Plan and Provider Enumeration System (NPPES) Data Dissemination File",
        "status_code": status_code,
        "response_content": response_content,
        "success": success,
        "error_message": error_message
    }
    return entry

def write_logs_to_file(log_entries, log_file):
    """
    Write log entries to the log file
    """
    try:
        # Read existing logs if file exists and has content
        existing_logs = []
        if os.path.exists(log_file) and os.path.getsize(log_file) > 0:
            with open(log_file, 'r') as f:
                existing_logs = json.load(f)
        
        # Combine existing logs with new entries
        all_logs = existing_logs + log_entries
        
        # Write all logs back to file
        with open(log_file, 'w') as f:
            json.dump(all_logs, f, indent=2)
            
    except Exception as e:
        print(f"Error writing logs to file: {e}")

def get_s3_key(file_name: str) -> str:
    """
    Generate S3 key for a file
    
    Args:
        file_name: Name of the file
        
    Returns:
        S3 object key
    """
    # Remove any leading ./ from the filename
    clean_name = file_name.lstrip('./')
    # Create S3 key structure: nppes/filename
    return f"nppes/{clean_name}"

def download_nppes_data(s3_bucket=None):
    # URL of the NPPES files page
    url = "https://download.cms.gov/nppes/NPI_Files.html"
    
    # Create logs directory
    logs_dir = os.path.join("/Users/arianakhavan/Documents/reference_data", "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    # Generate log filename
    run_id = str(uuid.uuid4())[:8]
    current_date = datetime.now().strftime("%Y_%m_%d")
    timestamp = datetime.now().strftime("%H_%M_%S")
    log_file = os.path.join(logs_dir, f"nppes_logs_run_{run_id}_{current_date}_{timestamp}.json")
    
    # Initialize log entries list
    log_entries = []
    
    # Initialize S3 storage if bucket is provided
    s3_storage = S3Storage(s3_bucket) if s3_bucket else None
    
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the link with text containing 'NPPES Data Dissemination V.2'
        download_link = soup.find('a', string=lambda x: x and 'NPPES Data Dissemination V.2' in x)
        
        if not download_link:
            error_msg = "Could not find the download link"
            log_entries.append(create_log_entry(
                'NOT_FOUND',
                response.text[:1000],
                False,
                error_msg
            ))
            write_logs_to_file(log_entries, log_file)
            raise Exception(error_msg)
        
        # Get the href attribute and construct the full URL
        file_url = url.rsplit('/', 1)[0] + '/' + download_link['href']
        
        # Get the file name from the href and clean it
        file_name = download_link['href'].lstrip('./')
        
        # Check if file exists in S3
        if s3_storage:
            s3_key = get_s3_key(file_name)
            if s3_storage.exists(s3_key):
                print(f"File already exists in S3: {s3_key}")
                log_entries.append(create_log_entry(
                    200,
                    f"File already exists in S3: {s3_key}",
                    True
                ))
                write_logs_to_file(log_entries, log_file)
                return None
        
        # Get file size for progress bar
        head_response = requests.head(file_url)
        total_size = int(head_response.headers.get('content-length', 0))
        
        # Download the file
        print(f"Downloading {file_name} to temporary file...")
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_path = Path(tmp_file.name)
            with requests.get(file_url, stream=True) as file_response:
                file_response.raise_for_status()
                with tqdm(total=total_size, unit='B', unit_scale=True, desc="Downloading") as pbar:
                    for chunk in file_response.iter_content(chunk_size=8192):
                        if chunk:
                            tmp_file.write(chunk)
                            pbar.update(len(chunk))
        print(f"Download completed! Temp file: {tmp_path}")
        
        if s3_storage:
            # Upload directly to S3
            print(f"Uploading {file_name} to S3 as {s3_key}...")
            s3_storage.upload_file(tmp_path, s3_key, total_size)
            print(f"Successfully uploaded to S3: {s3_key}")
            log_entries.append(create_log_entry(
                200,
                f"Successfully downloaded and uploaded to S3: {s3_key}",
                True
            ))
        else:
            # If no S3 bucket specified, save locally
            target_dir = "/Users/arianakhavan/Documents/reference_data"
            os.makedirs(target_dir, exist_ok=True)
            final_path = Path(target_dir) / file_name
            tmp_path.replace(final_path)
            print(f"Download completed! File saved to: {final_path}")
            log_entries.append(create_log_entry(
                200,
                f"Successfully downloaded file to {final_path}",
                True
            ))
        
        # Clean up temp file if it still exists
        if tmp_path.exists():
            tmp_path.unlink()
            print(f"Temporary file {tmp_path} deleted.")
        
        write_logs_to_file(log_entries, log_file)
        return file_name
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Error downloading file: {e}"
        print(error_msg)
        log_entries.append(create_log_entry(
            getattr(e.response, 'status_code', 'N/A'),
            getattr(e.response, 'text', 'N/A')[:1000],
            False,
            error_msg
        ))
        write_logs_to_file(log_entries, log_file)
        return None
    except Exception as e:
        error_msg = f"An error occurred: {e}"
        print(error_msg)
        log_entries.append(create_log_entry(
            'ERROR',
            str(e),
            False,
            error_msg
        ))
        write_logs_to_file(log_entries, log_file)
        return None

if __name__ == "__main__":
    # Configure S3 bucket name if needed
    S3_BUCKET = 'downloaded-data'  # Set to None to disable S3 upload
    download_nppes_data(s3_bucket=S3_BUCKET) 