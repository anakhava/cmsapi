import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import json
import uuid

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

def download_nppes_data():
    # URL of the NPPES files page
    url = "https://download.cms.gov/nppes/NPI_Files.html"
    
    # Specify the target directory
    target_dir = "/Users/arianakhavan/Documents/reference_data"
    
    # Create logs directory
    logs_dir = os.path.join(target_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    # Generate log filename
    run_id = str(uuid.uuid4())[:8]
    current_date = datetime.now().strftime("%Y_%m_%d")
    timestamp = datetime.now().strftime("%H_%M_%S")
    log_file = os.path.join(logs_dir, f"nppes_logs_run_{run_id}_{current_date}_{timestamp}.json")
    
    # Initialize log entries list
    log_entries = []
    
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
        
        # Get the file name from the href
        file_name = download_link['href']
        
        # Create the target directory if it doesn't exist
        os.makedirs(target_dir, exist_ok=True)
        
        # Download the file
        print(f"Downloading {file_name}...")
        file_response = requests.get(file_url, stream=True)
        file_response.raise_for_status()
        
        # Save the file
        file_path = os.path.join(target_dir, file_name)
        with open(file_path, 'wb') as f:
            for chunk in file_response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        print(f"Download completed! File saved to: {file_path}")
        
        # Log successful download
        log_entries.append(create_log_entry(
            200,
            f"Successfully downloaded file to {file_path}",
            True
        ))
        write_logs_to_file(log_entries, log_file)
        
        return file_path
        
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
    download_nppes_data() 