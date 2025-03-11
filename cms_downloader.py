import requests
import sys
import json
import csv
import pandas as pd
from pathlib import Path
from datetime import datetime
import uuid as uuid_lib

def create_log_entry(uuid, status_code, response_content, success, dataset_info, error_message=None):
    """
    Create a structured log entry
    """
    return {
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

def download_cms_data(uuid, dataset_info, log_entries):
    """
    Download data from CMS API using provided UUID and dataset info
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
                True,
                dataset_info
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
                dataset_info,
                f"JSON Parse Error: {str(e)}"
            ))
            return False
        
        # Create output filename using dataset name
        # Handle NaN or empty dataset names
        dataset_name = dataset_info.get('dataset_name', '')
        if pd.isna(dataset_name) or dataset_name == '':
            dataset_name = f"Unknown_Dataset_{uuid[:8]}"
        else:
            dataset_name = str(dataset_name)
            
        safe_dataset_name = dataset_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
        output_filename = output_dir / f"{safe_dataset_name}_{uuid}.csv"
        
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
            dataset_info,
            str(e)
        ))
        return False

def process_cms_datasets_file(csv_path="cms_datasets.csv"):
    """
    Read dataset information from cms_datasets.csv and download data for each
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
        print(f"Logging to: {log_filename}")
        
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
            
            if download_cms_data(row['uuid'], dataset_info, log_entries):
                successful_downloads += 1
            else:
                failed_downloads += 1
        
        # Save logs to file
        with open(log_filename, 'w') as f:
            json.dump(log_entries, f, indent=2)
        
        # Print summary
        print(f"\nDownload Summary:")
        print(f"Total datasets processed: {total_datasets}")
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
    if len(sys.argv) > 1:
        csv_path = sys.argv[1]
    else:
        csv_path = "cms_datasets.csv"  # Default to cms_datasets.csv if no argument provided
    
    print(f"Using dataset file: {csv_path}")
    process_cms_datasets_file(csv_path)

if __name__ == "__main__":
    main() 