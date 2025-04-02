import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime

def download_nppes_data():
    # URL of the NPPES files page
    url = "https://download.cms.gov/nppes/NPI_Files.html"
    
    # Specify the target directory
    target_dir = "/Users/arianakhavan/Documents/reference_data"
    
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        # Parse the HTML content
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find the link with text containing 'NPPES Data Dissemination V.2'
        download_link = soup.find('a', string=lambda x: x and 'NPPES Data Dissemination V.2' in x)
        
        if not download_link:
            raise Exception("Could not find the download link")
        
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
        return file_path
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

if __name__ == "__main__":
    download_nppes_data() 