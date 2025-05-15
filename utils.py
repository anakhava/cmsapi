"""
Utility functions for CMS cost report scraping and downloading.
"""
import os
import sys
import time
import logging
from pathlib import Path
from typing import Optional, Dict, Any
import requests
from urllib.parse import urlparse

# Configure logging
def setup_logging(name: str) -> logging.Logger:
    """
    Set up logging configuration
    
    Args:
        name: Name of the logger
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(handler)
    
    return logger

def get_page_content(url: str, max_retries: int = 3, logger: Optional[logging.Logger] = None) -> Optional[str]:
    """
    Get HTML content from a URL with retry logic
    
    Args:
        url: URL to retrieve
        max_retries: Maximum number of retry attempts
        logger: Optional logger instance
        
    Returns:
        HTML content as string or None if failed
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    for attempt in range(max_retries):
        try:
            if logger:
                logger.info(f"Fetching URL: {url} (Attempt {attempt+1}/{max_retries})")
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as e:
            if logger:
                logger.error(f"Error fetching URL (Attempt {attempt+1}): {e}")
            if attempt < max_retries - 1:
                wait_time = 2 * (attempt + 1)  # Exponential backoff
                if logger:
                    logger.info(f"Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            else:
                if logger:
                    logger.error(f"Failed to fetch URL after {max_retries} attempts")
                return None

def download_file(url: str, output_path: Path, max_retries: int = 3, logger: Optional[logging.Logger] = None) -> bool:
    """
    Download a file from a URL with retry logic
    
    Args:
        url: URL to download from
        output_path: Path to save the file to
        max_retries: Maximum number of retry attempts
        logger: Optional logger instance
        
    Returns:
        True if download was successful, False otherwise
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # Create output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    for attempt in range(max_retries):
        try:
            if logger:
                logger.info(f"Downloading {url} to {output_path} (Attempt {attempt+1}/{max_retries})")
            
            # Stream the download to handle large files
            with requests.get(url, headers=headers, stream=True, timeout=30) as response:
                response.raise_for_status()
                total_size = int(response.headers.get('content-length', 0))
                
                # Download with progress tracking
                with open(output_path, 'wb') as f:
                    if total_size == 0:
                        f.write(response.content)
                    else:
                        downloaded = 0
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                if logger:
                                    progress = (downloaded / total_size) * 100
                                    logger.info(f"Download progress: {progress:.1f}%")
            
            return True
            
        except Exception as e:
            if logger:
                logger.error(f"Error downloading file (Attempt {attempt+1}): {e}")
            if attempt < max_retries - 1:
                wait_time = 2 * (attempt + 1)  # Exponential backoff
                if logger:
                    logger.info(f"Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            else:
                if logger:
                    logger.error(f"Failed to download file after {max_retries} attempts")
                return False

def clean_url(url: str) -> str:
    """
    Clean and normalize a URL
    
    Args:
        url: URL to clean
        
    Returns:
        Cleaned URL
    """
    # Remove leading/trailing whitespace
    url = url.strip()
    
    # Handle single-quoted URLs that might have spaces
    url = url.strip("'").strip()
    
    # Force HTTPS
    url = url.replace('http://', 'https://')
    
    # Normalize case for consistency
    url = url.replace('/FILES/', '/files/').replace('/HCRIS/', '/hcris/')
    
    return url

def get_file_extension(url: str) -> str:
    """
    Get the file extension from a URL
    
    Args:
        url: URL to get extension from
        
    Returns:
        File extension (including the dot) or empty string if no extension
    """
    parsed = urlparse(url)
    path = parsed.path
    ext = os.path.splitext(path)[1].lower()
    return ext if ext else '.zip'  # Default to .zip if no extension found 