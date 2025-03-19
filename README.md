# CMS Data Scraper and Downloader

A comprehensive toolset for scraping, extracting, and downloading healthcare datasets from the Centers for Medicare & Medicaid Services (CMS) data portal.

## Overview

This project provides two main tools:

1. **API URL Scraper (`api_url_scraper.py`)**: Scrapes the CMS data portal website to extract dataset metadata, including API URLs and UUIDs required for programmatic data access.

2. **Data Downloader (`cms_downloader.py`)**: Uses the extracted dataset information to download the actual data from CMS APIs, with support for large datasets through pagination.

## Prerequisites

### Required Python Packages

```
requests>=2.31.0
pandas>=2.0.0
selenium>=4.0.0
webdriver_manager
```

You can install the required dependencies using:

```bash
pip install -r requirements.txt
```

Note: The `api_url_scraper.py` requires additional Selenium-related packages that are not listed in `requirements.txt`. You may need to install them separately.

## Component Details

### 1. API URL Scraper (`api_url_scraper.py`)

This script scrapes the CMS data portal to collect information about available datasets, including:
- Dataset titles and descriptions
- API URLs
- Dataset UUIDs
- Original dataset URLs

#### How It Works

1. Utilizes Selenium to navigate through the CMS data search pages
2. Extracts dataset URLs from search results
3. Visits each dataset page to extract detailed metadata
4. Extracts API URLs and UUIDs from each dataset page
5. Exports the collected information to a CSV file (`cms_datasets.csv`)

#### Usage

```bash
python api_url_scraper.py
```

The script will:
- Navigate through multiple search result pages
- Follow links to individual dataset pages
- Extract API URLs and UUIDs
- Save results to `cms_datasets.csv`

### 2. Data Downloader (`cms_downloader.py`)

This script uses the information collected by the API URL scraper to download the actual dataset content from CMS APIs.

#### Features

- Paginated data retrieval to handle large datasets
- CSV streaming to minimize memory usage
- Comprehensive logging of download attempts
- Skip functionality to bypass problematic datasets
- Progress reporting during downloads

#### Usage

```bash
python cms_downloader.py [path_to_csv_file]
```

Where:
- `path_to_csv_file` is optional and defaults to `cms_datasets.csv` if not provided

During execution:
- Type `s` or `skip` and press Enter at any time to skip the current dataset
- Progress and status updates are displayed in the console
- Download results are logged to a JSON file for tracking

#### Output Location

Downloaded data is saved to:
```
/Users/arianakhavan/Documents/reference_data/
```

Logs are saved to:
```
/Users/arianakhavan/Documents/reference_data/logs/
```

**Note**: You may need to modify these paths in the code to match your environment.

## Data Structure

### `cms_datasets.csv` (Output from API URL Scraper)

This CSV file contains the following columns:
- `title`: Dataset title
- `description`: Dataset description
- `api_url`: API URL for accessing the dataset
- `uuid`: Unique identifier for the dataset
- `dataset_url`: Original URL of the dataset page

### Downloaded Dataset Files

Each downloaded dataset is saved as a CSV file with a name format:
```
{safe_dataset_name}_{uuid}.csv
```

Where:
- `safe_dataset_name` is the dataset title with spaces and special characters replaced with underscores
- `uuid` is the unique identifier for the dataset

## Logging

The data downloader creates detailed logs for each run in JSON format, including:
- Timestamp
- Dataset information
- Success/failure status
- Response details
- Error messages if applicable

Log files are named with the format:
```
logs_run_{run_id}_{date}_{timestamp}.json
```

## Error Handling

Both scripts include comprehensive error handling, including:
- Network errors
- Parsing errors
- Timeouts
- Graceful exits on keyboard interrupts

## Troubleshooting

### Common Issues

1. **Selenium WebDriver Issues**: If you encounter errors with the Chrome WebDriver in `api_url_scraper.py`, ensure you have Chrome installed and try updating the WebDriver:
   ```python
   from webdriver_manager.chrome import ChromeDriverManager
   service = Service(ChromeDriverManager().install())
   ```

2. **Rate Limiting**: CMS APIs may impose rate limits. If you encounter HTTP 429 errors, consider adding delays between requests.

3. **Permission Issues**: If you cannot write to the output directories, modify the paths in the code or ensure you have the necessary write permissions.

### Restarting Downloads

If downloads are interrupted, you can restart the process. The downloader will create new files and logs for each run.

## License

This project is provided as-is with no explicit license.

## Acknowledgments

This tool interacts with the CMS data portal and APIs to retrieve publicly available healthcare datasets. Please ensure you comply with CMS terms of service when using this tool.