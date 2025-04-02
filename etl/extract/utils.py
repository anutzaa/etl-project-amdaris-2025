import os
from datetime import datetime, timedelta
import json

from logger import logger


def extract_date_from_timestamp(data):
    """Extracts date (YYYY-MM-DD) from timestamp in the JSON data file."""
    logger.debug("Extracting date from timestamp")
    if isinstance(data, dict) and "timestamp" in data:
        timestamp = data["timestamp"]
        if isinstance(timestamp, (int, float)):
            date_str = datetime.utcfromtimestamp(timestamp / 1000).strftime("%Y-%m-%d")
            logger.debug(f"Extracted date: {date_str}")
            return date_str
    logger.debug("Could not extract date from timestamp")
    return None


def process_api_response(response):
    """Process an API response: extract status code, handle errors, parse JSON."""

    response_code = response.status_code
    error_message = response.text if response_code != 200 else None

    if response_code != 200:
        logger.warning(f"API request failed with status code {response_code}: {error_message}")
    else:
        logger.debug("API request successful")

    response.raise_for_status()
    data = response.json()
    logger.debug("API response parsed successfully")

    return response_code, error_message, data


def save_to_file(data, api_type):
    """
       Saves API fetched data to a JSON file, appending it with a timestamp.
       The file is stored in a directory based on the API (either 'btc' or 'gold').
       """

    logger.debug(f"Saving {api_type} data to file")

    base_dir = '../../data/raw/'
    output_dir = os.path.join(base_dir, 'bitcoin' if api_type == 'btc' else 'gold')
    os.makedirs(output_dir, exist_ok=True)
    logger.debug(f"Output directory: {output_dir}")

    file_name = f'{api_type}_{datetime.today().strftime("%Y%m%d")}.json'
    file_path = os.path.join(output_dir, file_name)
    logger.debug(f"File path: {file_path}")

    if os.path.exists(file_path):
        logger.debug(f"File {file_path} exists, loading existing data")
        try:
            with open(file_path, 'r') as json_file:
                existing_data = json.load(json_file)
                if not isinstance(existing_data, list):
                    existing_data = [existing_data]
                    logger.debug("Converted non-list data to list")
            logger.debug(f"Loaded {len(existing_data)} existing data entries")
        except json.JSONDecodeError:
            logger.warning(f"Could not decode JSON in {file_path}, starting with empty list")
            existing_data = []
        file_created_date = datetime.fromtimestamp(os.path.getctime(file_path))
        logger.debug(f"File creation date: {file_created_date}")
    else:
        logger.debug(f"File {file_path} does not exist, creating new file")
        existing_data = []
        with open(file_path, 'w') as f:
            json.dump([], f)
        file_created_date = datetime.fromtimestamp(os.path.getctime(file_path))
        logger.debug(f"New file created at: {file_created_date}")

    file_last_modified_date = file_created_date
    logger.debug(f"Initial last modified date: {file_last_modified_date}")

    if api_type == 'gold':
        new_date = extract_date_from_timestamp(data.get("data", {}))
        new_currency = data.get("data", {}).get("base_currency")
        logger.debug(f"Checking for duplicate gold data: date={new_date}, currency={new_currency}")

        for item in existing_data:
            existing_date = extract_date_from_timestamp(item.get("data", {}))
            existing_currency = item.get("data", {}).get("base_currency")

            if existing_date == new_date and existing_currency == new_currency:
                logger.info(f"Duplicate gold data found for date={new_date}, currency={new_currency}")
                return file_path, file_created_date, file_last_modified_date

    elif api_type == 'btc':
        new_last_refreshed = data.get("Meta Data", {}).get("6. Last Refreshed", "")
        new_currency = data.get("Meta Data", {}).get("4. Market Code", "")
        logger.debug(f"Checking for duplicate BTC data: last_refreshed={new_last_refreshed}, currency={new_currency}")

        for item in existing_data:
            existing_last_refreshed = item.get("Meta Data", {}).get("6. Last Refreshed", "")
            existing_currency = item.get("Meta Data", {}).get("4. Market Code", "")

            if existing_last_refreshed == new_last_refreshed and existing_currency == new_currency:
                logger.info(f"Duplicate BTC data found for last_refreshed={new_last_refreshed}, currency={new_currency}")
                return file_path, file_created_date, file_last_modified_date

    logger.debug("No duplicate found, appending data")
    existing_data.append(data)

    with open(file_path, 'w') as json_file:
        json.dump(existing_data, json_file, indent=4)
    logger.debug(f"Wrote {len(existing_data)} entries to {file_path}")

    file_last_modified_date = datetime.fromtimestamp(os.path.getmtime(file_path))
    logger.debug(f"Updated last modified date: {file_last_modified_date}")

    return file_path, file_created_date, file_last_modified_date
