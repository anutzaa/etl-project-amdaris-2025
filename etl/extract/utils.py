import os
from datetime import datetime
import json

from etl.extract.logger import logger


def process_api_response(response):
    """Process an API response: extract status code, handle errors, parse JSON."""

    response_code = response.status_code
    error_message = response.text if response_code != 200 else None

    if response_code != 200:
        logger.warning(
            f"API request failed with status code {response_code}: {error_message}"
        )
    else:
        logger.debug("API request successful")

    response.raise_for_status()
    data = response.json()
    logger.debug("API response parsed successfully")

    return response_code, error_message, data


def save_to_file(data, api_type):
    """
    Saves API fetched data to a JSON file.
    The file is stored in a directory based on the API (either 'btc' or 'gold').
    """

    logger.debug(f"Saving {api_type} data to file")

    base_dir = '../data/raw/'
    output_dir = os.path.join(
        base_dir, 'bitcoin' if api_type == 'btc' else 'gold'
    )
    os.makedirs(output_dir, exist_ok=True)
    logger.debug(f"Output directory: {output_dir}")

    file_name = f'{api_type}_{datetime.today().strftime("%Y%m%d_%H%M%S_%f")[:-3]}.json'
    file_path = os.path.join(output_dir, file_name)
    logger.debug(f"File path: {file_path}")

    with open(file_path, 'w') as json_file:
        json.dump(data, json_file, indent=4)
    logger.debug(f"Saved new API response to {file_path}")

    file_created_date = datetime.fromtimestamp(os.path.getctime(file_path))
    file_last_modified_date = file_created_date
    logger.debug(f"File created at: {file_created_date}")

    return file_path, file_created_date, file_last_modified_date
