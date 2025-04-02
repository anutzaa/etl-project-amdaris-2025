import os
from datetime import datetime, timedelta
import json


def extract_date_from_timestamp(data):
    """Extracts date (YYYY-MM-DD) from timestamp in the JSON data file."""

    if isinstance(data, dict) and "timestamp" in data:
        timestamp = data["timestamp"]
        if isinstance(timestamp, (int, float)):
            return datetime.utcfromtimestamp(timestamp / 1000).strftime("%Y-%m-%d")
    return None


def save_to_file(data, api_type):
    """
       Saves API fetched data to a JSON file, appending it with a timestamp.
       The file is stored in a directory based on the API (either 'btc' or 'gold').
       """

    base_dir = '../../data/raw/'
    output_dir = os.path.join(base_dir, 'bitcoin' if api_type == 'btc' else 'gold')
    os.makedirs(output_dir, exist_ok=True)

    file_name = f'{api_type}_{datetime.today().strftime("%Y%m%d")}.json'
    file_path = os.path.join(output_dir, file_name)

    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as json_file:
                existing_data = json.load(json_file)
                if not isinstance(existing_data, list):
                    existing_data = [existing_data]
        except json.JSONDecodeError:
            existing_data = []
        file_created_date = datetime.fromtimestamp(os.path.getctime(file_path))
    else:
        existing_data = []
        with open(file_path, 'w') as f:
            json.dump([], f)
        file_created_date = datetime.fromtimestamp(os.path.getctime(file_path))

    file_last_modified_date = file_created_date

    if api_type == 'gold':
        new_date = extract_date_from_timestamp(data.get("data", {}))
        new_currency = data.get("data", {}).get("base_currency")

        for item in existing_data:
            existing_date = extract_date_from_timestamp(item.get("data", {}))
            existing_currency = item.get("data", {}).get("base_currency")

            if existing_date == new_date and existing_currency == new_currency:
                return file_path, file_created_date, file_last_modified_date

    elif api_type == 'btc':
        new_last_refreshed = data.get("Meta Data", {}).get("6. Last Refreshed", "")
        new_currency = data.get("Meta Data", {}).get("3. Digital Currency Name", "")

        for item in existing_data:
            existing_last_refreshed = item.get("Meta Data", {}).get("6. Last Refreshed", "")
            existing_currency = item.get("Meta Data", {}).get("3. Digital Currency Name", "")

            if existing_last_refreshed == new_last_refreshed and existing_currency == new_currency:
                return file_path, file_created_date, file_last_modified_date

    existing_data.append(data)

    with open(file_path, 'w') as json_file:
        json.dump(existing_data, json_file, indent=4)

    file_last_modified_date = datetime.fromtimestamp(os.path.getmtime(file_path))

    return file_path, file_created_date, file_last_modified_date
