import os
from datetime import datetime
import json


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
    else:
        existing_data = []

    existing_data.append(data)

    with open(file_path, 'w') as json_file:
        json.dump(existing_data, json_file, indent=4)

    file_created_date = datetime.fromtimestamp(os.path.getctime(file_path))
    file_last_modified_date = datetime.fromtimestamp(os.path.getmtime(file_path))

    return file_path, file_created_date, file_last_modified_date
