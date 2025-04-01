import os
from datetime import datetime
import json


def save_to_file(data, api_type):
    base_dir = '../../data/raw/'

    output_dir = os.path.join(base_dir, 'bitcoin' if api_type == 'btc' else 'gold')
    file_name = f'{api_type}_{datetime.today().strftime("%Y%m%d")}.json'

    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, file_name)

    timestamped_data = {
        "timestamp": datetime.now().isoformat(),
        "data": data
    }

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

    existing_data.append(timestamped_data)

    with open(file_path, 'w') as json_file:
        json.dump(existing_data, json_file, indent=4)

    return file_path
