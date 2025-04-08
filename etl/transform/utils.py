import json
import os
import shutil
from logger import logger


def move_file(status, data_type, file_path):
    """
    Move a file to either the processed or error directory using shutil.
    """
    logger.info(f"Moving file {file_path} to {status} directory for {data_type}")
    try:
        base_dir = os.path.normpath("../../data")
        logger.debug(f"Base directory: {base_dir}")

        target_dir = os.path.join(base_dir, status, data_type)
        logger.debug(f"Target directory: {target_dir}")

        os.makedirs(target_dir, exist_ok=True)
        logger.debug(f"Ensured target directory exists: {target_dir}")

        filename = os.path.basename(file_path)
        new_file_path = os.path.join(target_dir, filename)

        logger.debug(f"Moving from {file_path} to {new_file_path}")
        shutil.move(file_path, new_file_path)

        logger.info(f"Successfully moved file from {file_path} to {new_file_path}")
        return new_file_path

    except Exception as e:
        logger.error(f"Error moving file {file_path}: {str(e)}", exc_info=True)
        return file_path


def process_file(data_type, directory, transform_func):
    logger.info(f"Starting {data_type} data Transform process")

    if not os.path.exists(directory):
        logger.warning(f"Directory not found: {directory}")
        return

    files = [f for f in os.listdir(directory) if f.endswith(".json")]
    logger.info(f"Found {len(files)} JSON files to process")

    for file in files:
        file_path = os.path.join(directory, file)
        transform_func(file_path)

    logger.info(f"{data_type} data Transform process completed")


def load_json_file(file_path):
    try:
        with open(file_path, 'r') as f:
            file_content = f.read()
            logger.debug("File content loaded")

            if file_content.strip().startswith('['):
                data_list = json.loads(file_content)
                logger.debug(f"Loaded JSON array with {len(data_list)} items")
            else:
                data_list = [json.loads(file_content)]
                logger.debug("Loaded single JSON object")

            return data_list
    except Exception as e:
        logger.error(f"Error loading JSON file {file_path}: {str(e)}")
        return None
