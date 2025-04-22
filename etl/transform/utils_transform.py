import json
import os
import shutil

from etl.transform.logger_transform import logger


def move_file(status, data_type, file_path):
    """
    Move a file to the corresponding status directory.

    Parameters:
        status    -- 'processed' or 'error'.
        data_type -- Type of data, e.g., 'bitcoin' or 'gold'.
        file_path -- Original path of the file.

    Returns:
        str -- New file path after moving.
    """
    logger.info(
        f"Moving file {file_path} to {status} directory for {data_type}"
    )
    try:
        base_dir = os.path.normpath("data")
        logger.debug(f"Base directory: {base_dir}")

        target_dir = os.path.join(base_dir, status, data_type)
        logger.debug(f"Target directory: {target_dir}")

        os.makedirs(target_dir, exist_ok=True)
        logger.debug(f"Ensured target directory exists: {target_dir}")

        filename = os.path.basename(file_path)
        new_file_path = os.path.join(target_dir, filename)

        logger.debug(f"Moving from {file_path} to {new_file_path}")
        shutil.move(file_path, new_file_path)

        logger.info(
            f"Successfully moved file from {file_path} to {new_file_path}"
        )
        return new_file_path

    except Exception as e:
        logger.error(f"Error moving file {file_path}: {str(e)}", exc_info=True)
        return file_path


def process_file(data_type, directory, transform_func):
    """
    Process all JSON files in the given directory using a transform function.

    Parameters:
        data_type      -- Type of data being processed.
        directory      -- Directory containing JSON files.
        transform_func -- Function to apply on each file.

    Returns:
        None
    """
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
    """
    Load a JSON file and return its content as a list of objects.

    Parameters:
        file_path -- Path to the JSON file.

    Returns:
        list or None -- List of JSON objects, or None on failure.
    """
    try:
        with open(file_path, "r") as f:
            file_content = f.read()
            logger.debug("File content loaded")

            if file_content.strip().startswith("["):
                data_list = json.loads(file_content)
                logger.debug(f"Loaded JSON array with {len(data_list)} items")
            else:
                data_list = [json.loads(file_content)]
                logger.debug("Loaded single JSON object")

            return data_list
    except Exception as e:
        logger.error(f"Error loading JSON file {file_path}: {str(e)}")
        return None
