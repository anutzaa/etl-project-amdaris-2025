import os
import shutil
import logging


def move_file(status, data_type, file_path):
    """
    Moves the file to the appropriate directory based on processing status.
    """
    if status not in ["processed", "error"]:
        return False

    dest_dir = f"../../data/{status}/{data_type}"
    os.makedirs(dest_dir, exist_ok=True)

    dest_file_path = os.path.join(dest_dir, os.path.basename(file_path))

    try:
        shutil.move(file_path, dest_file_path)
        return True
    except Exception as e:
        return False
