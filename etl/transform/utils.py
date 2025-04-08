import os
import shutil
import logging


def move_file(status, data_type, file_path):
    """
    Move a file to either the processed or error directory.
    """

    base_dir = os.path.normpath("../../data")

    target_dir = os.path.join(base_dir, status, data_type)
    os.makedirs(target_dir, exist_ok=True)

    filename = os.path.basename(file_path)

    new_file_path = os.path.join(target_dir, filename)

    os.rename(file_path, new_file_path)

    print(f"Moved file from {file_path} to {new_file_path}")

    return new_file_path
