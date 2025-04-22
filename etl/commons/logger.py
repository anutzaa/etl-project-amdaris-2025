import logging
import os
from datetime import datetime


def setup_logger(app_name, base_dir="etl"):
    """
    Configure and return a logger for the app based on its name.

    Parameters:
        app_name -- Name of the app for which the logger is to be configured
        base_dir -- Base directory where logs will be stored (default is "etl")

    Returns:
        logger -- Configured logger instance for the app
    """
    log_dir = os.path.join(base_dir, app_name, "logs")
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(app_name)

    if not logger.handlers:
        logger.setLevel(logging.DEBUG)

        console_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)

        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
        )
        log_file = os.path.join(
            log_dir, f'{app_name}_{datetime.now().strftime("%Y%m%d")}.log'
        )
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger
