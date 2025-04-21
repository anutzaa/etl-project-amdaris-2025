from datetime import datetime

from etl.commons.database import DBConnector
from etl.extract.logger import logger


class DBConnectorExtract(DBConnector):
    """
    Extends DBConnector with methods specific to logging extract operations.

    Methods:
        log_import()      -- Insert metadata about file-based data imports
        log_api_import()  -- Insert metadata about API-based data imports
    """

    def log_import(
        self,
        currency_id,
        import_directory_name,
        import_file_name,
        file_created_date,
        file_last_modified_date,
        row_count,
    ):
        """
        Log file import data to the 'import_log' table.

        Parameters:
            currency_id             -- ID of the currency
            import_directory_name   -- Directory where the file is stored
            import_file_name        -- File name of the import
            file_created_date       -- Original file creation timestamp
            file_last_modified_date -- Last modified timestamp of the file
            row_count               -- Number of data rows imported

        Returns:
            None
        """
        try:
            logger.debug(
                f"Logging import for currency_id: {currency_id}, file: {import_file_name}, rows: {row_count}"
            )
            query = """
                    INSERT INTO extract.import_log (batch_date, currency_id, import_directory_name, import_file_name, 
                    file_created_date, file_last_modified_date, row_count)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                datetime.today(),
                currency_id,
                import_directory_name,
                import_file_name,
                file_created_date,
                file_last_modified_date,
                row_count,
            )
            self.cursor.execute(query, values)
            self.conn.commit()
            logger.debug("Import logged successfully")
        except Exception as e:
            logger.error(f"Failed to log import: {str(e)}", exc_info=True)

    def log_api_import(
        self,
        currency_id,
        api_id,
        start_time,
        end_time,
        code_response,
        error_messages=None,
    ):
        """
        Log API call metadata to the extract.api_import_log table.

        Parameters:
            currency_id    -- ID of the currency
            api_id         -- API source identifier (e.g., 'BTC', 'XAU')
            start_time     -- Start timestamp of the API request
            end_time       -- End timestamp of the API request
            code_response  -- HTTP response code from the API
            error_messages -- Optional error message from the API

        Returns:
            None
        """
        try:
            logger.debug(
                f"Logging API import for currency_id: {currency_id}, api_id: {api_id}"
            )
            query = """
                    INSERT INTO extract.api_import_log (currency_id, api_id, start_time, end_time, code_response, 
                    error_messages)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
            values = (
                currency_id,
                api_id,
                start_time,
                end_time,
                code_response,
                error_messages,
            )
            self.cursor.execute(query, values)
            self.conn.commit()
            logger.debug("API import logged successfully")
        except Exception as e:
            logger.error(f"Failed to log API import: {str(e)}", exc_info=True)
