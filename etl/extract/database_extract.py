from datetime import datetime

from etl.commons.database import DBConnector
from etl.extract.logger_extract import logger


class DBConnectorExtract(DBConnector):
    def log_import(
        self,
        currency_id,
        import_directory_name,
        import_file_name,
        file_created_date,
        file_last_modified_date,
        row_count,
    ):
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

    def log_api_import(
        self,
        currency_id,
        api_id,
        start_time,
        end_time,
        code_response,
        error_messages=None,
    ):
        logger.debug(
            f"Logging API import for currency_id: {currency_id}, api_id: {api_id}"
        )
        query = """
        INSERT INTO extract.api_import_log (currency_id, api_id, start_time, end_time, code_response, error_messages)
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
