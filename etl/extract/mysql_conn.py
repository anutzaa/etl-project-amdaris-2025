import mysql.connector
from datetime import datetime

from logger import logger


class MySQLConnector:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None
        logger.debug(f"MySQL connector initialized for database: {database}")

    def connect(self):
        try:
            if self.database:
                self.connection = mysql.connector.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                )
            else:
                self.connection = mysql.connector.connect(
                    host=self.host, user=self.user, password=self.password
                )
            self.cursor = self.connection.cursor()
            logger.info(f"Connected to MySQL database: {self.database}")
        except mysql.connector.Error as error:
            logger.info(f"Error connecting to MySQL: {error}")
            self.connection = None

    def is_connected(self):
        if self.connection and self.connection.is_connected():
            return True
        return False

    def disconnect(self):
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from MySQL database")

    def get_currencies(self):
        logger.debug("Fetching currencies from database")
        query = "SELECT Id, code FROM currency"
        self.cursor.execute(query)
        currencies = self.cursor.fetchall()
        logger.debug(f"Found {len(currencies)} currencies")
        return currencies

    def get_currency_by_code(self, code):
        logger.debug(f"Looking up currency ID for code: {code}")
        query = "SELECT Id FROM currency WHERE code = %s"
        self.cursor.execute(query, (code,))
        result = self.cursor.fetchone()

        if result:
            logger.debug(f"Found currency ID {result[0]} for code {code}")
            return int(result[0])
        logger.warning(f"No currency found for code: {code}")
        return None

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
        INSERT INTO import_log (batch_date, currency_id, import_directory_name, import_file_name, 
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
        self.connection.commit()
        logger.debug("Import logged successfully")

    def log_api_import(
        self,
        currency_id,
        api_id,
        start_time,
        code_response,
        error_messages=None,
    ):
        logger.debug(
            f"Logging API import for currency_id: {currency_id}, api_id: {api_id}"
        )
        query = """
        INSERT INTO api_import_log (currency_id, api_id, start_time, code_response, error_messages)
        VALUES (%s, %s, %s, %s, %s)
        """
        values = (
            currency_id,
            api_id,
            start_time,
            code_response,
            error_messages,
        )
        self.cursor.execute(query, values)
        self.connection.commit()
        logger.debug("API import logged successfully")

    def log_api_import_end(self, currency_id, api_id, start_time, end_time):
        logger.debug(
            f"Updating API import end time for currency_id: {currency_id}, api_id: {api_id}"
        )
        query = """
        UPDATE api_import_log
        SET end_time = %s
        WHERE currency_id = %s AND api_id = %s AND start_time = %s
        """
        values = (end_time, currency_id, api_id, start_time)
        self.cursor.execute(query, values)
        rows_affected = self.cursor.rowcount
        self.connection.commit()
        logger.debug(
            f"API import end time updated successfully. Rows affected: {rows_affected}"
        )
