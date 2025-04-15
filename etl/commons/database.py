import os

import mysql.connector


class DBConnector:
    def __init__(self, logger):
        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT")
        self.database = os.getenv("DB_DATABASE")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.conn = None
        self.cursor = None
        self.logger = logger
        logger.debug(f"MySQL connector initialized for database: {self.database}")

    def connect(self):
        try:
            if self.database:
                self.conn = mysql.connector.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                )
            else:
                self.conn = mysql.connector.connect(
                    host=self.host, user=self.user, password=self.password
                )
            self.cursor = self.conn.cursor()
            self.logger.info(f"Connected to MySQL database: {self.database}")
        except mysql.connector.Error as error:
            self.logger.info(f"Error connecting to MySQL: {error}")
            self.conn = None

    def disconnect(self):
        if self.conn:
            self.conn.close()
            self.logger.info("Disconnected from MySQL database")

    def get_currencies(self):
        self.logger.debug("Fetching currencies from database")
        query = "SELECT Id, code FROM warehouse.dim_currency"
        self.cursor.execute(query)
        currencies = self.cursor.fetchall()
        self.logger.debug(f"Found {len(currencies)} currencies")
        return currencies

    def get_currency_by_code(self, code):
        self.logger.debug(f"Looking up currency ID for code: {code}")
        query = "SELECT Id FROM warehouse.dim_currency WHERE code = %s"
        self.cursor.execute(query, (code,))
        result = self.cursor.fetchone()

        if result:
            self.logger.debug(f"Found currency ID {result[0]} for code {code}")
            return int(result[0])
        self.logger.warning(f"No currency found for code: {code}")
        return None

    def get_rate_cols(self):
        self.logger.debug("Retrieving rate columns from gold_data_import")
        try:
            column_query = """
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = 'transform' 
                        AND table_name = 'gold_data_import' 
                        AND column_name LIKE 'rate_%'
                        """
            self.cursor.execute(column_query)
            rate_columns = [row[0] for row in self.cursor.fetchall()]

            currency_codes = [column[5:].upper() for column in rate_columns]
            self.logger.debug(f"Found rate columns for currencies: {', '.join(currency_codes)}")

            return currency_codes
        except Exception as e:
            self.logger.error(f"Error retrieving rate columns: {str(e)}", exc_info=True)
            return []
