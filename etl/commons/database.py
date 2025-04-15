import os

import mysql.connector


class DBConnector:
    """
       Handles MySQL database connection and currency-related operations.

       Methods:
           __init__()             -- Initializes with a logger instance
           connect()              -- Establish connection to the database
           disconnect()           -- Close the database connection
           get_currencies()       -- Fetch all currencies from the dim_currency table
           get_currency_by_code() -- Get currency ID by its code
           get_rate_cols()        -- Retrieve currency rate columns from gold_data_import

       Instance Variables:
           conn    -- Active MySQL connection
           cursor  -- Database cursor
           logger  -- Logger instance
       """
    def __init__(self, logger):
        """
        Initialize DBConnector with environment-based DB credentials and logger.

        Parameters:
           logger -- Logger instance of the current app
        """
        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT")
        self.database = os.getenv("DB_DATABASE")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.conn = None
        self.cursor = None
        self.logger = logger
        logger.debug(f"MySQL connector initialized for database: {self.database}")

        if not all([self.host, self.port, self.database, self.user, self.password]):
            self.logger.error("Missing required environment variables. Database connection cannot be established.")
            raise ValueError("Missing required environment variables")

    def connect(self):
        """
        Establish connection to MySQL database.

        Returns:
            None
        """
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
        """
        Close the database connection and cursor.

        Returns:
            None
        """
        if self.conn:
            self.cursor.close()
            self.conn.close()
            self.logger.info("Disconnected from MySQL database")

    def get_currencies(self):
        """
        Return a list of (id, code) for all currencies in dim_currency.

        Returns:
            list -- A list of tuples containing currency IDs and codes
        """
        self.logger.debug("Fetching currencies from database")
        query = "SELECT Id, code FROM warehouse.dim_currency"
        try:
            self.cursor.execute(query)
            currencies = self.cursor.fetchall()
            self.logger.debug(f"Found {len(currencies)} currencies")
            return currencies
        except mysql.connector.Error as e:
            self.logger.error(f"Error fetching currencies: {e}")
            return []

    def get_currency_by_code(self, code):
        """
        Return the ID of a currency given its code.

        Parameters:
            code -- 3-letter currency code (e.g., 'USD')

        Returns:
            int or None -- Currency ID if found, otherwise None
        """
        self.logger.debug(f"Looking up currency ID for code: {code}")
        query = "SELECT Id FROM warehouse.dim_currency WHERE code = %s"
        try:
            self.cursor.execute(query, (code,))
            result = self.cursor.fetchone()

            if result:
                self.logger.debug(f"Found currency ID {result[0]} for code {code}")
                return int(result[0])
            self.logger.warning(f"No currency found for code: {code}")
            return None
        except mysql.connector.Error as e:
            self.logger.error(f"Error retrieving currency by code: {e}")
            return None

    def get_rate_cols(self):
        """
        Return a list of currency codes from rate columns in gold_data_import.

        Returns:
            list -- A list of currency codes (e.g., ['USD', 'EUR'])
        """
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
