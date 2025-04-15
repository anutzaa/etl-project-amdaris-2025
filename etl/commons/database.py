import mysql.connector


class DBConnector:
    def __init__(self, host, port, database, user, password, logger):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None
        self.logger = logger
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
            self.logger.info(f"Connected to MySQL database: {self.database}")
        except mysql.connector.Error as error:
            self.logger.info(f"Error connecting to MySQL: {error}")
            self.connection = None

    def disconnect(self):
        if self.connection:
            self.connection.close()
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
