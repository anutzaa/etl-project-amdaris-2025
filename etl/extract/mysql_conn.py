import mysql.connector
from datetime import datetime


class MySQLConnector:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            if self.database:
                self.connection = mysql.connector.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
            else:
                self.connection = mysql.connector.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password
                )
            self.cursor = self.connection.cursor()
            print("Connected to MySQL")
        except mysql.connector.Error as error:
            print("Error while connecting to MySQL", error)
            self.connection = None

    def is_connected(self):
        if self.connection and self.connection.is_connected():
            return True
        return False

    def disconnect(self):
        if self.connection:
            self.connection.close()
            print("Disconnected from MySQL")

    def get_currencies(self):
        query = "SELECT Id, code FROM currency"
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_currency_by_code(self, code):
        query = "SELECT Id FROM currency WHERE code = %s"
        self.cursor.execute(query, (code,))
        result = self.cursor.fetchone()

        if result:
            return int(result[0])
        return None

    def log_import(self, currency_id, import_directory_name, import_file_name,
                   file_created_date, file_last_modified_date, row_count):
        query = """
        INSERT INTO import_log (batch_date, currency_id, import_directory_name, import_file_name, 
        file_created_date, file_last_modified_date, row_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        values = (datetime.today(), currency_id, import_directory_name, import_file_name,
                  file_created_date, file_last_modified_date, row_count)
        self.cursor.execute(query, values)
        self.connection.commit()

    def log_api_import(self, currency_id, api_id, start_time, code_response, error_messages=None):
        query = """
        INSERT INTO api_import_log (currency_id, api_id, start_time, code_response, error_messages)
        VALUES (%s, %s, %s, %s, %s)
        """
        values = (currency_id, api_id, start_time, code_response, error_messages)
        self.cursor.execute(query, values)
        self.connection.commit()

    def log_api_import_end(self, currency_id, api_id, start_time, end_time):
        query = """
        UPDATE api_import_log
        SET end_time = %s
        WHERE currency_id = %s AND api_id = %s AND start_time = %s
        """
        values = (end_time, currency_id, api_id, start_time)
        self.cursor.execute(query, values)
        self.connection.commit()
