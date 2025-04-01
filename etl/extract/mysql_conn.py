import mysql.connector


class MySQLConnector:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None

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
