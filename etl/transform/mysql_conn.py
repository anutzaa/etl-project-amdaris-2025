from etl.extract.mysql_conn import MySQLConnector


class MySQLConnectorTransform(MySQLConnector):
    def insert_btc_data(self, currency_id, date, open, high, low, close, volume):
        query = """
                INSERT INTO btc_data_import (currency_id, date, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
        values = (
            currency_id,
            date,
            open,
            high,
            low,
            close,
            volume
        )
        self.cursor.execute(query, values)
        self.connection.commit()

    def insert_gold_data(self, currency_id, date, open, high, low, price, price_24k, price_18k, price_14k):
        query = """
               INSERT INTO gold_data_import (currency_id, date, open, high, low, price, price_24k, price_18k, price_14k)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               """
        values = (
            currency_id,
            date,
            open,
            high,
            low,
            price,
            price_24k,
            price_18k,
            price_14k
        )
        self.cursor.execute(query, values)
        self.connection.commit()

    def log_transform(self,
                      batch_date,
                      currency_id,
                      processed_directory_name,
                      processed_file_name,
                      row_count,
                      status):
        query = """
              INSERT INTO transform_log (batch_date, currency_id, processed_directory_name ,processed_file_name, row_count, status)
              VALUES (%s, %s, %s, %s, %s, %s)
              """
        values = (
            batch_date,
            currency_id,
            processed_directory_name,
            processed_file_name,
            row_count,
            status
        )
        self.cursor.execute(query, values)
        self.connection.commit()
