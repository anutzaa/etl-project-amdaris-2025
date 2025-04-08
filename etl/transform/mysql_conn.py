from datetime import datetime

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

    def upsert_btc_data(self, currency_id, date, open, high, low, close, volume):
        try:
            upsert_query = """
            INSERT INTO btc_data_import (currency_id, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                close = VALUES(close),
                volume = VALUES(volume)
            """
            self.cursor.execute(upsert_query, (
                currency_id, date, open, high, low, close, volume
            ))

            self.connection.commit()

            if self.cursor.rowcount == 1:
                print(f"Inserted new record for currency_id {currency_id}, date {date}")
            elif self.cursor.rowcount == 2:  
                print(f"Updated record for currency_id {currency_id}, date {date}")
            return True
        
        except Exception as e:
            self.connection.rollback()
            print(f"Error in upsert_btc_data: {str(e)}")
            return False

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
            datetime.today(),
            currency_id,
            processed_directory_name,
            processed_file_name,
            row_count,
            status
        )
        self.cursor.execute(query, values)
        self.connection.commit()
