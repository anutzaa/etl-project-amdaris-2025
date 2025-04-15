from datetime import datetime

from etl.commons.database import DBConnector
from etl.transform.logger import logger


class DBConnectorTransform(DBConnector):
    def upsert_btc_data(self, currency_id, date, open, high, low, close, volume):
        try:
            logger.debug(f"Upserting BTC data for currency_id {currency_id}, date {date}")
            query = """
            INSERT INTO transform.btc_data_import (currency_id, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                close = VALUES(close),
                volume = VALUES(volume)
            """
            values = (currency_id, date, open, high, low, close, volume)

            self.cursor.execute(query, values)
            self.connection.commit()

            if self.cursor.rowcount == 1:
                logger.info(f"Inserted new record for currency_id {currency_id}, date {date}")
            elif self.cursor.rowcount == 2:
                logger.info(f"Updated record for currency_id {currency_id}, date {date}")
            return True

        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error in upsert_btc_data: {str(e)}", exc_info=True)
            return False

    def upsert_gold_data(
        self,
        currency_id,
        date,
        open_price,
        high_price,
        low_price,
        price,
        price_24k,
        price_18k,
        price_14k,
        rate_usd,
        rate_eur,
        rate_gbp,
    ):
        try:
            logger.debug(f"Upserting gold data for currency_id {currency_id}, date {date}")
            query = """
            INSERT INTO transform.gold_data_import (
                currency_id, date, open, high, low, price, 
                price_24k, price_18k, price_14k, 
                rate_USD, rate_EUR, rate_GBP
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                price = VALUES(price),
                price_24k = VALUES(price_24k),
                price_18k = VALUES(price_18k),
                price_14k = VALUES(price_14k),
                rate_USD = VALUES(rate_USD),
                rate_EUR = VALUES(rate_EUR),
                rate_GBP = VALUES(rate_GBP)
            """
            values = (
                currency_id,
                date,
                open_price,
                high_price,
                low_price,
                price,
                price_24k,
                price_18k,
                price_14k,
                rate_usd,
                rate_eur,
                rate_gbp,
            )
            self.cursor.execute(query, values)
            self.connection.commit()

            if self.cursor.rowcount == 1:
                logger.info(f"Inserted new record for currency_id {currency_id}, date {date}")
            elif self.cursor.rowcount == 2:
                logger.info(f"Updated record for currency_id {currency_id}, date {date}")

            return True
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error in upsert_gold_data: {str(e)}", exc_info=True)
            return False

    def log_transform(self, currency_id, processed_directory_name, processed_file_name, row_count, status):
        try:
            logger.debug(
                f"Logging transform: currency_id={currency_id}, file={processed_file_name}, rows={row_count}, status={status}"
            )
            query = """
                  INSERT INTO transform.transform_log (batch_date, currency_id, processed_directory_name ,processed_file_name, row_count, status)
                  VALUES (%s, %s, %s, %s, %s, %s)
                  """
            values = (datetime.today(), currency_id, processed_directory_name, processed_file_name, row_count, status)
            self.cursor.execute(query, values)
            self.connection.commit()
            logger.info(f"Transform log entry created successfully")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error in log_transform: {str(e)}", exc_info=True)
            return False

    def truncate_import_tables(self):
        tables = ['transform.btc_data_import', 'transform.gold_data_import']
        logger.info(f"Truncating import tables")

        try:
            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

            for table in tables:
                logger.debug(f"Truncating table: {table}")
                self.cursor.execute(f"TRUNCATE TABLE {table}")

            self.cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

            self.connection.commit()
            logger.info("All import tables truncated successfully")
            return True

        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error truncating import tables: {str(e)}", exc_info=True)
            return False
