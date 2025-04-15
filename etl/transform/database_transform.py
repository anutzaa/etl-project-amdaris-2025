import re
from datetime import datetime

from etl.commons.database import DBConnector
from etl.transform.logger_transform import logger


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
            self.conn.commit()

            if self.cursor.rowcount == 1:
                logger.debug(f"Inserted new record for currency_id {currency_id}, date {date}")
            elif self.cursor.rowcount == 2:
                logger.debug(f"Updated record for currency_id {currency_id}, date {date}")
            return True

        except Exception as e:
            self.conn.rollback()
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
            rate_data
    ):
        try:
            logger.debug(f"Upserting gold data for currency_id {currency_id}, date {date}")

            if rate_data is None:
                rate_data = {}

            if not self.check_rate_columns(rate_data):
                return False

            rate_columns = []
            rate_placeholders = []
            rate_values = []
            rate_updates = []

            for currency_code, rate_value in rate_data.items():
                column_name = f"rate_{currency_code.lower()}"
                rate_columns.append(column_name)
                rate_placeholders.append("%s")
                rate_values.append(rate_value)
                rate_updates.append(f"{column_name} = VALUES({column_name})")

            base_columns = "currency_id, date, open, high, low, price, price_24k, price_18k, price_14k"
            base_values = "%s, %s, %s, %s, %s, %s, %s, %s, %s"
            base_updates = """
                open = VALUES(open),
                high = VALUES(high),
                low = VALUES(low),
                price = VALUES(price),
                price_24k = VALUES(price_24k),
                price_18k = VALUES(price_18k),
                price_14k = VALUES(price_14k)
            """

            if rate_columns:
                columns = f"{base_columns}, {', '.join(rate_columns)}"
                placeholders = f"{base_values}, {', '.join(rate_placeholders)}"
                updates = f"{base_updates}, {', '.join(rate_updates)}"
            else:
                columns = base_columns
                placeholders = base_values
                updates = base_updates

            query = f"""
            INSERT INTO transform.gold_data_import ({columns})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE 
                {updates}
            """

            values = [
                         currency_id,
                         date,
                         open_price,
                         high_price,
                         low_price,
                         price,
                         price_24k,
                         price_18k,
                         price_14k,
                     ] + rate_values

            self.cursor.execute(query, values)
            self.conn.commit()

            if self.cursor.rowcount == 1:
                logger.debug(f"Inserted new record for currency_id {currency_id}, date {date}")
            elif self.cursor.rowcount == 2:
                logger.debug(f"Updated record for currency_id {currency_id}, date {date}")

            return True
        except Exception as e:
            self.conn.rollback()
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
            self.conn.commit()
            logger.info(f"Transform log entry created successfully")
        except Exception as e:
            self.conn.rollback()
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

            self.conn.commit()
            logger.info("All import tables truncated successfully")
            return True

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error truncating import tables: {str(e)}", exc_info=True)
            return False

    def check_rate_columns(self, rate_data):
        logger.debug(f"Ensuring rate columns exist for currencies: {', '.join(rate_data.keys())}")

        try:
            columns_added = 0
            columns_already_exist = 0

            for currency_code in rate_data.keys():
                if not re.match(r'^[A-Z]{3}$', currency_code):
                    raise ValueError(f"Invalid currency code: {currency_code}")

                column_name = f"rate_{currency_code.lower()}"

                self.cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM information_schema.columns 
                    WHERE table_schema = 'transform' 
                    AND table_name = 'gold_data_import' 
                    AND column_name = '{column_name}'
                """)

                if self.cursor.fetchone()[0] == 0:
                    logger.info(f"Adding column {column_name} to gold_data_import table")
                    self.cursor.execute(f"""
                        ALTER TABLE transform.gold_data_import 
                        ADD COLUMN {column_name} DECIMAL(18,6) NULL
                    """)
                    self.conn.commit()
                    columns_added += 1
                else:
                    columns_already_exist += 1

            if columns_added > 0:
                logger.info(
                    f"Rate columns summary: {columns_added} columns added, {columns_already_exist} columns already existed")

            return True

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error ensuring rate columns exist: {str(e)}", exc_info=True)
            return False
