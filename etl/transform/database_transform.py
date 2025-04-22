import re
from datetime import datetime

from etl.commons.database import DBConnector
from etl.transform.logger_transform import logger


class DBConnectorTransform(DBConnector):
    """
    Extends DBConnector to support data transformation-specific operations,
    including inserting Bitcoin and Gold data, logging, and table management.

    Methods:
        upsert_btc_data()        -- Inserts or updates Bitcoin data
        upsert_gold_data()       -- Inserts or updates Gold data with dynamic rate columns
        log_transform()          -- Logs file transformation status
        truncate_import_tables() -- Clears import tables for a fresh load
        check_rate_columns()     -- Ensures required currency rate columns exist

    Instance Variables:
        conn   -- Inherited MySQL connection object
        cursor -- Inherited DB cursor object
    """

    def upsert_btc_data(
        self, currency_id, date, open, high, low, close, volume
    ):
        """
        Insert or update Bitcoin data in the 'btc_data_import' table.

        This method performs an 'INSERT' operation if the record does not exist,
        or an 'UPDATE' if the record already exists based on the currency_id and date.

        Parameters:
            currency_id -- The ID of the currency in the database.
            date        -- The date of the Bitcoin data.
            open        -- The opening price of Bitcoin on the given date.
            high        -- The highest price of Bitcoin on the given date.
            low         -- The lowest price of Bitcoin on the given date.
            close       -- The closing price of Bitcoin on the given date.
            volume      -- The trading volume of Bitcoin on the given date.

        Returns:
            bool -- True if the upsert operation was successful, False otherwise.
        """
        try:
            logger.debug(
                f"Upserting BTC data for currency_id {currency_id}, date {date}"
            )
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
                logger.debug(
                    f"Inserted new record for currency_id {currency_id}, date {date}"
                )
            elif self.cursor.rowcount == 2:
                logger.debug(
                    f"Updated record for currency_id {currency_id}, date {date}"
                )
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
        rate_data,
    ):
        """
        Insert or update Gold data in the 'gold_data_import' table.

        This method performs an 'INSERT' operation if the record does not exist,
        or an 'UPDATE' if the record already exists based on the currency_id and date.
        It also supports adding new rate columns if the provided rate data includes new
        currencies.

        Parameters:
            currency_id -- The ID of the currency in the database.
            date        -- The date of the Gold data.
            open_price  -- The opening price of Gold on the given date.
            high_price  -- The highest price of Gold on the given date.
            low_price   -- The lowest price of Gold on the given date.
            price       -- The price of Gold on the given date.
            price_24k   -- The 24k Gold price on the given date.
            price_18k   -- The 18k Gold price on the given date.
            price_14k   -- The 14k Gold price on the given date.
            rate_data   -- A dictionary containing rate data for different currencies.

        Returns:
            bool -- True if the upsert operation was successful, False otherwise.
        """
        try:
            logger.debug(
                f"Upserting gold data for currency_id {currency_id}, date {date}"
            )

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
                logger.debug(
                    f"Inserted new record for currency_id {currency_id}, date {date}"
                )
            elif self.cursor.rowcount == 2:
                logger.debug(
                    f"Updated record for currency_id {currency_id}, date {date}"
                )

            return True
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error in upsert_gold_data: {str(e)}", exc_info=True)
            return False

    def log_transform(
        self,
        currency_id,
        processed_directory_name,
        processed_file_name,
        row_count,
        status,
    ):
        """
        Logs the details of the data transformation process into the 'transform_log' table.

        This method logs the currency ID, processed file information, number of rows
        processed, and the transformation status (success or failure).

        Parameters:
            currency_id              -- The ID of the currency in the database.
            processed_directory_name -- The directory where the processed file is stored.
            processed_file_name      -- The name of the processed file.
            row_count                -- The number of rows processed from the file.
            status                   -- The status of the transformation (either 'success' or 'error').

        Returns:
            bool -- True if the log operation was successful, False otherwise.
        """
        try:
            logger.debug(
                f"Logging transform: currency_id={currency_id}, file={processed_file_name}, rows={row_count}, status={status}"
            )
            query = """
                  INSERT INTO transform.transform_log (batch_date, currency_id, processed_directory_name ,processed_file_name, row_count, status)
                  VALUES (%s, %s, %s, %s, %s, %s)
                  """
            values = (
                datetime.today(),
                currency_id,
                processed_directory_name,
                processed_file_name,
                row_count,
                status,
            )
            self.cursor.execute(query, values)
            self.conn.commit()
            logger.info(f"Transform log entry created successfully")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error in log_transform: {str(e)}", exc_info=True)
            return False

    def truncate_import_tables(self):
        """
        Clears all records from the import tables ('btc_data_import' and 'gold_data_import').

        This method executes 'TRUNCATE' commands on the import tables to clear existing
        records before new data is inserted.

        Returns:
            bool -- True if the truncate operation was successful, False otherwise.
        """
        tables = ["transform.btc_data_import", "transform.gold_data_import"]
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
            logger.error(
                f"Error truncating import tables: {str(e)}", exc_info=True
            )
            return False

    def check_rate_columns(self, rate_data):
        """
        Ensures that columns for currency exchange rates (like 'rate_usd', 'rate_eur')
        exist in the 'gold_data_import' table and adds them if necessary.

        Parameters:
            rate_data -- A dictionary containing rate data for different currencies.

        Returns:
            bool -- True if rate columns were successfully verified/added, False if there was an error.
        """
        logger.debug(
            f"Ensuring rate columns exist for currencies: {', '.join(rate_data.keys())}"
        )

        try:
            columns_added = 0
            columns_already_exist = 0

            for currency_code in rate_data.keys():
                if not re.match(r"^[A-Z]{3}$", currency_code):
                    raise ValueError(f"Invalid currency code: {currency_code}")

                column_name = f"rate_{currency_code.lower()}"

                self.cursor.execute(
                    f"""
                    SELECT COUNT(*) 
                    FROM information_schema.columns 
                    WHERE table_schema = 'transform' 
                    AND table_name = 'gold_data_import' 
                    AND column_name = '{column_name}'
                """
                )

                if self.cursor.fetchone()[0] == 0:
                    logger.info(
                        f"Adding column {column_name} to gold_data_import table"
                    )
                    self.cursor.execute(
                        f"""
                        ALTER TABLE transform.gold_data_import 
                        ADD COLUMN {column_name} DECIMAL(18,6) NULL
                    """
                    )
                    self.conn.commit()
                    columns_added += 1
                else:
                    columns_already_exist += 1

            if columns_added > 0:
                logger.info(
                    f"Rate columns summary: {columns_added} columns added, {columns_already_exist} columns already existed"
                )

            return True

        except Exception as e:
            self.conn.rollback()
            logger.error(
                f"Error ensuring rate columns exist: {str(e)}", exc_info=True
            )
            return False

    def get_files_to_process(self, data_type):
        """
        Retrieves files to process from the import_log table based on data type,
        excluding files that have already been processed (exist in transform_log).

        Parameters:
            data_type -- Type of data ('gold' or 'bitcoin')

        Returns:
            list -- List of dictionaries containing file information
        """
        try:
            logger.info(f"Retrieving {data_type} files to process from import_log (excluding already processed files)")

            directory_pattern = f"%{data_type}%"

            query = """
        SELECT 
            il.Id, 
            il.batch_date, 
            il.currency_id, 
            il.import_directory_name, 
            il.import_file_name,
            il.file_created_date,
            il.file_last_modified_date,
            il.row_count
        FROM 
            extract.import_log il
        LEFT JOIN 
            transform.transform_log tl ON il.import_file_name = tl.processed_file_name
        WHERE 
            il.import_directory_name LIKE %s
            AND tl.Id IS NULL
        ORDER BY 
            il.batch_date DESC
        """

            self.cursor.execute(query, (directory_pattern,))
            results = self.cursor.fetchall()

            files_to_process = []
            for row in results:
                files_to_process.append({
                    'id': row[0],
                    'batch_date': row[1],
                    'currency_id': row[2],
                    'directory': row[3],
                    'filename': row[4],
                    'created_date': row[5],
                    'modified_date': row[6],
                    'row_count': row[7],
                    'full_path': f"{row[3]}\\{row[4]}"
                })

            logger.info(f"Found {len(files_to_process)} new {data_type} files to process")
            return files_to_process

        except Exception as e:
            logger.error(f"Error retrieving files to process: {str(e)}", exc_info=True)
            return []

