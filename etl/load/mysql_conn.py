from etl.extract.mysql_conn import MySQLConnector
from logger import logger


class MySQLConnectorLoad(MySQLConnector):
    def load_fact_btc(self):
        logger.info("Loading data into fact_btc from staging table")

        query = """
             INSERT INTO fact_btc (date, currency_id, open, high, low, close, volume, created_at, updated_at)
                SELECT 
                    imp.date, 
                    imp.currency_id, 
                    imp.open, 
                    imp.high, 
                    imp.low, 
                    imp.close, 
                    imp.volume,
                    NOW(4), 
                    NOW(4)  
                FROM btc_data_import imp
                WHERE NOT EXISTS (
                    SELECT 1 FROM fact_btc fact
                    WHERE fact.currency_id = imp.currency_id
                      AND fact.date = imp.date
                      AND MD5(CONCAT_WS(',', fact.open, fact.high, fact.low, fact.close, fact.volume)) =
                          MD5(CONCAT_WS(',', imp.open, imp.high, imp.low, imp.close, imp.volume))
                )
                ON DUPLICATE KEY UPDATE
                    open = VALUES(open),
                    high = VALUES(high),
                    low = VALUES(low),
                    close = VALUES(close),
                    volume = VALUES(volume),
                    updated_at = NOW(4)
                """
        try:
            self.cursor.execute(query)
            rows_affected = self.cursor.rowcount
            self.connection.commit()
            logger.info(f"Data loaded into fact_btc: {rows_affected} rows affected")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error loading fact_btc: {str(e)}", exc_info=True)

    def load_fact_gold(self):
        logger.info("Loading data into fact_gold from staging table")

        query = """
                INSERT INTO fact_gold (
                    currency_id, 
                    date, 
                    open, 
                    high, 
                    low, 
                    price, 
                    price_24k, 
                    price_18k, 
                    price_14k, 
                    created_at, 
                    updated_at
                )
                SELECT 
                    imp.currency_id, 
                    imp.date, 
                    imp.open, 
                    imp.high, 
                    imp.low, 
                    imp.price, 
                    imp.price_24k, 
                    imp.price_18k, 
                    imp.price_14k,
                    NOW(4),
                    NOW(4)
                FROM gold_data_import imp
                WHERE NOT EXISTS (
                    SELECT 1 FROM fact_gold fact
                    WHERE fact.currency_id = imp.currency_id
                      AND fact.date = imp.date
                      AND MD5(CONCAT_WS(',', 
                          fact.open, 
                          fact.high, 
                          fact.low, 
                          fact.price, 
                          fact.price_24k, 
                          fact.price_18k, 
                          fact.price_14k
                      )) = MD5(CONCAT_WS(',', 
                          imp.open, 
                          imp.high, 
                          imp.low, 
                          imp.price, 
                          imp.price_24k, 
                          imp.price_18k, 
                          imp.price_14k
                      ))
                )
                ON DUPLICATE KEY UPDATE
                    open = VALUES(open),
                    high = VALUES(high),
                    low = VALUES(low),
                    price = VALUES(price),
                    price_24k = VALUES(price_24k),
                    price_18k = VALUES(price_18k),
                    price_14k = VALUES(price_14k),
                    updated_at = NOW(4)
                """
        try:
            self.cursor.execute(query)
            rows_affected = self.cursor.rowcount
            self.connection.commit()
            logger.info(f"Data loaded into fact_gold: {rows_affected} rows affected")
        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error loading fact_gold: {str(e)}", exc_info=True)

    def get_rate_cols(self):
        logger.debug("Retrieving rate columns from gold_data_import")
        try:
            column_query = """
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = DATABASE() 
                        AND table_name = 'gold_data_import' 
                        AND column_name LIKE 'rate_%'
                        """
            self.cursor.execute(column_query)
            rate_columns = [row[0] for row in self.cursor.fetchall()]

            currency_codes = [column[5:].upper() for column in rate_columns]
            logger.debug(f"Found rate columns for currencies: {', '.join(currency_codes)}")

            return currency_codes
        except Exception as e:
            logger.error(f"Error retrieving rate columns: {str(e)}", exc_info=True)
            return []

    def load_fact_exchange_rates(self):
        logger.info("Loading data into fact_exchange_rates from staging table")

        try:
            currency_codes = self.get_rate_cols()

            total_rows_affected = 0

            for currency_code in currency_codes:
                logger.debug(f"Processing exchange rates for currency: {currency_code}")
                currency_id = self.get_currency_by_code(currency_code)

                if not currency_id:
                    logger.warning(f"Currency {currency_code} not found in currency table, skipping")
                    continue

                insert_query = f"""
                INSERT INTO fact_exchange_rates (
                    date,
                    base_currency_id,
                    target_currency_id,
                    rate,
                    created_at,
                    updated_at
                )
                SELECT 
                    imp.date,
                    imp.currency_id AS base_currency_id,
                    %s AS target_currency_id,
                    imp.rate_{currency_code} AS rate,
                    NOW(4) AS created_at,
                    NOW(4) AS updated_at
                FROM gold_data_import imp
                WHERE imp.rate_{currency_code} IS NOT NULL
                AND imp.currency_id != %s
                AND NOT EXISTS (
                    SELECT 1 FROM fact_exchange_rates fact
                    WHERE fact.date = imp.date
                      AND fact.base_currency_id = imp.currency_id
                      AND fact.target_currency_id = %s
                      AND fact.rate = imp.rate_{currency_code}
                )
                ON DUPLICATE KEY UPDATE
                    rate = VALUES(rate),
                    updated_at = NOW(4)
                """

                self.cursor.execute(insert_query, (currency_id, currency_id, currency_id))
                rows_affected = self.cursor.rowcount
                total_rows_affected += rows_affected

                logger.info(f"Loaded {rows_affected} {currency_code} exchange rates")

            self.connection.commit()
            logger.info(f"Total data loaded into fact_exchange_rates: {total_rows_affected} rows affected")
            return total_rows_affected

        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error loading fact_exchange_rates: {str(e)}", exc_info=True)
            return 0

    def load_dim_date(self):
        logger.info("Starting load of dim_date dimension table")

        try:
            cursor = self.connection.cursor()

            count_query = """
            SELECT COUNT(DISTINCT date) 
            FROM (
                SELECT date FROM btc_data_import
                UNION
                SELECT date FROM gold_data_import
            ) AS all_dates
            WHERE date NOT IN (SELECT date FROM dim_date)
            """
            cursor.execute(count_query)
            date_count = cursor.fetchone()[0]

            if date_count == 0:
                logger.info("No new dates to load into dim_date")
                return 0

            logger.info(f"Found {date_count} new dates to load into dim_date")

            insert_query = """
            INSERT INTO dim_date (
                date,
                day,
                month,
                month_name,
                quarter,
                year,
                day_of_week,
                week_of_year,
                is_weekend
            )
            SELECT DISTINCT
                date,
                DAYOFMONTH(date) AS day,
                MONTH(date) AS month,
                MONTHNAME(date) AS month_name,
                QUARTER(date) AS quarter,
                YEAR(date) AS year,
                DAYOFWEEK(date) AS day_of_week,
                WEEKOFYEAR(date) AS week_of_year,
                DAYOFWEEK(date) IN (1, 7) AS is_weekend
            FROM (
                SELECT date FROM btc_data_import
                UNION
                SELECT date FROM gold_data_import
            ) AS all_dates
            WHERE date NOT IN (SELECT date FROM dim_date)
            ON DUPLICATE KEY UPDATE
                day = VALUES(day),
                month = VALUES(month),
                month_name = VALUES(month_name),
                quarter = VALUES(quarter),
                year = VALUES(year),
                day_of_week = VALUES(day_of_week),
                week_of_year = VALUES(week_of_year),
                is_weekend = VALUES(is_weekend)
            """

            cursor.execute(insert_query)
            rows_affected = cursor.rowcount

            self.connection.commit()

            logger.info(f"Successfully loaded {rows_affected} dates into dim_date")
            return rows_affected

        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error loading dim_date: {str(e)}", exc_info=True)
            return 0
