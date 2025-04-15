from etl.commons.database import DBConnector
from etl.load.logger import logger


class DBConnectorLoad(DBConnector):
    def upsert_fact_btc(self):
        logger.info("Starting upsert for fact_btc")
        try:
            query = """
                INSERT INTO warehouse.fact_btc (
                    date, currency_id, open, high, low, close, volume, created_at, updated_at
                )
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
                FROM transform.btc_data_import imp
                WHERE NOT EXISTS (
                    SELECT 1 FROM warehouse.fact_btc fact
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

            self.cursor.execute(query)
            rows = self.cursor.rowcount
            logger.info(f"Upserted {rows} rows into fact_btc")
            return True

        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error upserting fact_btc: {str(e)}", exc_info=True)
            return False

    def upsert_fact_gold(self):
        logger.info("Starting upsert for fact_gold")
        try:
            insert_query = """
                INSERT INTO warehouse.fact_gold (
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
                FROM transform.gold_data_import imp
                WHERE NOT EXISTS (
                    SELECT 1 FROM warehouse.fact_gold fact
                    WHERE fact.currency_id = imp.currency_id
                      AND fact.date = imp.date
                      AND MD5(CONCAT_WS(',', 
                          fact.open, fact.high, fact.low, fact.price, 
                          fact.price_24k, fact.price_18k, fact.price_14k
                      )) = MD5(CONCAT_WS(',', 
                          imp.open, imp.high, imp.low, imp.price, 
                          imp.price_24k, imp.price_18k, imp.price_14k
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

            self.cursor.execute(insert_query)
            rows = self.cursor.rowcount
            logger.info(f"Upserted {rows} rows into fact_gold")
            return True

        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error upserting fact_gold: {str(e)}", exc_info=True)
            return False

    def upsert_exchange_rates(self, currency_code):
        logger.info(f"Starting upsert for exchange rates: {currency_code}")
        try:
            currency_id = self.get_currency_by_code(currency_code)
            if not currency_id:
                logger.warning(f"No currency_id found for {currency_code}, skipping")
                return False

            insert_query = f"""
                 INSERT INTO warehouse.fact_exchange_rates (
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
                     NOW(4),
                     NOW(4)
                 FROM transform.gold_data_import imp
                 WHERE imp.rate_{currency_code} IS NOT NULL
                   AND imp.currency_id != %s
                   AND NOT EXISTS (
                       SELECT 1 FROM warehouse.fact_exchange_rates fact
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
            rows = self.cursor.rowcount
            logger.info(f"Upserted {rows} rows for {currency_code}")
            return True

        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error upserting exchange rates for {currency_code}: {str(e)}", exc_info=True)
            return False

    def upsert_dim_date(self, source_table):
        logger.info(f"Starting upsert of dim_date from {source_table}")
        try:
            count_query = f"""
                SELECT COUNT(DISTINCT date)
                FROM {source_table}
                WHERE date NOT IN (SELECT date FROM warehouse.dim_date)
            """
            self.cursor.execute(count_query)
            date_count = self.cursor.fetchone()[0]

            if date_count == 0:
                logger.info("No new dates to load into dim_date")
                return 0

            logger.info(f"Found {date_count} new dates to load into dim_date")

            insert_query = f"""
                INSERT INTO warehouse.dim_date (
                    date, day, month, month_name, quarter, year,
                    day_of_week, week_of_year, is_weekend, created_at, updated_at
                )
                SELECT DISTINCT
                    date,
                    DAYOFMONTH(date),
                    MONTH(date),
                    MONTHNAME(date),
                    QUARTER(date),
                    YEAR(date),
                    DAYOFWEEK(date),
                    WEEKOFYEAR(date),
                    DAYOFWEEK(date) IN (1, 7),
                    NOW(4),
                    NOW(4)
                FROM {source_table}
                WHERE date NOT IN (SELECT date FROM warehouse.dim_date)
                ON DUPLICATE KEY UPDATE
                    day = VALUES(day),
                    month = VALUES(month),
                    month_name = VALUES(month_name),
                    quarter = VALUES(quarter),
                    year = VALUES(year),
                    day_of_week = VALUES(day_of_week),
                    week_of_year = VALUES(week_of_year),
                    is_weekend = VALUES(is_weekend),
                    updated_at = NOW(4)
            """

            self.cursor.execute(insert_query)
            rows = self.cursor.rowcount
            self.connection.commit()
            logger.info(f"Upserted {rows} rows into dim_date")
            return True

        except Exception as e:
            self.connection.rollback()
            logger.error(f"Error upserting dim_date from {source_table}: {str(e)}", exc_info=True)
            return False
