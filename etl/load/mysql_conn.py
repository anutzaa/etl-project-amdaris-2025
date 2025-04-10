from etl.extract.mysql_conn import MySQLConnector


class MySQLConnectorLoad(MySQLConnector):
    def load_fact_btc(self):
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
            self.connection.commit()
            print("Data loaded into fact_btc from staging table")
        except Exception as e:
            self.connection.rollback()
            print(f"Error loading fact_btc: {str(e)}")

    def load_fact_gold(self):
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
            self.connection.commit()
            print("Data loaded into fact_gold from staging table")
        except Exception as e:
            self.connection.rollback()
            print(f"Error loading fact_btc: {str(e)}")

    def load_fact_exchange_rates(self):
        query = """
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
                (SELECT Id FROM currency WHERE code = 'USD') AS target_currency_id,
                imp.rate_usd AS rate,
                NOW(4) AS created_at,
                NOW(4) AS updated_at
            FROM gold_data_import imp
            WHERE imp.rate_usd IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM fact_exchange_rates fact
                WHERE fact.date = imp.date
                  AND fact.base_currency_id = imp.currency_id
                  AND fact.target_currency_id = (SELECT Id FROM currency WHERE code = 'USD')
                  AND fact.rate = imp.rate_usd
            )
            
            UNION ALL
            
            SELECT 
                imp.date,
                imp.currency_id AS base_currency_id,
                (SELECT Id FROM currency WHERE code = 'EUR') AS target_currency_id,
                imp.rate_eur AS rate,
                NOW(4) AS created_at,
                NOW(4) AS updated_at
            FROM gold_data_import imp
            WHERE imp.rate_eur IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM fact_exchange_rates fact
                WHERE fact.date = imp.date
                  AND fact.base_currency_id = imp.currency_id
                  AND fact.target_currency_id = (SELECT Id FROM currency WHERE code = 'EUR')
                  AND fact.rate = imp.rate_eur
            )
            
            UNION ALL
            
            SELECT 
                imp.date,
                imp.currency_id AS base_currency_id,
                (SELECT Id FROM currency WHERE code = 'GBP') AS target_currency_id,
                imp.rate_gbp AS rate,
                NOW(4) AS created_at,
                NOW(4) AS updated_at
            FROM gold_data_import imp
            WHERE imp.rate_gbp IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM fact_exchange_rates fact
                WHERE fact.date = imp.date
                  AND fact.base_currency_id = imp.currency_id
                  AND fact.target_currency_id = (SELECT Id FROM currency WHERE code = 'GBP')
                  AND fact.rate = imp.rate_gbp
            )
            
            ON DUPLICATE KEY UPDATE
                rate = VALUES(rate),
                updated_at = NOW(4)
                        """
        try:
            self.cursor.execute(query)
            self.connection.commit()
            print("Data loaded into fact_exchange_rates from staging table")
        except Exception as e:
            self.connection.rollback()
            print(f"Error loading fact_btc: {str(e)}")

    def load_dim_date(self):
        print("Starting load of dim_date dimension table")

        try:
            cursor = self.connection.cursor()

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

            print(f"Successfully loaded {rows_affected} dates into dim_date")
            return True

        except Exception as e:
            self.connection.rollback()
            print(f"Error loading dim_date: {str(e)}")
            return False
