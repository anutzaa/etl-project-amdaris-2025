from etl.load.database_load import DBConnectorLoad
from etl.load.logger_load import logger


class GoldLoad:
    def __init__(self, conn:DBConnectorLoad):
        self.conn = conn

    def load_dim_date(self):
        self.conn.upsert_dim_date('transform.gold_data_import')

    def load_fact_gold(self):
        logger.info("Loading data into fact_gold from staging table")
        try:
            rows = self.conn.upsert_fact_gold()
            self.conn.conn.commit()
            logger.info(f"Total data loaded into fact_gold: {rows} rows affected")
            return True

        except Exception as e:
            self.conn.conn.rollback()
            logger.error(f"Error loading fact_gold: {str(e)}", exc_info=True)
            return False

    def load_fact_exchange_rates(self):
        logger.info("Loading data into fact_exchange_rates from staging table")
        try:
            currency_codes = self.conn.get_rate_cols()
            total_rows_affected = 0

            for currency_code in currency_codes:
                logger.debug(f"Processing exchange rates for currency: {currency_code}")
                rows = self.conn.upsert_exchange_rates(currency_code)
                if rows:
                    total_rows_affected += rows

            self.conn.conn.commit()
            logger.info(f"Total data loaded into fact_exchange_rates: {total_rows_affected} rows affected")
            return True

        except Exception as e:
            self.conn.conn.rollback()
            logger.error(f"Error loading fact_exchange_rates: {str(e)}", exc_info=True)
            return False

    def call(self):
        self.load_dim_date()
        self.load_fact_gold()
        self.load_fact_exchange_rates()
