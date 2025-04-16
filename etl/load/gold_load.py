from etl.load.mysql_conn import DBConnectorLoad
from etl.load.logger import logger


class GoldLoad:
    """
    Loads transformed gold data and exchange rates into the data warehouse.

    Methods:
        __init__()                 -- Initializes with a DB connection
        load_dim_date()            -- Populates the dim_date table
        load_fact_gold()           -- Loads data into the fact_gold table
        load_fact_exchange_rates() -- Loads data into the fact_exchange_rates table
        call()                     -- Executes the full data load process

    Instance Variables:
        conn -- DBConnectorLoad instance used for database operations
    """

    def __init__(self, conn: DBConnectorLoad):
        """
        Initializes GoldLoad with a database connection.

        Parameters:
            conn -- DBConnectorLoad instance used for database operations
        """
        self.conn = conn

    def load_dim_date(self):
        """
        Populates the 'dim_date' table using dates from the 'gold_data_import' table.

        Returns:
            None
        """
        self.conn.upsert_dim_date("transform.gold_data_import")

    def load_fact_gold(self):
        """
        Loads data from the 'gold_data_import' table into the 'fact_gold' table.

        Returns:
            bool -- True if the load succeeds, False otherwise
        """
        logger.info("Loading data into fact_gold from staging table")
        try:
            rows = self.conn.upsert_fact_gold()
            self.conn.conn.commit()
            logger.info(
                f"Total data loaded into fact_gold: {rows} rows affected"
            )
            return True

        except Exception as e:
            self.conn.conn.rollback()
            logger.error(f"Error loading fact_gold: {str(e)}", exc_info=True)
            return False

    def load_fact_exchange_rates(self):
        """
        Loads currency exchange rates into the 'fact_exchange_rates' table.

        Returns:
            bool -- True if the load succeeds, False otherwise
        """
        logger.info("Loading data into fact_exchange_rates from staging table")
        try:
            currency_codes = self.conn.get_rate_cols()
            total_rows_affected = 0

            for currency_code in currency_codes:
                logger.debug(
                    f"Processing exchange rates for currency: {currency_code}"
                )
                rows = self.conn.upsert_exchange_rates(currency_code)
                if rows:
                    total_rows_affected += rows

            self.conn.conn.commit()
            logger.info(
                f"Total data loaded into fact_exchange_rates: {total_rows_affected} rows affected"
            )
            return True

        except Exception as e:
            self.conn.conn.rollback()
            logger.error(
                f"Error loading fact_exchange_rates: {str(e)}", exc_info=True
            )
            return False

    def call(self):
        """
        Executes the full load process for gold data and exchange rates.
        Runs 'dim_date', 'fact_gold', and 'fact_exchange_rates' load methods.

        Returns:
            None
        """
        self.load_dim_date()
        self.load_fact_gold()
        self.load_fact_exchange_rates()
