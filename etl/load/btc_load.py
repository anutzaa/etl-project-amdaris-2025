from etl.load.database_load import DBConnectorLoad
from etl.load.logger_load import logger


class BitcoinLoad:
    """
    Loads Bitcoin data from the staging table into the data warehouse.

    Methods:
        __init__()              -- Initializes with a DB connection
        load_dim_date()         -- Populates the dim_date table from BTC data
        load_fact_bitcoin()     -- Loads data into fact_btc from staging table
        call()                  -- Executes the full load process

    Instance Variables:
        conn -- DBConnectorLoad instance for DB operations
    """

    def __init__(self, conn: DBConnectorLoad):
        """
        Initializes BitcoinLoad with a database connection.

        Parameters:
            conn -- DBConnectorLoad instance used for database operations
        """
        self.conn = conn

    def load_dim_date(self):
        """
        Populates the 'dim_date' table using dates from 'btc_data_import'.

        Returns:
            None
        """
        self.conn.upsert_dim_date("transform.btc_data_import")

    def load_fact_bitcoin(self):
        """
        Loads data from 'btc_data_import' into the 'fact_btc' table.

        Returns:
            bool -- True if the load succeeds, False otherwise
        """
        logger.info("Loading data into fact_btc from staging table")
        try:
            rows = self.conn.upsert_fact_btc()
            self.conn.conn.commit()
            logger.info(
                f"Total data loaded into fact_btc: {rows} rows affected"
            )
            return True
        except Exception as e:
            self.conn.conn.rollback()
            logger.error(f"Error loading fact_btc: {str(e)}", exc_info=True)
            return False

    def call(self):
        """
        Executes the full load process for Bitcoin data.
        Runs 'dim_date' and 'fact_btc' load methods.

        Returns:
            None
        """
        self.load_dim_date()
        self.load_fact_bitcoin()
