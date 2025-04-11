from etl.load.mysql_conn import MySQLConnectorLoad
from logger import logger


class BitcoinLoad:
    def __init__(self, conn: MySQLConnectorLoad):
        self.conn = conn

    def load_dim_date(self):
        self.conn.upsert_dim_date('btc_data_import')

    def load_fact_bitcoin(self):
        logger.info("Loading data into fact_btc from staging table")
        try:
            rows = self.conn.upsert_fact_btc()
            self.conn.connection.commit()
            logger.info(f"Total data loaded into fact_btc: {rows} rows affected")
            return True
        except Exception as e:
            self.conn.connection.rollback()
            logger.error(f"Error loading fact_btc: {str(e)}", exc_info=True)
            return False

    def call(self):
        self.load_dim_date()
        self.load_fact_bitcoin()
