from etl.load.btc_load import BitcoinLoad
from etl.load.gold_load import GoldLoad
from etl.load.mysql_conn import DBConnectorLoad
from etl.load.logger import logger


def load():
    """
    Run the full data loading process.

    This function initializes the database connection, processes Bitcoin and Gold data loading,
    logs each process, then closes the database connection.
    """
    logger.info("Starting Load process")

    conn = DBConnectorLoad(logger=logger)

    conn.connect()

    btc = BitcoinLoad(conn)
    btc.call()

    gold = GoldLoad(conn)
    gold.call()

    conn.disconnect()
    logger.info("Load process completed successfully")
