from etl.load.btc_load import BitcoinLoad
from etl.load.gold_load import GoldLoad
from etl.load.database import DBConnectorLoad
from etl.load.logger import logger


def load():
    logger.info("Starting Load process")

    conn = DBConnectorLoad(logger=logger)

    conn.connect()

    btc = BitcoinLoad(conn)
    btc.call()

    gold = GoldLoad(conn)
    gold.call()

    conn.disconnect()
    logger.info("Load process completed successfully")
