from etl.extract.btc_extract import BitcoinExtract
from etl.extract.gold_extract import GoldExtract
from etl.extract.database_extract import DBConnectorExtract
from etl.extract.logger_extract import logger


def extract():
    """
     Run the full data extraction process.

     This function establishes a connection to the MySQL database,
     extracts data from the Bitcoin and Gold APIs, logs the imports,
     and then closes the DB connection.
     """
    logger.info("Starting Extract process")

    logger.info("Connecting to MySQL database")
    conn = DBConnectorExtract(logger=logger)

    conn.connect()

    logger.info("Starting Bitcoin API data extraction")
    btc = BitcoinExtract(conn)
    btc.call()
    logger.info("Bitcoin API data extraction complete")

    logger.info("Starting Gold API data extraction")
    gold = GoldExtract(conn)
    gold.call()
    logger.info("Gold API data extraction complete")

    conn.disconnect()
    logger.info("Connection to MySQL database closed")

    logger.info("Extract process completed successfully")
