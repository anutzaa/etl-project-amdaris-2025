from etl.transform.btc_transform import BitcoinTransform
from etl.transform.gold_transform import GoldTransform
from etl.transform.database_transform import DBConnectorTransform
from etl.transform.logger_transform import logger


def transform():
    """
     Run the full data transformation process.

     This function connects to the database, processes all Bitcoin and Gold files,
     logs each transformation, and closes the connection.
    """
    logger.info("Starting Transform process")

    logger.info("Connecting to MySQL database")
    conn = DBConnectorTransform(logger=logger)

    conn.connect()
    conn.truncate_import_tables()

    logger.info("Starting Bitcoin data transformation")
    btc = BitcoinTransform(conn)
    btc.call()
    logger.info("Bitcoin data transformation complete")

    logger.info("Starting Gold data transformation")
    gold = GoldTransform(conn)
    gold.call()
    logger.info("Gold data transformation complete")

    conn.disconnect()
    logger.info("Connection to MySQL database closed")

    logger.info("Transform process completed successfully")
