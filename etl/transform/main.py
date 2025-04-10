import os

from dotenv import load_dotenv

from btc_transform import BitcoinTransform
from gold_transform import GoldTransform
from mysql_conn import MySQLConnectorTransform
from logger import logger

if __name__ == "__main__":
    logger.info("Starting Transform process")

    load_dotenv()
    logger.debug("Environment variables loaded")

    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_DATABASE = os.getenv("DB_DATABASE")

    logger.info("Connecting to MySQL database")
    conn = MySQLConnectorTransform(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_DATABASE)

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
