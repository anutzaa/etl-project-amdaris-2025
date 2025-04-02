import os
import logging
from dotenv import load_dotenv

from btc_api import BitcoinAPI
from gold_api import GoldAPI
from mysql_conn import MySQLConnector
from logger import logger

if __name__ == "__main__":
    logger.info("Starting ETL")

    load_dotenv()
    logger.debug("Environment variables loaded")

    BTC_API_KEY = os.environ.get('BTC_API_KEY')
    GOLD_API_KEY = os.environ.get('GOLD_API_KEY')

    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_DATABASE = os.getenv("DB_DATABASE")

    logger.info("Connecting to MySQL database")
    conn = MySQLConnector(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_DATABASE
    )

    conn.connect()

    logger.info("Starting Bitcoin API data extraction")
    btc_client = BitcoinAPI(BTC_API_KEY, conn)
    btc_client.call()
    logger.info("Bitcoin API data extraction complete")

    logger.info("Starting Gold API data extraction")
    gold_client = GoldAPI(GOLD_API_KEY, conn)
    gold_client.call()
    logger.info("Gold API data extraction complete")

    logger.info("ETL process completed successfully")
