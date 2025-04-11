import os
from dotenv import load_dotenv

from etl.extract.btc_api import BitcoinAPI
from etl.extract.gold_api import GoldAPI
from etl.extract.mysql_conn import MySQLConnector
from etl.extract.logger import logger


def extract():
    logger.info("Starting Extract process")

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
    btc = BitcoinAPI(BTC_API_KEY, conn)
    btc.call()
    logger.info("Bitcoin API data extraction complete")

    logger.info("Starting Gold API data extraction")
    gold = GoldAPI(GOLD_API_KEY, conn)
    gold.call()
    logger.info("Gold API data extraction complete")

    conn.disconnect()
    logger.info("Connection to MySQL database closed")

    logger.info("Extract process completed successfully")
