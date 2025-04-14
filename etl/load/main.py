import os

from dotenv import load_dotenv

from etl.load.btc_load import BitcoinLoad
from etl.load.gold_load import GoldLoad
from etl.load.mysql_conn import MySQLConnectorLoad
from etl.load.logger import logger


def load():
    logger.info("Starting Load process")

    load_dotenv()
    logger.debug("Environment variables loaded")

    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_DATABASE = os.getenv("DB_DATABASE")

    conn = MySQLConnectorLoad(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_DATABASE)

    conn.connect()

    btc = BitcoinLoad(conn)
    btc.call()

    gold = GoldLoad(conn)
    gold.call()

    conn.disconnect()
    logger.info("Load process completed successfully")
