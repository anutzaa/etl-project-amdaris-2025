import os
from dotenv import load_dotenv

from btc_api import BitcoinAPI
from gold_api import GoldAPI
from mysql_conn import MySQLConnector


if __name__ == "__main__":

    load_dotenv()

    BTC_API_KEY = os.environ.get('BTC_API_KEY')
    GOLD_API_KEY = os.environ.get('GOLD_API_KEY')

    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_DATABASE = os.getenv("DB_DATABASE")

    conn = MySQLConnector(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_DATABASE
    )

    conn.connect()

    btc_client = BitcoinAPI(BTC_API_KEY, conn)
    btc_client.call()

    gold_client = GoldAPI(GOLD_API_KEY, conn)
    gold_client.call()
