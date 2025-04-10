import os

from dotenv import load_dotenv

from etl.load.mysql_conn import MySQLConnectorLoad
from logger import logger

if __name__ == "__main__":
    load_dotenv()

    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_DATABASE = os.getenv("DB_DATABASE")

    conn = MySQLConnectorLoad(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_DATABASE)

    conn.connect()

    conn.load_dim_date()

    conn.load_fact_btc()
    conn.load_fact_gold()
    conn.load_fact_exchange_rates()

    conn.disconnect()
