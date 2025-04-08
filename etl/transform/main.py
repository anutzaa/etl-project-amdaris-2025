import os

from dotenv import load_dotenv

from etl.transform.btc_transform import BitcoinTransform
from etl.transform.gold_transform import GoldTransform
from etl.transform.mysql_conn import MySQLConnectorTransform

if __name__ == '__main__':
    load_dotenv()

    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_DATABASE = os.getenv("DB_DATABASE")

    conn = MySQLConnectorTransform(host=DB_HOST,
                                   port=DB_PORT,
                                   user=DB_USER,
                                   password=DB_PASSWORD,
                                   database=DB_DATABASE
                                   )

    conn.connect()

    btc = BitcoinTransform(conn)
    btc.call()

    gold = GoldTransform(conn)
    gold.call()

    conn.disconnect()
