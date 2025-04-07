import json
import os
from datetime import datetime, timedelta

from etl.transform.mysql_conn import MySQLConnectorTransform


class BitcoinTransform:
    def __init__(self, conn: MySQLConnectorTransform):
        self.directory = "../../data/raw/bitcoin/"
        self.conn = conn

    def transform(self, file_path):
        with open(file_path, 'r') as f:
            data = json.load(f)

        meta = data.get("Meta Data", {})
        last_refreshed_str = meta.get("6. Last Refreshed", "")

        if not last_refreshed_str:
            return

        last_refreshed = datetime.strptime(last_refreshed_str.split()[0], "%Y-%m-%d")
        date = last_refreshed - timedelta(days=1)

        date_str = date.strftime("%Y-%m-%d")

        currency_code = meta.get("4. Market Code", "")
        time_series = data.get("Time Series (Digital Currency Daily)", {})

        latest_data = time_series.get(date_str)

        if not latest_data:
            return

        currency_id = self.conn.get_currency_by_code(currency_code)

        if not currency_id:
            return

        self.conn.insert_btc_data(
            currency_id,
            date,
            float(latest_data["1. open"]),
            float(latest_data["2. high"]),
            float(latest_data["3. low"]),
            float(latest_data["4. close"]),
            float(latest_data["5. volume"])
        )

    def call(self):
        for file in os.listdir(self.directory):
            if file.endswith(".json"):
                self.transform(os.path.join(self.directory, file))
