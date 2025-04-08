import json
import os
from datetime import datetime, timedelta

from etl.transform.mysql_conn import MySQLConnectorTransform
from etl.transform.utils import move_file


class BitcoinTransform:
    def __init__(self, conn: MySQLConnectorTransform):
        self.directory = "../../data/raw/bitcoin/"
        self.conn = conn

    def transform(self, file_path):
        data_type = "bitcoin"
        status = "error"
        processed_count = 0

        try:
            with open(file_path, 'r') as f:
                file_content = f.read()

                if file_content.strip().startswith('['):
                    data_list = json.loads(file_content)
                else:
                    data_list = [json.loads(file_content)]

            for data in data_list:
                meta = data.get("Meta Data", {})
                if not meta:
                    continue

                currency_code = meta.get("4. Market Code", "")
                if not currency_code:
                    continue

                currency_id = self.conn.get_currency_by_code(currency_code)
                if not currency_id:
                    continue

                time_series = data.get("Time Series (Digital Currency Daily)", {})
                if not time_series:
                    continue

                for date_str, daily_data in time_series.items():
                    try:
                        date = datetime.strptime(date_str, "%Y-%m-%d").date()

                        open_price = float(daily_data.get("1. open", 0))
                        high_price = float(daily_data.get("2. high", 0))
                        low_price = float(daily_data.get("3. low", 0))
                        close_price = float(daily_data.get("4. close", 0))
                        volume = float(daily_data.get("5. volume", 0))

                        self.conn.upsert_btc_data(
                            currency_id,
                            date,
                            open_price,
                            high_price,
                            low_price,
                            close_price,
                            volume
                        )

                        processed_count += 1
                    except Exception as e:
                        print(f"Error processing date {date_str}: {str(e)}")

            if processed_count > 0:
                self.conn.log_transform(
                    currency_id,
                    os.path.dirname(file_path),
                    os.path.basename(file_path),
                    processed_count,
                    "SUCCESS"
                )
                status = "processed"
                print(f"Successfully processed {processed_count} data points from file: {file_path}")
            else:
                print(f"No data processed from file: {file_path}")

        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
            status = "error"

        move_file(status, data_type, file_path)

    def call(self):
        if not os.path.exists(self.directory):
            return

        files = [f for f in os.listdir(self.directory) if f.endswith(".json")]
        for file in files:
            file_path = os.path.join(self.directory, file)
            self.transform(file_path)
