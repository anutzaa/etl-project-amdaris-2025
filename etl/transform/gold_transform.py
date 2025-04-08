import json
import os
from datetime import datetime

from etl.transform.mysql_conn import MySQLConnectorTransform
from etl.transform.utils import move_file


class GoldTransform:
    def __init__(self, conn: MySQLConnectorTransform):
        self.directory = "../../data/raw/gold/"
        self.conn = conn

    def transform(self, file_path):
        data_type = "gold"
        status = "error"
        processed_count = 0
        currency_id = None

        try:
            with open(file_path, 'r') as f:
                file_content = f.read()

                if file_content.strip().startswith('['):
                    data_list = json.loads(file_content)
                else:
                    data_list = [json.loads(file_content)]

            for data_obj in data_list:
                if data_obj.get("status") != "success" or "data" not in data_obj:
                    print(f"Invalid data format in file: {file_path}")
                    continue

                data = data_obj["data"]

                base_currency = data.get("base_currency", "")
                if not base_currency:
                    print(f"No base currency found in file: {file_path}")
                    continue

                currency_id = self.conn.get_currency_by_code(base_currency)
                if not currency_id:
                    print(f"No currency ID found for code: {base_currency}")
                    continue

                timestamp_ms = data.get("timestamp")
                if not timestamp_ms:
                    print(f"No timestamp found in file: {file_path}")
                    continue

                date = datetime.fromtimestamp(timestamp_ms / 1000).date()

                metal_prices = data.get("metal_prices", {}).get("XAU", {})
                if not metal_prices:
                    print(f"No XAU metal prices found in file: {file_path}")
                    continue

                currency_rates = data.get("currency_rates", {})
                if not currency_rates:
                    print(f"No currency rates found in file: {file_path}")
                    continue

                try:
                    open_price = float(metal_prices.get("open", 0))
                    high_price = float(metal_prices.get("high", 0))
                    low_price = float(metal_prices.get("low", 0))
                    price = float(metal_prices.get("price", 0))

                    price_24k = float(metal_prices.get("price_24k", 0))
                    price_18k = float(metal_prices.get("price_18k", 0))
                    price_14k = float(metal_prices.get("price_14k", 0))

                    rate_usd = float(currency_rates.get("USD", 0))
                    rate_eur = float(currency_rates.get("EUR", 0))
                    rate_gbp = float(currency_rates.get("GBP", 0))

                    self.conn.upsert_gold_data(
                        currency_id,
                        date,
                        open_price,
                        high_price,
                        low_price,
                        price,
                        price_24k,
                        price_18k,
                        price_14k,
                        rate_usd,
                        rate_eur,
                        rate_gbp
                    )

                    processed_count += 1
                except Exception as e:
                    print(f"Error processing gold data: {str(e)}")

            if processed_count > 0:
                status = "processed"
                print(f"Successfully processed {processed_count} data points from file: {file_path}")
            else:
                print(f"No data processed from file: {file_path}")

        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
            status = "error"

        new_file_path = move_file(status, data_type, file_path)

        self.conn.log_transform(
            currency_id,
            os.path.dirname(new_file_path),
            os.path.basename(new_file_path),
            processed_count,
            status
        )

    def call(self):
        if not os.path.exists(self.directory):
            print(f"Directory not found: {self.directory}")
            return

        files = [f for f in os.listdir(self.directory) if f.endswith(".json")]
        for file in files:
            file_path = os.path.join(self.directory, file)
            self.transform(file_path)
