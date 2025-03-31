import json
import os
import requests
from datetime import datetime


def save_to_file(data, api_type):
    base_dir = '../../data/raw/'

    output_dir = os.path.join(base_dir, 'bitcoin' if api_type == 'btc' else 'gold')
    file_name = f'{api_type}_{datetime.today().strftime("%Y%m%d")}.json'

    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, file_name)

    timestamped_data = {
        "timestamp": datetime.now().isoformat(),
        "data": data
    }

    if os.path.exists(file_path):
        try:
            with open(file_path, 'r') as json_file:
                existing_data = json.load(json_file)
                if not isinstance(existing_data, list):
                    existing_data = [existing_data]
        except json.JSONDecodeError:
            existing_data = []
    else:
        existing_data = []

    existing_data.append(timestamped_data)

    with open(file_path, 'w') as json_file:
        json.dump(existing_data, json_file, indent=4)

    return file_path


class BitcoinAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = 'https://www.alphavantage.co/query'

    def get_bitcoin_data(self, symbol, market, function='DIGITAL_CURRENCY_DAILY'):
        params = {'function': function,
                  'symbol': symbol,
                  'market': market,
                  'apikey': self.api_key
                  }
        response = requests.get(self.base_url, params=params)
        response.raise_for_status()

        data = response.json()
        save_to_file(data, 'btc')


class GoldAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = 'https://gold.g.apised.com/v1/latest'

    def get_gold_data(self, symbol):
        params = {'metals': 'XAU',
                  'base_currency': symbol,
                  'weight_unit': 'gram'
                  }

        headers = {
            'x-api-key': self.api_key
        }

        response = requests.get(self.base_url, params=params, headers=headers)
        response.raise_for_status()

        data = response.json()
        save_to_file(data, 'gold')


if __name__ == "__main__":
    btc_api_key = 'VERHHKYF2OE4HKK8'
    gold_api_key = 'sk_59f2433F46BC20EcB6A6BF7317862e4cEF3abd16EB3d4058'

    btc_client = BitcoinAPI(btc_api_key)
    gold_client = GoldAPI(gold_api_key)

    currencies = ['EUR', 'USD', 'GBP']

    for currency in currencies:
        btc_client.get_bitcoin_data('BTC', market=currency)
        gold_client.get_gold_data(currency)
