import requests

from utils import save_to_file


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
