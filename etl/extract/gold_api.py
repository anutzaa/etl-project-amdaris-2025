import requests

from utils import save_to_file


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

