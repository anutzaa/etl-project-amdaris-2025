import os
from dotenv import load_dotenv

from btc_api import BitcoinAPI
from gold_api import GoldAPI

load_dotenv()

if __name__ == "__main__":
    BTC_API_KEY = os.environ.get('BTC_API_KEY')
    GOLD_API_KEY = os.environ.get('GOLD_API_KEY')

    btc_client = BitcoinAPI(BTC_API_KEY)
    gold_client = GoldAPI(GOLD_API_KEY)

    currencies = ['EUR', 'USD', 'GBP']

    for currency in currencies:
        btc_client.get_bitcoin_data('BTC', market=currency)
        gold_client.get_gold_data(currency)



