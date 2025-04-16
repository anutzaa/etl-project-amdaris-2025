from dotenv import load_dotenv

from extract.main import extract
from transform.main import transform
from load.main import load


if __name__ == "__main__":
    load_dotenv()

    extract()
    transform()
    load()
