from extract.main_extract import extract
from transform.main_transform import transform
from load.main_load import load


if __name__ == "__main__":
    extract()
    transform()
    load()
