import sys

from dotenv import load_dotenv

from etl.extract.main_extract import extract
from etl.transform.main_transform import transform
from etl.load.main_load import load


if __name__ == "__main__":
    load_dotenv()

    actions = {
        "extract": extract,
        "transform": transform,
        "load": load,
        "all": lambda: (extract(), transform(), load())
    }

    func = actions.get(sys.argv[1]) if len(sys.argv) > 1 else None

    if func:
        func()
    else:
        print("Usage: python -m run [extract|transform|load|all]")
