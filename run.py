import sys

from dotenv import load_dotenv

from etl.extract.main_extract import extract
from etl.transform.main import transform
from etl.load.main import load


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
        print("Usage: python -m etl.etl_run [extract|transform|load|all]")
