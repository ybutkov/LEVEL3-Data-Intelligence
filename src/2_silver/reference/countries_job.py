import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--root_path", default="")
args, _ = parser.parse_known_args()

if args.root_path and args.root_path not in sys.path:
    sys.path.insert(0, args.root_path)

from src.app.init_app import init_app
from src.config.config_properties import get_ConfigProperties
from src.services.parser.references.countries import run_countries
from src.services.parser.references.cities import run_cities


def main():
    init_app()
    cfg = get_ConfigProperties()
    run_countries(spark, cfg)
    run_cities(spark, cfg)


if __name__ == "__main__":
    main()
