import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--root_path", default="")
args, _ = parser.parse_known_args()

if args.root_path and args.root_path not in sys.path:
    sys.path.insert(0, args.root_path)

from src.app.init_app import init_app
from src.app.logger import get_logger
from src.config.config_properties import get_ConfigProperties
from src.services.parser.references.countries import run_countries
from src.services.parser.references.cities import run_cities
from src.services.parser.references.airports import run_airports
from src.services.parser.references.airlines import run_airlines
from src.services.parser.references.aircraft import run_aircraft

def reference_silver_job():
    init_app()
    cfg = get_ConfigProperties()
    logger = get_logger(__name__)
    logger.info(f"START silver references job")
    run_countries(spark, cfg)
    run_cities(spark, cfg)
    run_airports(spark, cfg)
    run_airlines(spark, cfg)
    run_aircraft(spark, cfg)
    logger.info(f"FINISH silver references job")


if __name__ == "__main__":
    reference_silver_job()
