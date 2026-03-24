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
from src.services.parser.operations.flight_status_by_route import run_flight_status_by_route

def operations_silver_job():
    init_app()
    cfg = get_ConfigProperties()
    logger = get_logger(__name__)
    logger.info(f"START silver operations job")
    run_flight_status_by_route(spark, cfg)
    logger.info(f"FINISH silver operations job")


if __name__ == "__main__":
    operations_silver_job()
