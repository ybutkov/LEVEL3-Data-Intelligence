import argparse
from app.config import init_config

def init_app():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="dev")
    args,_ = parser.parse_known_args()
    init_config(args.profile)