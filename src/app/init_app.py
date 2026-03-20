from src.config.config_properties import init_ConfigProperties
import argparse


def init_app():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="dev")
    args,_ = parser.parse_known_args()
    init_ConfigProperties(args.profile)
