from src.config.config_properties import init_ConfigProperties
import sys
import argparse


def init_app():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="dev")
    parser.add_argument("--root_path", default="")
    args,_ = parser.parse_known_args()
    if args.root_path:
        sys.path.insert(0, args.root_path)
    init_ConfigProperties(args.profile)
