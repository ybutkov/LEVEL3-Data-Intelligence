from src.config.config_properties import init_ConfigProperties
import sys
import argparse


def init_app():
    """
    Initialize the application with configuration based on command-line arguments.
    
    Parses --profile and --root_path arguments and initializes ConfigProperties.
    Updates sys.path if root_path is provided.
    
    Args:
        --profile (str): Configuration profile name (default: 'dev')
        --root_path (str): Root path to add to sys.path (default: '')
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="dev")
    parser.add_argument("--root_path", default="")
    args,_ = parser.parse_known_args()
    if args.root_path:
        sys.path.insert(0, args.root_path)
    init_ConfigProperties(args.profile)
