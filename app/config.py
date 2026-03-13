import yaml
from pathlib import Path
from copy import deepcopy

BASE_DIR = Path(__file__).resolve().parents[1]
_CONFIG = None

class Config:
    def __init__(self, data: dict):
        for key, value in data.items():
            if isinstance(value, dict):
                value = Config(value)
            setattr(self, key, value)

def load_yaml(path: str) -> dict:
    file_path = BASE_DIR / path
    with open(file_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def deep_merge(base: dict, override: dict) -> dict:
    result = deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result

def load_config(profile: str) -> Config:
    base_config = load_yaml("config/base.yaml")
    profile_config = load_yaml(f"config/{profile}.yaml")
    merged_config = deep_merge(base_config, profile_config)
    return Config(merged_config)

def init_config(profile):
    global _CONFIG
    _CONFIG = load_config(profile)

def get_config():
    # Error if config is not initialized
    return _CONFIG