import yaml
from pathlib import Path
from copy import deepcopy


BASE_DIR = Path(__file__).resolve().parents[1]
_CONFIG = None
DEFAULT_PROFILE = "dev"

class ConfigProperties:
    """
    Configuration property container that converts nested dicts to objects.
    
    Provides dict-like access (via __getitem__ and get) and attribute access
    to configuration values. Recursively converts nested dicts to ConfigProperties.
    """
    def __init__(self, data: dict):
        for key, value in data.items():
            if isinstance(value, dict):
                value = ConfigProperties(value)
            setattr(self, key, value)
    def __getitem__(self, key):
        return getattr(self, key)
    def get(self, key, default=None):
        return getattr(self, key, default)
    def to_dict(self):
        result = {
            key: value.to_dict() if isinstance(value, ConfigProperties) else value 
            for key, value in self.__dict__.items()
            }
        return result

def load_yaml(path: str) -> dict:
    """Load YAML file from resources directory."""
    file_path = BASE_DIR / path
    with open(file_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def deep_merge(base: dict, override: dict) -> dict:
    """
    Deep merge override dict into base dict.
    
    Recursively merges nested dicts, allowing profile-specific configs
    to override base configuration.
    
    Args:
        base (dict): Base configuration dictionary
        override (dict): Override configuration dictionary
        
    Returns:
        dict: Merged configuration
    """
    result = deepcopy(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result

def load_ConfigProperties(profile: str) -> ConfigProperties:
    """
    Load and merge configuration from base and profile-specific YAML files.
    
    Loads application.yaml and application-{profile}.yaml, merges them,
    and returns as ConfigProperties object.
    
    Args:
        profile (str): Configuration profile name (e.g., 'dev', 'prod')
        
    Returns:
        ConfigProperties: Merged configuration object
    """
    base_config = load_yaml("resources/application.yaml")
    profile_config = load_yaml(f"resources/application-{profile}.yaml")
    merged_config = deep_merge(base_config, profile_config)
    return ConfigProperties(merged_config)

def init_ConfigProperties(profile):
    """Initialize global configuration with specified profile."""
    global _CONFIG
    _CONFIG = load_ConfigProperties(profile)

def get_ConfigProperties():
    global _CONFIG
    if _CONFIG is None:
        init_ConfigProperties(DEFAULT_PROFILE)
    return _CONFIG