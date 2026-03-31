from src.config.config_properties import get_ConfigProperties
from src.app.runtime import get_dbutils


def get_secret(key):
    """
    Retrieve a secret from Databricks secret store.
    
    Args:
        key (str): Secret key to retrieve
        
    Returns:
        str: Secret value
    """
    return get_dbutils().secrets.get(get_ConfigProperties().secrets.secret_scope, key)

def get_proxy_password():
    """Retrieve proxy password from secrets."""
    return get_secret(get_ConfigProperties().secrets.password_key)
