from config.config_properties import get_ConfigProperties
from app.runtime import get_dbutils

def get_secret(key):
    return get_dbutils().secrets.get(get_ConfigProperties().secrets.secret_scope, key)

def get_proxy_password():
    return get_secret(get_ConfigProperties().secrets.password_key)
