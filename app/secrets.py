import config.config as cfg
from app.runtime import get_dbutils

def get_secret(key):
    return get_dbutils().secrets.get(cfg.get_config().secrets.secret_scope, key)

def get_proxy_password():
    return get_secret(cfg.get_config().secrets.password_key)
