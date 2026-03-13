import config.config as cfg
from app.runtime import get_dbutils

def get_secret(key):
    return get_dbutils().secrets.get(cfg.SECRET_SCOPE, key)

def get_proxy_password():
    return get_secret(cfg.PROXY_PASSWORD_KEY)
