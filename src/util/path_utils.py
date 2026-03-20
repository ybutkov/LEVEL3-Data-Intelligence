from src.app.runtime import get_dbutils


def path_exists(path: str):
    try:
        get_dbutils().fs.ls(path)
        return True
    except Exception:
        return False
    