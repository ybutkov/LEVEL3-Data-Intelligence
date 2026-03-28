import sys

try:
    root_path = spark.conf.get("root_path")
    if root_path and root_path not in sys.path:
        sys.path.insert(0, root_path)
except:
    pass


from src.services.scd.operations.flight_status_scd import *

