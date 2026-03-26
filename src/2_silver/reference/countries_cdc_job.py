"""
Delta Live Tables (DLT) pipeline for Countries CDC/SCD.

This module defines a DLT pipeline that reads from bronze.countries_raw 
(with Change Data Feed enabled) and creates 2 silver dimension tables:
- ref_dim_country (countries dimension with SCD Type 1)
- ref_country_names_flat (country names in multiple languages)

**Prerequisites:**
Bronze table must have CDF enabled:
  ALTER TABLE main.bronze.countries_raw SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

**How to run:**
Via Databricks DLT Pipeline configuration in resources/pipelines/countries_cdc.yml
"""

import sys

try:
    root_path = spark.conf.get("root_path")
    if root_path and root_path not in sys.path:
        sys.path.insert(0, root_path)
except:
    pass

from src.services.scd.countries_scd import *

# All DLT functions are automatically executed by the DLT runtime


