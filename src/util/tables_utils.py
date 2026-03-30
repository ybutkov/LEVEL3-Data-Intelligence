def build_table_name(cfg, schema_name: str, table_name: str) -> str:
    return f"{cfg.storage.catalog}.{schema_name}.{table_name}"