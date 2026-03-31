def build_table_name(cfg, schema_name: str, table_name: str) -> str:
    return f"{cfg.storage.catalog}.{schema_name}.{table_name}"

def build_full_table_name(catalog: str, schema_name: str, table_name: str) -> str:
    return f"{catalog}.{schema_name}.{table_name}"