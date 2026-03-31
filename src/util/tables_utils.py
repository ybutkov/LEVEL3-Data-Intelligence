def build_table_name(cfg, schema_name: str, table_name: str) -> str:
    """
    Build fully qualified table name using config catalog and schema.
    
    Args:
        cfg: Configuration object with storage.catalog property
        schema_name (str): Schema name
        table_name (str): Table name
        
    Returns:
        str: Fully qualified table name in format "catalog.schema.table"
    """
    return f"{cfg.storage.catalog}.{schema_name}.{table_name}"

def build_full_table_name(catalog: str, schema_name: str, table_name: str) -> str:
    """
    Build fully qualified table name from individual components.
    
    Args:
        catalog (str): Catalog name
        schema_name (str): Schema name
        table_name (str): Table name
        
    Returns:
        str: Fully qualified table name in format "catalog.schema.table"
    """
    return f"{catalog}.{schema_name}.{table_name}"