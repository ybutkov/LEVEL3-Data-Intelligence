from pyspark.sql.functions import col, explode


def explode_entity(valid_df, entity_path: str, entity_alias: str):
    return (
        valid_df
        .select(
            "source_file",
            "bronze_ingested_at",
            explode(col(entity_path)).alias(entity_alias)
        )
    )


def build_array_names_flat_df(
    exploded_df,
    entity_alias: str,
    code_field: str,
    code_alias: str,
    name_alias: str,
):
    return (
        exploded_df
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col(f"{entity_alias}.{code_field}").alias(code_alias),
            explode(col(f"{entity_alias}.Names.Name")).alias("name")
        )
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col(code_alias),
            col("name.@LanguageCode").alias("language_code"),
            col("name.$").alias(name_alias),
        )
    )


def build_single_names_flat_df(
    exploded_df,
    entity_alias: str,
    code_field: str,
    code_alias: str,
    name_alias: str,
):
    return (
        exploded_df
        .select(
            col("source_file"),
            col("bronze_ingested_at"),
            col(f"{entity_alias}.{code_field}").alias(code_alias),
            col(f"{entity_alias}.Names.Name.@LanguageCode").alias("language_code"),
            col(f"{entity_alias}.Names.Name.$").alias(name_alias),
        )
    )


def build_simple_dim_df(
    exploded_df,
    entity_alias: str,
    key_field: str,
    key_alias: str,
    extra_fields: dict[str, str] | None = None,
):
    select_exprs = [
        col("source_file"),
        col("bronze_ingested_at"),
        col(f"{entity_alias}.{key_field}").alias(key_alias),
    ]

    if extra_fields:
        for source_field, target_alias in extra_fields.items():
            select_exprs.append(
                col(f"{entity_alias}.{source_field}").alias(target_alias)
            )

    return (
        exploded_df
        .select(*select_exprs)
        .dropDuplicates([key_alias])
    )
