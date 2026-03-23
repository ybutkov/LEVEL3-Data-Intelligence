from pyspark.sql.functions import col, current_timestamp, to_json, struct, lit


def build_condition(rules: dict):
    cond = None

    for field_name, checks in rules.items():
        field_col = col(field_name)

        field_cond = None
        for check in checks:
            expr = check(field_col)
            field_cond = expr if field_cond is None else (field_cond & expr)

        cond = field_cond if cond is None else (cond & field_cond)

    return cond


def split_by_rules(df, rules: dict):
    cond = build_condition(rules)
    valid_df = df.filter(cond)
    invalid_df = df.filter(~cond)
    return valid_df, invalid_df


def build_quarantine_df(
    df,
    dataset_name: str,
    target_table: str,
    rule_name: str,
):
    payload_cols = [
        col for col in df.columns
        if col not in ["source_file", "bronze_ingested_at"]
        ]
    return (
        df
        .withColumn("dataset_name", lit(dataset_name))
        .withColumn("target_table", lit(target_table))
        .withColumn("rule_name", lit(rule_name))
        .withColumn("row_payload", to_json(struct(*payload_cols)))
        .withColumn("quarantined_at", current_timestamp())
        .select(
            "dataset_name",
            "target_table",
            "rule_name",
            "source_file",
            "bronze_ingested_at",
            "row_payload",
            "quarantined_at",
        )
    )
