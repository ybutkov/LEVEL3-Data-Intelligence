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
        if col not in ["source_file", "bronze_ingested_at", "processed_at"]
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

def validate_outputs(outputs: dict, dataset_name: str, table_rules: dict):
    valid_outputs = {}
    quarantine_dfs = []

    for table_name, df in outputs.items():
        rules = table_rules.get(table_name)

        if not rules:
            valid_outputs[table_name] = df
            continue

        valid_df, invalid_df = split_by_rules(df, rules)
        valid_outputs[table_name] = valid_df

        if invalid_df.take(1):
            quarantine_dfs.append(
                build_quarantine_df(
                    invalid_df,
                    dataset_name=dataset_name,
                    target_table=table_name,
                    rule_name=f"{table_name}_rules",
                )
            )

    quarantine_df = None
    if quarantine_dfs:
        quarantine_df = quarantine_dfs[0]
        for qdf in quarantine_dfs[1:]:
            quarantine_df = quarantine_df.unionByName(qdf)

    return valid_outputs, quarantine_df
