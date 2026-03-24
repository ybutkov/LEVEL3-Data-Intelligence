from pyspark.sql import DataFrame


def apply_transformations(df: DataFrame, transformations: list) -> DataFrame:
    result_df = df

    for transform in transformations:
        result_df = transform(result_df)

    return result_df


def transform_outputs(
    outputs: dict[str, DataFrame],
    transformation_map: dict[str, list],
) -> dict[str, DataFrame]:
    transformed_outputs = {}

    for table_name, df in outputs.items():
        transformations = transformation_map.get(table_name, [])
        transformed_outputs[table_name] = apply_transformations(df, transformations)

    return transformed_outputs
