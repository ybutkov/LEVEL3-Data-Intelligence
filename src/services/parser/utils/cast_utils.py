from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def cast_columns(df: DataFrame, cast_map: dict[str, str]) -> DataFrame:
    result_df = df

    for column_name, target_type in cast_map.items():
        if column_name in result_df.columns:
            result_df = result_df.withColumn(
                column_name,
                col(column_name).cast(target_type)
            )

    return result_df


def cast_outputs(outputs: dict[str, DataFrame], casting_map: dict[str, dict[str, str]]):
    casted_outputs = {}

    for table_name, df in outputs.items():
        table_cast_map = casting_map.get(table_name, {})
        casted_outputs[table_name] = cast_columns(df, table_cast_map)

    return casted_outputs
