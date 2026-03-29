from src.app.logger import get_logger
from src.services.parser.utils.parser_utils import (
    build_parsed_df,
    split_valid_invalid,
    log_invalid_records,
    add_silver_metadata,
    build_table_name,
)
from src.config.endpoints import get_endpoint_config
from src.services.parser.utils.validation_utils import validate_outputs
from src.services.parser.utils.normalize_utils import normalize_outputs
from src.services.parser.utils.transform_utils import transform_outputs


logger = get_logger(__name__)

APPEND_TABLES = {
    "op_fact_flight_status",
}



def run_parser(
    spark,
    cfg,
    endpoint_key,
    schema,
    build_outputs_fn,
    normalization_map,
    transformation_map,
    rules_map,
    deduplicate_fn=None,
): 

    endpoint_cfg = get_endpoint_config(endpoint_key)
    dataset_name = endpoint_cfg.bronze_table
    bronze_table = endpoint_cfg.build_bronze_table_name(cfg.storage)

    invalid_log_table = build_table_name(
        cfg, cfg.storage.silver_schema, "audit_invalid_json_log"
    )
    quarantine_table = build_table_name(
        cfg, cfg.storage.silver_schema, "audit_quarantine_records"
    )

    logger.info(f"START parser: {dataset_name}")
    logger.info(f"Bronze source table: {bronze_table}")

    try:
        parsed_df = build_parsed_df(
            spark=spark,
            bronze_table=bronze_table,
            schema=schema,
            raw_json_col="raw_json",
        )

        valid_df, invalid_df = split_valid_invalid(parsed_df)

        logger.info("Log invalid JSON rows")
        log_invalid_records(
            invalid_df=invalid_df,
            log_table=invalid_log_table,
            raw_json_col="raw_json",
            dataset_name=dataset_name,
        )

        logger.info("Build entity outputs")
        outputs = build_outputs_fn(valid_df)
        if not outputs:
            logger.warning(f"Not outputs for dataset: {dataset_name}")

        logger.info("Normalize outputs")
        normalized_outputs = normalize_outputs(
            outputs=outputs,
            normalization_map=normalization_map,
        )

        logger.info("Transform outputs")
        transformed_outputs = transform_outputs(
            outputs=normalized_outputs,
            transformation_map=transformation_map,
        )

        logger.info("Validate outputs")
        table_rules = rules_map.get(dataset_name, {})
        validated_outputs, quarantine_df = validate_outputs(
            outputs=transformed_outputs,
            dataset_name=dataset_name,
            table_rules=table_rules,
        )

        for table_name, df in validated_outputs.items():
            full_table_name = build_table_name(
                cfg, cfg.storage.silver_schema, table_name
            )

            if not table_name.startswith("audit_"):
                logger.info(f"Add silver metadata: {table_name}")
                df = add_silver_metadata(df)

            write_mode = "append" if table_name in APPEND_TABLES else "overwrite"
            if deduplicate_fn and table_name in APPEND_TABLES:
                logger.info(f"Deduplicate before append: {table_name}")
                df = deduplicate_fn(
                    spark=spark,
                    new_df=df,
                    target_table=full_table_name,
                )


            logger.info(f"Write table: {full_table_name}")
            df.write.mode(write_mode).saveAsTable(full_table_name)

        if quarantine_df is not None:
            logger.info(f"Write quarantine table: {quarantine_table}")
            quarantine_df.write.mode("append").saveAsTable(quarantine_table)

        logger.info(f"FINISH parser: {dataset_name}")

    except Exception as e:
        logger.exception(f"FAILED parser: {dataset_name}, error={e}")
        raise