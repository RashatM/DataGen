from collections.abc import Mapping
from typing import Any, cast

from app.application.constants import WriteMode
from app.application.dto.pipeline import (
    ComparisonQuerySpec,
    PipelineExecutionSpec,
    TableExecutionSpec,
    TableLoadSpec,
)
from app.infrastructure.converters.contract.fields import (
    require_list_of_mappings,
    require_mapping,
    require_non_empty_string,
)
from app.infrastructure.converters.contract.load_projection_builder import (
    build_hive_load_columns,
    build_iceberg_load_columns,
)
from app.infrastructure.converters.contract.table_compiler import ContractTableCompiler
from app.infrastructure.errors import SchemaValidationError


def convert_to_pipeline_execution_spec(raw_contract: Mapping[str, Any]) -> PipelineExecutionSpec:
    raw_queries = require_mapping(raw_contract.get("queries"), "Contract queries")
    comparison = ComparisonQuerySpec(
        hive_sql=require_non_empty_string(raw_queries.get("hive_sql"), "Contract queries hive_sql"),
        iceberg_sql=require_non_empty_string(raw_queries.get("iceberg_sql"), "Contract queries iceberg_sql"),
        hive_exclude_columns=tuple(raw_queries.get("hive_exclude_columns") or ()),
        iceberg_exclude_columns=tuple(raw_queries.get("iceberg_exclude_columns") or ()),
    )

    raw_tables = require_list_of_mappings(raw_contract.get("tables"), "Contract tables")
    compiler = ContractTableCompiler(raw_tables)

    execution_tables: list[TableExecutionSpec] = []
    for table_data in compiler.raw_tables:
        table_name = cast(str, table_data["table_name"])
        table_columns_data = compiler.get_raw_columns(table_name)
        table_spec = compiler.build_table_spec(table_name)
        try:
            write_mode = WriteMode(require_non_empty_string(table_data.get("write_mode"), f"Table {table_name} write_mode"))
        except ValueError as exc:
            raise SchemaValidationError(
                f"Unsupported write_mode for table {table_name}: {table_data.get('write_mode')}"
            ) from exc
        execution_tables.append(
            TableExecutionSpec(
                table=table_spec,
                load_spec=TableLoadSpec(
                    hive_target_table=require_non_empty_string(
                        table_data.get("hive_target_table"),
                        f"Table {table_name} hive_target_table",
                    ),
                    iceberg_target_table=require_non_empty_string(
                        table_data.get("iceberg_target_table"),
                        f"Table {table_name} iceberg_target_table",
                    ),
                    write_mode=write_mode,
                    hive_columns=build_hive_load_columns(
                        table_name=table_name,
                        raw_columns=table_columns_data,
                    ),
                    iceberg_columns=build_iceberg_load_columns(
                        table_name=table_name,
                        raw_columns=table_columns_data,
                    ),
                ),
            )
        )

    return PipelineExecutionSpec(
        tables=tuple(execution_tables),
        comparison=comparison,
    )
