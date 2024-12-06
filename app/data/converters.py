from typing import Dict
from datetime import date, datetime
from dateutil.parser import parse

from app.dto.constraints import DateConstraints, StringConstraints, IntConstraints, FloatConstraints, \
    TimestampConstraints, BooleanConstraints
from app.dto.mock_data import MockDataEntity, MockDataColumn, MockDataForeignKey, MockDataSchema
from app.enums import DataType, RelationType, SourceType


def convert_to_mock_data_entity(schema_name: str, entity_data: Dict) -> MockDataEntity:
    table_name = entity_data["name"]
    total_rows = entity_data["total_rows"]
    entity_columns = []

    for column_data in entity_data["columns"]:
        col_name = column_data["name"]
        col_type = getattr(DataType, column_data["data_type"].upper())
        is_primary_key = column_data.get("is_primary_key", False)
        constraints_data = column_data.get("constraints", {})

        fk_info = column_data.get("foreign_key")
        foreign_key = MockDataForeignKey(
            table_name=fk_info["table_name"],
            column_name=fk_info["column_name"],
            relation_type=getattr(RelationType, fk_info["relation_type"].upper())) if fk_info else None

        null_ratio = constraints_data.get("null_ratio", 0)
        is_unique = constraints_data.get("unique", False) if not column_data.get("is_primary_key") else column_data.get("is_primary_key")
        allowed_values = constraints_data.get("allowed_values", None)

        if col_type == DataType.STRING:
            constraints = StringConstraints(
                null_ratio=null_ratio,
                is_unique=is_unique,
                allowed_values=allowed_values,
                length=constraints_data.get("length", 10),
                uppercase=constraints_data.get("uppercase", False),
                lowercase=constraints_data.get("lowercase", False),
                regular_expr=constraints_data.get("regular_expr", None)
            )
        elif col_type == DataType.INT:
            constraints = IntConstraints(
                null_ratio=null_ratio,
                is_unique=is_unique,
                allowed_values=allowed_values,
                min_value=constraints_data.get("min_value", 0),
                max_value=constraints_data.get("max_value", 1000),
                greater_than=constraints_data.get("min_value", 0),
                less_than=constraints_data.get("less_than", 1000)
            )
        elif col_type == DataType.FLOAT:
            constraints = FloatConstraints(
                null_ratio=null_ratio,
                is_unique=is_unique,
                allowed_values=allowed_values,
                min_value=constraints_data.get("min_value", 0),
                max_value=constraints_data.get("max_value", 1000),
                greater_than=constraints_data.get("min_value", 0),
                less_than=constraints_data.get("less_than", 1000),
                precision=constraints_data.get("precision", 2)
            )
        elif col_type == DataType.DATE:
            min_date = constraints_data.get("min_value")
            max_date = constraints_data.get("max_value")
            greater_than = constraints_data.get("min_value")
            less_than = constraints_data.get("less_than")

            constraints = DateConstraints(
                null_ratio=null_ratio,
                is_unique=is_unique,
                allowed_values=allowed_values,
                min_date=parse(min_date).date() if min_date else date(date.today().year, 1, 1),
                max_date=parse(max_date).date() if max_date else date(date.today().year, 12, 31),
                greater_than=parse(greater_than).date() if greater_than else date(date.today().year, 1, 1),
                less_than=parse(less_than).date() if less_than else date(date.today().year, 12, 31),
                date_format=constraints_data.get("date_format", "%Y-%m-%d")
            )
        elif col_type == DataType.TIMESTAMP:
            min_timestamp = constraints_data.get("min_timestamp")
            max_timestamp = constraints_data.get("max_timestamp")
            greater_than = constraints_data.get("min_value")
            less_than = constraints_data.get("less_than")
            constraints = TimestampConstraints(
                null_ratio=null_ratio,
                is_unique=is_unique,
                allowed_values=allowed_values,
                min_timestamp=parse(max_timestamp) if min_timestamp else datetime(datetime.now().year, 1, 1, 0, 0, 0),
                max_timestamp=parse(max_timestamp) if max_timestamp else datetime(datetime.now().year, 12, 31, 0, 0, 0),
                greater_than=parse(greater_than) if greater_than else datetime(datetime.now().year, 1, 1, 0, 0, 0),
                less_than=parse(less_than) if less_than else datetime(datetime.now().year, 12, 31, 0, 0, 0),
                timestamp_format=constraints_data.get("timestamp_format", "%Y-%m-%d %H:%M:%S")
            )
        elif col_type == DataType.BOOLEAN:
            constraints = BooleanConstraints(
                null_ratio=null_ratio,
                allowed_values=allowed_values
            )
        else:
            constraints = None

        entity_column = MockDataColumn(
            name=col_name,
            data_type=col_type,
            is_primary_key=is_primary_key,
            constraints=constraints,
            foreign_key=foreign_key
        )
        entity_columns.append(entity_column)


    return MockDataEntity(
        schema_name=schema_name,
        table_name=table_name,
        columns=entity_columns,
        total_rows=total_rows
    )



def convert_to_mock_data_schema(entity_schema: Dict):
    schema_name = entity_schema["schema"]
    entities = [convert_to_mock_data_entity(schema_name, entity_data) for entity_data in entity_schema["entities"]]
    return MockDataSchema(
        source_type=getattr(SourceType, entity_schema["source_type"].upper()),
        schema_name=schema_name,
        entities=entities
    )


