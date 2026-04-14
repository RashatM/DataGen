from app.domain.entities import TableColumnSpec
from app.domain.value_types import ColumnValues, NonNullColumnValue
from app.domain.validation_errors import InvalidConstraintsError, UnsatisfiableConstraintsError


def validate_column_output_values(
    table_column: TableColumnSpec,
    values: ColumnValues,
) -> None:
    seen_values: set[NonNullColumnValue] = set()

    for value in values:
        if value is None:
            if table_column.is_primary_key:
                raise InvalidConstraintsError(f"Primary key column {table_column.name} cannot contain null values")
            continue

        if not table_column.output_constraints.is_unique:
            continue

        if value in seen_values:
            raise UnsatisfiableConstraintsError(
                f"Generated values for column {table_column.name} are not unique in final output"
            )
        seen_values.add(value)
