from datetime import date, datetime
from typing import TypeAlias

from app.domain.constraints import OutputConstraints
from app.domain.entities import TableColumnSpec, TableDerivationSpec
from app.domain.enums import DataType, DerivationRule
from app.domain.value_types import NonNullColumnValue, NonNullColumnValues
from app.domain.validation_errors import InvalidDerivationError

TemporalSourceValue: TypeAlias = date | datetime
TEMPORAL_SOURCE_TYPES: frozenset[DataType] = frozenset((DataType.DATE, DataType.TIMESTAMP))
TEMPORAL_SOURCE_VALUE_TYPES = (date, datetime)
DERIVATION_ALLOWED_OUTPUT_TYPES: dict[DerivationRule, frozenset[DataType]] = {
    DerivationRule.YYYYMMDD: frozenset((DataType.INT, DataType.STRING)),
    DerivationRule.YYYY: frozenset((DataType.INT, DataType.STRING)),
    DerivationRule.MM: frozenset((DataType.INT, DataType.STRING)),
}
DERIVATION_RULE_FORMATS: dict[DerivationRule, str] = {
    DerivationRule.YYYYMMDD: "%Y%m%d",
    DerivationRule.YYYY: "%Y",
    DerivationRule.MM: "%m",
}


class DerivationPolicy:
    """Инкапсулирует правила построения производных колонок из DATE/TIMESTAMP источников."""
    @staticmethod
    def derive_output_constraints_from_source(source_column: TableColumnSpec) -> OutputConstraints:
        return OutputConstraints(
            null_ratio=source_column.output_constraints.null_ratio,
            is_unique=False,
        )

    @staticmethod
    def validate_derived_column_spec(
        column_name: str,
        source_column: TableColumnSpec,
        derivation: TableDerivationSpec,
        output_data_type: DataType,
    ) -> None:
        if source_column.is_derived:
            raise InvalidDerivationError(
                f"Derived column {column_name} cannot reference another derived column {source_column.name}"
            )
        if source_column.is_foreign_key:
            raise InvalidDerivationError(
                f"Derived column {column_name} cannot reference foreign key column {source_column.name}"
            )

        source_data_type = source_column.generator_data_type
        if source_data_type not in TEMPORAL_SOURCE_TYPES:
            raise InvalidDerivationError(
                f"Derived column {column_name} requires DATE or TIMESTAMP source column, got {source_data_type.value}"
            )

        allowed_output_types = DERIVATION_ALLOWED_OUTPUT_TYPES[derivation.rule]
        if output_data_type not in allowed_output_types:
            supported = ", ".join(sorted([allowed_type.value for allowed_type in allowed_output_types]))
            raise InvalidDerivationError(
                f"Derived column {column_name} does not support output_data_type={output_data_type.value}. "
                f"Supported: {supported}"
            )

    @staticmethod
    def get_derivation_date_format(
        derivation_rule: DerivationRule,
        column_name: str,
    ) -> str:
        date_format = DERIVATION_RULE_FORMATS.get(derivation_rule)
        if date_format is None:
            raise InvalidDerivationError(
                f"Unsupported derivation rule for column {column_name}: {derivation_rule.value}"
            )
        return date_format

    @staticmethod
    def cast_derived_output_value(
        rendered_value: str,
        target_type: DataType,
        column_name: str,
    ) -> NonNullColumnValue:
        if target_type == DataType.STRING:
            return rendered_value

        if target_type == DataType.INT:
            return int(rendered_value)

        raise InvalidDerivationError(
            f"Derived column {column_name} does not support output_data_type={target_type.value}"
        )

    def derive_output_value(
        self,
        source_value: NonNullColumnValue,
        date_format: str,
        target_type: DataType,
        column_name: str,
    ) -> NonNullColumnValue:
        if not isinstance(source_value, TEMPORAL_SOURCE_VALUE_TYPES):
            raise InvalidDerivationError(
                f"Derived column {column_name} requires DATE/TIMESTAMP source values, "
                f"got {type(source_value).__name__}"
            )

        rendered_value = source_value.strftime(date_format)
        return self.cast_derived_output_value(
            rendered_value=rendered_value,
            target_type=target_type,
            column_name=column_name,
        )

    def derive_output_values(
        self,
        table_column: TableColumnSpec,
        source_non_null_values: NonNullColumnValues,
    ) -> NonNullColumnValues:
        """Применяет правило derivation ко всему non-null набору исходных значений одной колонки."""
        derivation = table_column.derivation
        if derivation is None:
            raise InvalidDerivationError(f"Column {table_column.name} is not derived")

        date_format = self.get_derivation_date_format(
            derivation_rule=derivation.rule,
            column_name=table_column.name,
        )

        return [
            self.derive_output_value(
                source_value=source_value,
                date_format=date_format,
                target_type=table_column.output_data_type,
                column_name=table_column.name,
            )
            for source_value in source_non_null_values
        ]
