from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from app.infrastructure.converters.schema_converter import convert_to_generation_run
from app.infrastructure.errors import SchemaValidationError
from app.infrastructure.input.excel_loader_support import (
    ALLOWED_OUTPUT_TYPES_BY_SOURCE,
    DATA_SHEET_REQUIRED_FIELDS,
    EXCEL_SUFFIXES,
    SHEET_TABLES,
    find_tables_header,
    format_issue,
    is_blank,
    normalize_constraints,
    normalize_foreign_key,
    normalize_generator_type,
    normalize_output_type,
    normalize_sheet,
    normalize_text,
    parse_key_value_text,
    parse_optional_bool,
    prefix_issue,
    row_has_data,
)


@dataclass(frozen=True)
class TableMeta:
    schema_name: str
    table_name: str
    total_rows: int

    @property
    def sheet_name(self) -> str:
        return f"{self.schema_name}.{self.table_name}"


class WorkbookValidationError(ValueError):

    def __init__(self, issues: list[str]):
        self.issues = issues
        super().__init__("\n".join(issues))


class ExcelToRawTablesConverter:
    def __init__(self, excel_path: str | Path) -> None:
        self._excel_path = Path(excel_path)

    def convert(self, validate_schema: bool = True) -> list[dict[str, Any]]:
        try:
            xls = pd.ExcelFile(self._excel_path)
        except FileNotFoundError as exc:
            raise WorkbookValidationError([f"workbook file not found: {self._excel_path}"]) from exc
        except ValueError as exc:
            raise WorkbookValidationError([f"invalid Excel file: {self._excel_path}"]) from exc

        table_specs = self._read_table_specs(xls)
        issues: list[str] = []
        raw_tables: list[dict[str, Any]] = []

        for table_meta in table_specs:
            try:
                raw_tables.append(self.build_table_payload(xls, table_meta))
            except WorkbookValidationError as exc:
                issues.extend(exc.issues)

        if issues:
            raise WorkbookValidationError(issues)

        if validate_schema:
            try:
                convert_to_generation_run(run_id="excel_import_validation", raw_tables=raw_tables)
            except (SchemaValidationError, ValueError) as exc:
                raise WorkbookValidationError([str(exc)]) from exc

        return raw_tables

    def _read_table_specs(self, xls: pd.ExcelFile) -> list[TableMeta]:
        tables_df = self.read_sheet(xls, SHEET_TABLES, header=None)
        try:
            header_row, header_map = find_tables_header(tables_df)
        except ValueError as exc:
            raise WorkbookValidationError([format_issue(SHEET_TABLES, str(exc))]) from exc

        issues: list[str] = []
        table_specs: list[TableMeta] = []

        for row_index in range(header_row + 1, len(tables_df.index)):
            schema_name = normalize_text(tables_df.iat[row_index, header_map["schema_name"]])
            table_name = normalize_text(tables_df.iat[row_index, header_map["table_name"]])
            total_rows_raw = tables_df.iat[row_index, header_map["total_rows"]]

            if not schema_name and not table_name and is_blank(total_rows_raw):
                continue

            if not schema_name or not table_name or is_blank(total_rows_raw):
                issues.append(format_issue(SHEET_TABLES, "invalid table row", row=row_index + 1))
                continue

            try:
                total_rows = int(total_rows_raw)
            except (TypeError, ValueError):
                issues.append(format_issue(SHEET_TABLES, "total_rows must be integer", row=row_index + 1))
                continue

            table_specs.append(
                TableMeta(
                    schema_name=schema_name,
                    table_name=table_name,
                    total_rows=total_rows,
                )
            )

        if not table_specs and not issues:
            issues.append(format_issue(SHEET_TABLES, "no table metadata found"))

        if issues:
            raise WorkbookValidationError(issues)

        return table_specs

    def build_table_payload(self, xls: pd.ExcelFile, table_meta: TableMeta) -> dict[str, Any]:
        sheet_df = normalize_sheet(self.read_sheet(xls, table_meta.sheet_name))
        missing_fields = sorted(DATA_SHEET_REQUIRED_FIELDS - set(sheet_df.columns))
        if missing_fields:
            raise WorkbookValidationError(
                [format_issue(table_meta.sheet_name, f"sheet is missing columns: {', '.join(missing_fields)}", row=1)]
            )

        columns: list[dict[str, Any]] = []
        issues: list[str] = []
        saw_user_rows = False

        for row_index, row in sheet_df.iterrows():
            if not row_has_data(row):
                continue

            saw_user_rows = True
            excel_row = int(row_index) + 2

            try:
                columns.append(self.build_column_payload(table_meta, row))
            except ValueError as exc:
                issues.append(format_issue(table_meta.sheet_name, str(exc), row=excel_row))

        if not columns and not saw_user_rows:
            issues.append(format_issue(table_meta.sheet_name, "sheet does not contain any columns"))

        if issues:
            raise WorkbookValidationError(issues)

        return {
            "schema_name": table_meta.schema_name,
            "table_name": table_meta.table_name,
            "total_rows": table_meta.total_rows,
            "columns": columns,
        }

    @staticmethod
    def build_column_payload(table_meta: TableMeta, row: pd.Series) -> dict[str, Any]:
        column_name = normalize_text(row.get("column_name"))
        if not column_name:
            raise ValueError("column_name must not be empty")

        generator_type = normalize_generator_type(row.get("generator_data_type"))
        output_type = normalize_output_type(row.get("output_data_type"))
        if output_type and output_type not in ALLOWED_OUTPUT_TYPES_BY_SOURCE[generator_type]:
            raise ValueError(f"unsupported conversion for {column_name}: {generator_type} -> {output_type}")

        is_primary_key = parse_optional_bool(row.get("is_primary_key"), "is_primary_key") or False
        constraints = normalize_constraints(
            parse_key_value_text(row.get("constraints"), "constraints"),
            generator_type,
        )
        foreign_key = normalize_foreign_key(
            parse_key_value_text(row.get("foreign_key"), "foreign_key"),
            current_schema_name=table_meta.schema_name,
        )

        payload: dict[str, Any] = {
            "name": column_name,
            "generator_data_type": generator_type,
            "constraints": constraints,
        }
        if output_type and output_type != generator_type:
            payload["output_data_type"] = output_type
        if is_primary_key:
            payload["is_primary_key"] = True
        if foreign_key:
            payload["foreign_key"] = foreign_key
        return payload

    @staticmethod
    def read_sheet(
        xls: pd.ExcelFile,
        sheet_name: str,
        header: int | None = 0,
    ) -> pd.DataFrame:
        if sheet_name not in xls.sheet_names:
            raise WorkbookValidationError([format_issue(sheet_name, "required sheet is missing")])
        return pd.read_excel(xls, sheet_name=sheet_name, header=header, dtype=object)


def load_raw_tables(
    raw_tables: list[dict[str, Any]] | None = None,
    input_path: str | Path | None = None,
    validate_schema: bool = True,
) -> list[dict[str, Any]]:
    if raw_tables is not None:
        return raw_tables

    if input_path is None:
        raise ValueError("input_path is required when raw_tables is not provided")

    issues: list[str] = []
    all_raw_tables: list[dict[str, Any]] = []
    seen_tables: dict[tuple[str, str], str] = {}

    for excel_path in find_excel_files(Path(input_path)):
        try:
            workbook_tables = ExcelToRawTablesConverter(excel_path).convert(validate_schema=validate_schema)
        except WorkbookValidationError as exc:
            issues.extend([prefix_issue(excel_path.name, issue) for issue in exc.issues])
            continue

        for raw_table in workbook_tables:
            table_key = (raw_table["schema_name"], raw_table["table_name"])
            previous_workbook = seen_tables.get(table_key)
            if previous_workbook:
                issues.append(
                    prefix_issue(
                        excel_path.name,
                        format_issue(
                            f"{raw_table['schema_name']}.{raw_table['table_name']}",
                            f"duplicate table definition also found in workbook {previous_workbook}",
                        ),
                    )
                )
                continue

            seen_tables[table_key] = excel_path.name
            all_raw_tables.append(raw_table)

    if issues:
        raise WorkbookValidationError(issues)

    return all_raw_tables


def find_excel_files(input_path: Path) -> list[Path]:
    if input_path.is_dir():
        excel_files = sorted(
            path for path in input_path.iterdir()
            if path.is_file() and path.suffix.lower() in EXCEL_SUFFIXES and not path.name.startswith("~$")
        )
        if excel_files:
            return excel_files
        raise WorkbookValidationError([f"no Excel files found in directory: {input_path}"])

    if input_path.suffix.lower() not in EXCEL_SUFFIXES:
        raise WorkbookValidationError([f"unsupported input path: {input_path}"])
    return [input_path]
