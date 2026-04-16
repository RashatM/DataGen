from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from app.infrastructure.converters.contract.table_compiler import convert_to_generation_run
from app.infrastructure.errors import SchemaValidationError
from app.infrastructure.input.excel_loader_support import (
    ALLOWED_OUTPUT_TYPES_BY_SOURCE,
    DATA_SHEET_REQUIRED_FIELDS,
    WORKBOOK_DATA_SHEET_REQUIRED_FIELDS,
    EXCEL_SUFFIXES,
    SHEET_TABLES,
    SHEET_QUERIES,
    WORKBOOK_TABLES_HEADER_FIELDS,
    WORKBOOK_QUERIES_HEADER_FIELDS,
    WORKBOOK_QUERY_ROW_FIELDS,
    WORKBOOK_ROW_DATA_FIELDS,
    find_tables_header,
    find_required_header,
    format_issue,
    is_blank,
    normalize_constraints,
    normalize_derive_spec,
    normalize_engine_scope,
    normalize_generator_type,
    normalize_output_type,
    normalize_reference,
    normalize_sheet,
    normalize_text,
    normalize_write_mode,
    parse_csv_list,
    parse_key_value_text,
    prefix_issue,
    row_has_data,
    row_has_data_by_fields,
)

REFERENCE_ALLOWED_CONSTRAINT_FIELDS = {"null_ratio"}


@dataclass(frozen=True)
class TableMeta:
    """Метаданные legacy-листа таблицы, где sheet name строится как schema.table."""
    schema_name: str
    table_name: str
    total_rows: int

    @property
    def sheet_name(self) -> str:
        return f"{self.schema_name}.{self.table_name}"


class WorkbookValidationError(ValueError):
    """Ошибки валидации legacy workbook-формата с агрегацией нескольких проблем сразу."""

    def __init__(self, issues: list[str]):
        self.issues = issues
        super().__init__("\n".join(issues))


@dataclass(frozen=True)
class WorkbookTableMeta:
    """Метаданные таблицы из актуального workbook-формата с target tables и write mode."""
    table_name: str
    total_rows: int
    hive_target_table: str
    iceberg_target_table: str
    write_mode: str

    @property
    def sheet_name(self) -> str:
        return self.table_name


class WorkbookSpecValidationError(ValueError):
    """Ошибки валидации нового workbook-spec формата с накоплением всех найденных проблем."""

    def __init__(self, issues: list[str]):
        self.issues = issues
        super().__init__("\n".join(issues))


class ExcelToRawTablesConverter:
    """Читает legacy Excel workbook и преобразует его в список raw-table payload для schema converter."""
    def __init__(self, excel_path: str | Path) -> None:
        self._excel_path = Path(excel_path)

    def convert(self, validate_schema: bool = True) -> list[dict[str, Any]]:
        """Загружает workbook, валидирует его построчно и при необходимости прогоняет через schema converter."""
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
        """Читает лист Tables legacy-формата и строит список таблиц с уникальностью по table_name."""
        tables_df = self.read_sheet(xls, SHEET_TABLES, header=None)
        try:
            header_row, header_map = find_tables_header(tables_df)
        except ValueError as exc:
            raise WorkbookValidationError([format_issue(SHEET_TABLES, str(exc))]) from exc

        issues: list[str] = []
        table_specs: list[TableMeta] = []
        seen_table_names: set[str] = set()

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

            if table_name in seen_table_names:
                issues.append(
                    format_issue(
                        SHEET_TABLES,
                        f"duplicate table_name: {table_name}. schema_name is no longer part of table identity",
                        row=row_index + 1,
                    )
                )
                continue

            seen_table_names.add(table_name)
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
        """Собирает raw payload одной таблицы из её data sheet в legacy-формате."""
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
            "table_name": table_meta.table_name,
            "total_rows": table_meta.total_rows,
            "columns": columns,
        }

    @staticmethod
    def build_column_payload(table_meta: TableMeta, row: pd.Series) -> dict[str, Any]:
        """Преобразует одну строку листа таблицы в raw column payload legacy-контракта."""
        column_name = normalize_text(row.get("column_name"))
        if not column_name:
            raise ValueError("column_name must not be empty")

        raw_constraints = parse_key_value_text(row.get("constraints"), "constraints")
        reference = normalize_reference(parse_key_value_text(row.get("reference"), "reference"))

        if reference:
            if not is_blank(row.get("generator_data_type")):
                raise ValueError("reference column must not define gen_data_type")
            if not is_blank(row.get("output_data_type")):
                raise ValueError("reference column must not define output_data_type")
            unsupported_constraint_fields = sorted(set(raw_constraints.keys()) - REFERENCE_ALLOWED_CONSTRAINT_FIELDS)
            if unsupported_constraint_fields:
                unsupported_fields = ", ".join(unsupported_constraint_fields)
                raise ValueError(f"reference column supports only null_ratio constraint, got: {unsupported_fields}")
            return {
                "name": column_name,
                "reference": reference,
                "constraints": raw_constraints,
            }

        generator_type = normalize_generator_type(row.get("generator_data_type"))
        output_type = normalize_output_type(row.get("output_data_type"))
        if output_type and output_type not in ALLOWED_OUTPUT_TYPES_BY_SOURCE[generator_type]:
            raise ValueError(f"unsupported conversion for {column_name}: {generator_type} -> {output_type}")

        constraints = normalize_constraints(raw_constraints, generator_type)
        payload: dict[str, Any] = {
            "name": column_name,
            "generator_data_type": generator_type,
            "constraints": constraints,
        }
        if output_type and output_type != generator_type:
            payload["output_data_type"] = output_type
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


class ExcelWorkbookSpecConverter:
    """Читает актуальный workbook-формат с engine-specific target tables, scopes и comparison query."""
    def __init__(self, excel_path: str | Path) -> None:
        self._excel_path = Path(excel_path)

    def convert(self) -> dict[str, Any]:
        """Преобразует workbook в структуру {'tables': ..., 'queries': ...} для дальнейшего schema conversion."""
        try:
            xls = pd.ExcelFile(self._excel_path)
        except FileNotFoundError as exc:
            raise WorkbookSpecValidationError([f"workbook file not found: {self._excel_path}"]) from exc
        except ValueError as exc:
            raise WorkbookSpecValidationError([f"invalid Excel file: {self._excel_path}"]) from exc

        table_specs = self._read_table_specs(xls)
        queries = self._read_queries_spec(xls)
        issues: list[str] = []
        raw_tables: list[dict[str, Any]] = []

        for table_meta in table_specs:
            try:
                raw_tables.append(self.build_table_payload(xls, table_meta))
            except WorkbookSpecValidationError as exc:
                issues.extend(exc.issues)

        if issues:
            raise WorkbookSpecValidationError(issues)

        return {
            "tables": raw_tables,
            "queries": queries,
        }

    def _read_table_specs(self, xls: pd.ExcelFile) -> list[WorkbookTableMeta]:
        """Читает лист Tables нового формата и валидирует routing полей для Hive и Iceberg."""
        tables_df = self.read_sheet(xls, SHEET_TABLES, header=None)
        try:
            header_row, header_map = find_required_header(tables_df, WORKBOOK_TABLES_HEADER_FIELDS)
        except ValueError as exc:
            raise WorkbookSpecValidationError([format_issue(SHEET_TABLES, str(exc))]) from exc

        issues: list[str] = []
        table_specs: list[WorkbookTableMeta] = []
        seen_table_names: set[str] = set()

        for row_index in range(header_row + 1, len(tables_df.index)):
            table_name = normalize_text(tables_df.iat[row_index, header_map["table_name"]])
            total_rows_raw = tables_df.iat[row_index, header_map["total_rows"]]
            hive_target_table = normalize_text(tables_df.iat[row_index, header_map["hive_target_table"]])
            iceberg_target_table = normalize_text(tables_df.iat[row_index, header_map["iceberg_target_table"]])
            write_mode_raw = tables_df.iat[row_index, header_map["write_mode"]]

            if (
                not table_name and is_blank(total_rows_raw) and not hive_target_table and not iceberg_target_table
                and is_blank(write_mode_raw)
            ):
                continue

            if not table_name or is_blank(total_rows_raw) or not hive_target_table or not iceberg_target_table:
                issues.append(format_issue(SHEET_TABLES, "invalid table row", row=row_index + 1))
                continue

            try:
                total_rows = int(total_rows_raw)
            except (TypeError, ValueError):
                issues.append(format_issue(SHEET_TABLES, "total_rows must be integer", row=row_index + 1))
                continue

            if total_rows <= 0:
                issues.append(format_issue(SHEET_TABLES, "total_rows must be greater than 0", row=row_index + 1))
                continue

            try:
                write_mode = normalize_write_mode(write_mode_raw)
            except ValueError as exc:
                issues.append(format_issue(SHEET_TABLES, str(exc), row=row_index + 1))
                continue

            if table_name in seen_table_names:
                issues.append(format_issue(SHEET_TABLES, f"duplicate table_name: {table_name}", row=row_index + 1))
                continue

            seen_table_names.add(table_name)
            table_specs.append(
                WorkbookTableMeta(
                    table_name=table_name,
                    total_rows=total_rows,
                    hive_target_table=hive_target_table,
                    iceberg_target_table=iceberg_target_table,
                    write_mode=write_mode,
                )
            )

        if not table_specs and not issues:
            issues.append(format_issue(SHEET_TABLES, "no table metadata found"))

        if issues:
            raise WorkbookSpecValidationError(issues)

        return table_specs

    def _read_queries_spec(self, xls: pd.ExcelFile) -> dict[str, Any]:
        """Читает единственную строку comparison query и исключаемых колонок из листа Queries."""
        queries_df = self.read_sheet(xls, SHEET_QUERIES, header=None)
        try:
            header_row, header_map = find_required_header(queries_df, WORKBOOK_QUERIES_HEADER_FIELDS)
        except ValueError as exc:
            raise WorkbookSpecValidationError([format_issue(SHEET_QUERIES, str(exc))]) from exc

        issues: list[str] = []
        query_rows: list[dict[str, Any]] = []

        for row_index in range(header_row + 1, len(queries_df.index)):
            row_values = {
                field_name: queries_df.iat[row_index, header_map[field_name]]
                for field_name in WORKBOOK_QUERY_ROW_FIELDS
            }
            row = pd.Series(row_values)
            if not row_has_data_by_fields(row, WORKBOOK_QUERY_ROW_FIELDS):
                continue

            hive_sql = normalize_text(row.get("hive_sql"))
            iceberg_sql = normalize_text(row.get("iceberg_sql"))
            if not hive_sql or not iceberg_sql:
                issues.append(format_issue(SHEET_QUERIES, "hive_sql and iceberg_sql are required", row=row_index + 1))
                continue

            query_rows.append(
                {
                    "hive_sql": hive_sql,
                    "iceberg_sql": iceberg_sql,
                    "hive_exclude_columns": parse_csv_list(row.get("hive_exclude_columns")),
                    "iceberg_exclude_columns": parse_csv_list(row.get("iceberg_exclude_columns")),
                }
            )

        if not query_rows and not issues:
            issues.append(format_issue(SHEET_QUERIES, "queries sheet does not contain a query row"))

        if len(query_rows) > 1:
            issues.append(format_issue(SHEET_QUERIES, "queries sheet must contain exactly one query row"))

        if issues:
            raise WorkbookSpecValidationError(issues)

        return query_rows[0]

    def build_table_payload(self, xls: pd.ExcelFile, table_meta: WorkbookTableMeta) -> dict[str, Any]:
        """Собирает raw payload одной таблицы в новом workbook-формате с load spec полями."""
        sheet_df = normalize_sheet(self.read_sheet(xls, table_meta.sheet_name))
        missing_fields = sorted(WORKBOOK_DATA_SHEET_REQUIRED_FIELDS - set(sheet_df.columns))
        if missing_fields:
            raise WorkbookSpecValidationError(
                [
                    format_issue(
                        table_meta.sheet_name,
                        f"sheet is missing columns: {', '.join(missing_fields)}",
                        row=1,
                    )
                ]
            )

        columns: list[dict[str, Any]] = []
        issues: list[str] = []
        saw_user_rows = False

        for row_index, row in sheet_df.iterrows():
            if not row_has_data_by_fields(row, WORKBOOK_ROW_DATA_FIELDS):
                continue

            saw_user_rows = True
            excel_row = int(row_index) + 2

            try:
                columns.append(self.build_column_payload(row))
            except ValueError as exc:
                issues.append(format_issue(table_meta.sheet_name, str(exc), row=excel_row))

        if not columns and not saw_user_rows:
            issues.append(format_issue(table_meta.sheet_name, "sheet does not contain any columns"))

        if issues:
            raise WorkbookSpecValidationError(issues)

        return {
            "table_name": table_meta.table_name,
            "total_rows": table_meta.total_rows,
            "hive_target_table": table_meta.hive_target_table,
            "iceberg_target_table": table_meta.iceberg_target_table,
            "write_mode": table_meta.write_mode,
            "columns": columns,
        }

    @staticmethod
    def build_column_payload(row: pd.Series) -> dict[str, Any]:
        """Нормализует одну строку описания колонки, включая reference, derive и engine_scope."""
        column_name = normalize_text(row.get("column_name"))
        if not column_name:
            raise ValueError("column_name must not be empty")

        generator_type_raw = row.get("generator_data_type", row.get("gen_data_type"))
        output_type_raw = row.get("output_data_type")
        raw_constraints = parse_key_value_text(row.get("constraints"), "constraints")
        raw_reference = parse_key_value_text(row.get("reference"), "reference")
        raw_derive = parse_key_value_text(row.get("derive"), "derive")
        engine_scope = normalize_engine_scope(row.get("engine_scope"))

        reference = normalize_reference(raw_reference)
        derive = normalize_derive_spec(raw_derive)

        if reference and derive:
            raise ValueError("reference and derive cannot be set at the same time")

        if reference:
            if not is_blank(generator_type_raw):
                raise ValueError("reference column must not define gen_data_type")
            if not is_blank(output_type_raw):
                raise ValueError("reference column must not define output_data_type")

            unsupported_constraint_fields = sorted(set(raw_constraints.keys()) - REFERENCE_ALLOWED_CONSTRAINT_FIELDS)
            if unsupported_constraint_fields:
                unsupported_fields = ", ".join(unsupported_constraint_fields)
                raise ValueError(
                    f"reference column supports only null_ratio constraint, got: {unsupported_fields}"
                )

            return {
                "name": column_name,
                "reference": reference,
                "engine_scope": engine_scope,
                "constraints": raw_constraints,
            }

        if derive:
            if not is_blank(generator_type_raw):
                raise ValueError("derived column must not define gen_data_type")
            if is_blank(output_type_raw):
                raise ValueError("derived column must define output_data_type")
            if raw_constraints:
                raise ValueError("derived column must not define constraints")

            output_type = normalize_output_type(output_type_raw)
            if output_type is None:
                raise ValueError("derived column must define output_data_type")

            return {
                "name": column_name,
                "output_data_type": output_type,
                "engine_scope": engine_scope,
                "derive": derive,
            }

        generator_type = normalize_generator_type(generator_type_raw)
        output_type = normalize_output_type(output_type_raw)
        if output_type and output_type not in ALLOWED_OUTPUT_TYPES_BY_SOURCE[generator_type]:
            raise ValueError(f"unsupported conversion for {column_name}: {generator_type} -> {output_type}")

        constraints = normalize_constraints(raw_constraints, generator_type)

        payload: dict[str, Any] = {
            "name": column_name,
            "generator_data_type": generator_type,
            "constraints": constraints,
            "engine_scope": engine_scope,
        }
        if output_type and output_type != generator_type:
            payload["output_data_type"] = output_type
        return payload

    @staticmethod
    def read_sheet(
        xls: pd.ExcelFile,
        sheet_name: str,
        header: int | None = 0,
    ) -> pd.DataFrame:
        if sheet_name not in xls.sheet_names:
            raise WorkbookSpecValidationError([format_issue(sheet_name, "required sheet is missing")])
        return pd.read_excel(xls, sheet_name=sheet_name, header=header, dtype=object)


def load_raw_tables(
    raw_tables: list[dict[str, Any]] | None = None,
    input_path: str | Path | None = None,
    validate_schema: bool = True,
) -> list[dict[str, Any]]:
    """Собирает raw tables из одного файла или директории workbook-ов legacy-формата."""
    if raw_tables is not None:
        return raw_tables

    if input_path is None:
        raise ValueError("input_path is required when raw_tables is not provided")

    issues: list[str] = []
    all_raw_tables: list[dict[str, Any]] = []
    seen_tables: dict[str, str] = {}

    for excel_path in find_excel_files(Path(input_path)):
        try:
            workbook_tables = ExcelToRawTablesConverter(excel_path).convert(validate_schema=validate_schema)
        except WorkbookValidationError as exc:
            issues.extend([prefix_issue(excel_path.name, issue) for issue in exc.issues])
            continue

        for raw_table in workbook_tables:
            table_name = raw_table["table_name"]
            previous_workbook = seen_tables.get(table_name)
            if previous_workbook:
                issues.append(
                    prefix_issue(
                        excel_path.name,
                        format_issue(
                            table_name,
                            "duplicate table definition also found in workbook "
                            f"{previous_workbook}. schema_name is no longer part of table identity",
                        ),
                    )
                )
                continue

            seen_tables[table_name] = excel_path.name
            all_raw_tables.append(raw_table)

    if issues:
        raise WorkbookValidationError(issues)

    return all_raw_tables


def load_workbook_specs(
    input_path: str | Path,
) -> list[dict[str, Any]]:
    """Собирает новый workbook-spec формат из директории Excel-файлов с проверкой конфликтов по table_name."""
    issues: list[str] = []
    workbook_specs: list[dict[str, Any]] = []
    seen_tables: dict[str, str] = {}

    for excel_path in find_excel_files(Path(input_path)):
        try:
            workbook_spec = ExcelWorkbookSpecConverter(excel_path).convert()
        except WorkbookSpecValidationError as exc:
            issues.extend([prefix_issue(excel_path.name, issue) for issue in exc.issues])
            continue

        for raw_table in workbook_spec["tables"]:
            table_name = raw_table["table_name"]
            previous_workbook = seen_tables.get(table_name)
            if previous_workbook:
                issues.append(
                    prefix_issue(
                        excel_path.name,
                        format_issue(
                            table_name,
                            f"duplicate table definition also found in workbook {previous_workbook}",
                        ),
                    )
                )
                continue

            seen_tables[table_name] = excel_path.name

        workbook_specs.append(workbook_spec)

    if issues:
        raise WorkbookSpecValidationError(issues)

    return workbook_specs


def find_excel_files(input_path: Path) -> list[Path]:
    """Возвращает список Excel-файлов для обработки из файла или директории, исключая временные файлы Excel."""
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
