import json
from typing import Any, Dict

import pandas as pd
import warnings


def parse_constraint_value(value: str) -> Any:
    if value.isdigit():
        return int(value)
    if value.count('.') == 1 and not value.isalnum():
        return float(value)
    return value.strip('"')


def convert_excel_to_json(file_path: str) -> Dict:
    warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
    excel_data = pd.ExcelFile(file_path)
    tables_df = pd.read_excel(file_path, sheet_name='tables')
    source_type = tables_df.iloc[0, 0]
    source_name = tables_df.iloc[0, 1]

    tables_cleaned = tables_df.iloc[5:, 0:3].dropna(how='all').reset_index(drop=True)
    tables_cleaned.columns = ['schema_name', 'table_name', 'total_rows']
    tables_cleaned['total_rows'] = pd.to_numeric(tables_cleaned['total_rows'], errors='coerce').fillna(0).astype(int)

    columns_info = {}
    for sheet_name in excel_data.sheet_names[2:]:  # Skip "tables" and example/test schema
        sheet_df = pd.read_excel(file_path, sheet_name=sheet_name)
        sheet_df = (
            sheet_df
            .drop(columns=['Constraints Info Help', 'foreign_key Info Help'], errors='ignore')
            .dropna(how='all')
        )
        columns_info[sheet_name] = sheet_df

    output_json = {
        "source_type": source_type,
        "source_name": source_name,
        "entities": []
    }

    for _, table_row in tables_cleaned.iterrows():
        schema_name, table_name, total_rows = table_row
        key = f"{schema_name}.{table_name}"
        if key in columns_info:
            columns = []
            for _, col_row in columns_info[key].iterrows():
                column = {
                    "name": col_row['column_name'].strip().lower(),
                    "data_type": col_row['data_type'].strip().lower()
                }

                if pd.notna(col_row["is_primary_key"]):
                    column["is_primary_key"] = str(col_row["is_primary_key"]).lower().strip()

                if pd.notna(col_row["foreign_key"]):
                    foreign_key = {}
                    for fk_data in col_row["foreign_key"].split(";"):
                        fk_name, fk_value = fk_data.split("=")
                        foreign_key[fk_name.strip().strip('"').lower()] = fk_value.strip().strip('"').lower()
                    column["foreign_key"] = foreign_key

                if pd.notna(col_row["constraints"]):
                    constraints = {}
                    for constraint_data in col_row["constraints"].split(";"):
                        constraint_name, constraint_value = constraint_data.split("=")
                        constraint_name = constraint_name.strip()
                        if constraint_name.lower() == "allowed_values":
                            constraint_value = json.loads(constraint_value)
                        else:
                            constraint_value = parse_constraint_value(constraint_value)
                        constraints[constraint_name] = constraint_value
                    column["constraints"] = constraints

                columns.append(column)
                entity = {
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "columns": columns,
                    "total_rows": total_rows
                }
            output_json["entities"].append(entity)
    return output_json
