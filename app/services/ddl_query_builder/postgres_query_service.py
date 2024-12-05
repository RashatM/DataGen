from app.dto.entities import MockEntity, MockColumn
from app.enums import DataType
from app.interfaces.ddl_query_service import IQueryBuilderService


class PostgresQueryBuilderService(IQueryBuilderService):
    default_type_mapping = {
        DataType.BOOLEAN: "BOOLEAN",
        DataType.DATE: "DATE",
        DataType.TIMESTAMP: "TIMESTAMP",
        DataType.INT: "INT",
        DataType.FLOAT: "REAL"
    }

    def map_column_type(self, column: MockColumn):
        if column.data_type == DataType.STRING:
            length = getattr(column.constraints, 'length', 0)
            return f"VARCHAR({length})" if length and length <= 255 else "TEXT"
        elif column.data_type == DataType.INT:
            return "INTEGER"
        elif column.data_type == DataType.FLOAT:
            return "REAL"
        else:
            raise ValueError(f"Unsupported data type: {column.data_type}")

    def create_ddl(self, entity: MockEntity) -> str:
        columns_sql = []
        for column in entity.columns:
            column_type = self.map_column_type(column)
            columns_sql.append(f"{column.name} {column_type} NULL")

        columns_definition = ",\n  ".join(columns_sql)
        return f"CREATE TABLE {entity.table_name} (\n  {columns_definition}\n);"

