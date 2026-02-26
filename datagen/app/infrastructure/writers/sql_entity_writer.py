from typing import Set

from app.core.application.ports.entity_writer_port import IEntityWriter
from app.core.application.ports.query_builder_port import IQueryBuilderService
from app.core.application.ports.repository_port import IMockRepository
from app.core.domain.entities import MockDataEntityResult


class SqlEntityWriter(IEntityWriter):
    def __init__(self, mock_repository: IMockRepository, ddl_query_builder: IQueryBuilderService):
        self.mock_repository = mock_repository
        self.ddl_query_builder = ddl_query_builder
        self.created_schema_names: Set[str] = set()

    def persist_entity_result(self, entity_result: MockDataEntityResult) -> None:
        schema_name = entity_result.entity.schema_name
        if schema_name not in self.created_schema_names:
            self.mock_repository.create_db_schema(schema_name)
            self.created_schema_names.add(schema_name)

        ddl_query = self.ddl_query_builder.create_ddl(entity_result.entity)
        self.mock_repository.create_as_table(
            ddl_query=ddl_query,
            full_table_name=entity_result.entity.full_table_name,
            generated_data=entity_result.generated_data,
        )
