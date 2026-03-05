from typing import Any, Dict, List, Optional

from app.core.application.dto import TablePublication
from app.core.application.ports.publication_repository_port import IPublicationRepository
from app.core.application.ports.query_builder_port import IQueryBuilder
from app.core.domain.entities import GeneratedTableData


class PublicationService:
    def __init__(
        self,
        repository: IPublicationRepository,
        ddl_builders: Dict[str, IQueryBuilder],
        run_id: str,
    ):
        self.repository = repository
        self.ddl_builders = ddl_builders
        self.run_id = run_id

    def build_ddl_queries(self, table_data: GeneratedTableData) -> Dict[str, str]:
        return {
            table_format: builder.generate_ddl(table_data.table)
            for table_format, builder in self.ddl_builders.items()
        }

    def cleanup_run_artifacts(self, generated_tables: List[GeneratedTableData]) -> None:
        for table_data in generated_tables:
            table = table_data.table
            self.repository.cleanup_run_artifacts(
                schema_name=table.schema_name,
                table_name=table.table_name,
                run_id=self.run_id,
            )

    def publish_tables(self, generated_tables: List[GeneratedTableData]) -> List[TablePublication]:
        try:
            staged_publications = [
                self.repository.stage_artifacts(
                    table_data=table_data,
                    run_id=self.run_id,
                    ddl_queries=self.build_ddl_queries(table_data),
                ) for table_data in generated_tables
            ]
        except Exception:
            self.cleanup_run_artifacts(generated_tables=generated_tables)
            raise

        for publication in staged_publications:
            self.repository.commit_pointer(
                schema_name=publication.schema_name,
                table_name=publication.table_name,
                run_id=publication.run_id,
            )

        return staged_publications

    def read_latest_table_data(
        self,
        schema_name: str,
        table_name: str,
    ) -> Optional[Dict[str, Any]]:
        latest_success_run_id = self.repository.get_latest_run_id(
            schema_name=schema_name,
            table_name=table_name,
        )
        if not latest_success_run_id:
            return None

        return self.repository.read_table_data(
            schema_name=schema_name,
            table_name=table_name,
            run_id=latest_success_run_id,
        )
