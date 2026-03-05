from typing import Any, Dict, List, Optional

from app.core.application.dto import TablePublication
from app.core.application.ports.publication_repository_port import IPublicationRepository
from app.core.application.ports.query_builder_port import IQueryBuilder
from app.core.domain.entities import MockDataEntityResult


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

    def build_ddl_queries(self, entity_result: MockDataEntityResult) -> Dict[str, str]:
        return {
            table_format: builder.generate_ddl(entity_result.entity)
            for table_format, builder in self.ddl_builders.items()
        }


    def cleanup_run_artifacts(self, entity_results: List[MockDataEntityResult]) -> None:
        for entity_result in entity_results:
            entity = entity_result.entity
            self.repository.cleanup_run_artifacts(
                schema_name=entity.schema_name,
                table_name=entity.table_name,
                run_id=self.run_id,
            )

    def publish_entities(self, entity_results: List[MockDataEntityResult]) -> List[TablePublication]:
        try:
            staged_publications = [
                self.repository.stage_artifacts(
                    entity_result=entity_result,
                    run_id=self.run_id,
                    ddl_queries=self.build_ddl_queries(entity_result)
                ) for entity_result in entity_results
            ]
        except Exception:
            self.cleanup_run_artifacts(entity_results=entity_results)
            raise

        for publication in staged_publications:
            self.repository.commit_pointer(
                schema_name=publication.schema_name,
                table_name=publication.table_name,
                run_id=publication.run_id,
            )

        return staged_publications

    def read_latest_entity_data(
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

        return self.repository.read_entity_data(
            schema_name=schema_name,
            table_name=table_name,
            run_id=latest_success_run_id,
        )
