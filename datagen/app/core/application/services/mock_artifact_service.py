from typing import Dict, List, Optional, Any

from app.core.application.dto import MockEntityArtifacts
from app.core.application.ports.artifact_repository_port import IArtifactRepository
from app.core.application.ports.query_builder_port import IQueryBuilder
from app.core.application.ports.run_state_repository_port import IRunStateRepository
from app.core.domain.entities import MockDataEntityResult


class MockArtifactService:

    def __init__(
        self,
        artifact_repository: IArtifactRepository,
        run_state_repository: IRunStateRepository,
        ddl_builders: Dict[str, IQueryBuilder],
    ):
        self.artifact_repository = artifact_repository
        self.run_state_repository = run_state_repository
        self.ddl_builders = ddl_builders

    def persist(
        self,
        entity_result: MockDataEntityResult,
        run_id: str,
    ) -> MockEntityArtifacts:
        entity = entity_result.entity

        # 1. save data
        data_uri = self.artifact_repository.save_entity_data(
            schema_name=entity.schema_name,
            table_name=entity.table_name,
            run_id=run_id,
            records=entity_result.generated_data,
        )

        # 2. build and save ddl
        for target, builder in self.ddl_builders.items():
            ddl_query = builder.create_ddl(entity)

            self.artifact_repository.save_entity_ddl(
                schema_name=entity.schema_name,
                table_name=entity.table_name,
                run_id=run_id,
                table_format=target,
                ddl_query=ddl_query,
            )

        # 3. update run state
        pointer_uri = self.run_state_repository.save_latest_run_pointer(
            schema_name=entity.schema_name,
            table_name=entity.table_name,
            run_id=run_id,
        )

        return MockEntityArtifacts(
            data_uri=data_uri,
            pointer_uri=pointer_uri,
        )

    def read_latest_entity_data(
        self,
        schema_name: str,
        table_name: str,
    ) -> Optional[Dict[str, List[Any]]]:
        latest_run_id = self.run_state_repository.get_latest_run_id(
            schema_name=schema_name,
            table_name=table_name,
        )
        if not latest_run_id:
            return None

        return self.artifact_repository.read_entity_data(
            schema_name=schema_name,
            table_name=table_name,
            run_id=latest_run_id,
        )
