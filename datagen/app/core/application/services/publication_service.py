from typing import Any, Dict, Optional

from app.core.application.dto import TablePublication
from app.core.application.ports.publication_repository_port import (
    IPublicationRepository,
)
from app.core.application.ports.query_builder_port import IQueryBuilder
from app.core.domain.entities import MockDataEntityResult


class PublicationService:
    def __init__(
        self,
        repository: IPublicationRepository,
        ddl_builders: Dict[str, IQueryBuilder],
    ):
        self.repository = repository
        self.ddl_builders = ddl_builders

    def publish(
        self,
        entity_result: MockDataEntityResult,
        run_id: str,
    ) -> TablePublication:
        entity = entity_result.entity

        ddl_map: Dict[str, str] = {}
        for target, builder in self.ddl_builders.items():
            ddl_map[target] = builder.create_ddl(entity)

        return self.repository.publish(
            entity_result=entity_result,
            run_id=run_id,
            ddl_map=ddl_map,
        )

    def read_latest_entity_data(
        self,
        schema_name: str,
        table_name: str,
    ) -> Optional[Dict[str, Any]]:
        run_id = self.repository.get_latest_run_id(
            schema_name=schema_name,
            table_name=table_name,
        )
        if not run_id:
            return None

        return self.repository.read_entity_data(
            schema_name=schema_name,
            table_name=table_name,
            run_id=run_id,
        )
