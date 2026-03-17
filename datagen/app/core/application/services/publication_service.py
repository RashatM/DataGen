from typing import Any, Dict, List, Optional

from app.core.application.dto import EngineLoadPayload, TablePublication
from app.core.application.ports.publication_repository_port import IArtifactPublicationRepository
from app.core.application.ports.query_builder_port import IQueryBuilder
from app.core.domain.entities import GeneratedTableData
from app.shared.logger import publication_logger

logger = publication_logger


class ArtifactPublicationService:
    def __init__(
            self,
            repository: IArtifactPublicationRepository,
            query_builders: Dict[str, IQueryBuilder],
    ):
        self.repository = repository
        self.query_builders = query_builders

    def build_engine_load_payloads(self, table_data: GeneratedTableData) -> Dict[str, EngineLoadPayload]:
        table = table_data.table
        payloads = {}
        for engine_name, builder in self.query_builders.items():
            target_table_name = builder.build_target_table_name(table)
            ddl_query = builder.generate_table_ddl(table, target_table_name)
            payloads[engine_name] = EngineLoadPayload(
                ddl_query=ddl_query,
                target_table_name=target_table_name,
            )
        return payloads

    def cleanup_run_artifacts(self, run_id: str) -> None:
        logger.info(f"Artifact cleanup started: run_id={run_id}")
        self.repository.cleanup_run_artifacts(run_id=run_id)
        logger.info(f"Artifact cleanup completed: run_id={run_id}")

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

    def stage_tables(
            self,
            run_id: str,
            generated_tables: List[GeneratedTableData],
    ) -> List[TablePublication]:
        table_publications = []

        for table_data in generated_tables:
            table_publication = self.repository.stage_table_artifacts(
                table_data=table_data,
                run_id=run_id,
                engine_load_payloads=self.build_engine_load_payloads(table_data),
            )
            table_publications.append(table_publication)
            table = table_data.table
            logger.info(
                f"Artifacts uploaded: table={table.full_table_name}, rows={table.total_rows}, columns={len(table.columns)}"
            )

        return table_publications

    def publish(
            self,
            run_id: str,
            generated_tables: List[GeneratedTableData],
    ) -> List[TablePublication]:
        try:
            table_publications = self.stage_tables(run_id, generated_tables)
        except Exception:
            logger.exception(f"Artifact upload failed: run_id={run_id}")
            self.cleanup_run_artifacts(run_id=run_id)
            raise

        for publication in table_publications:
            self.repository.commit_pointer(
                schema_name=publication.schema_name,
                table_name=publication.table_name,
                run_id=publication.run_id,
            )
            logger.info(f"Pointer updated: table={publication.schema_name}.{publication.table_name}, run_id={publication.run_id}")

        return table_publications
