from typing import Any, Dict, List, Optional

from app.core.application.dto import TablePublication
from app.core.application.ports.publication_repository_port import IPublicationRepository
from app.core.application.ports.query_builder_port import IQueryBuilder
from app.core.domain.entities import GeneratedTableData
from app.shared.logger import get_logger

logger = get_logger("datagen.publication")


class PublicationService:
    def __init__(
            self,
            repository: IPublicationRepository,
            ddl_builders: Dict[str, IQueryBuilder],
    ):
        self.repository = repository
        self.ddl_builders = ddl_builders

    def build_table_ddl_queries(self, table_data: GeneratedTableData) -> Dict[str, str]:
        return {
            engine_name: builder.generate_table_ddl(table_data.table)
            for engine_name, builder in self.ddl_builders.items()
        }

    def cleanup_run_artifacts(self, run_id: str) -> None:
        self.repository.cleanup_run_artifacts(run_id=run_id)

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
                ddl_queries=self.build_table_ddl_queries(table_data),
            )
            table_publications.append(table_publication)
            table = table_data.table
            logger.info(
                f"Artifacts uploaded. table={table.full_table_name}, rows={table.total_rows}, columns={len(table.columns)}"
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
            logger.exception(f"Artifact upload failed. run_id={run_id}")
            self.cleanup_run_artifacts(run_id=run_id)
            raise

        for publication in table_publications:
            self.repository.commit_pointer(
                schema_name=publication.schema_name,
                table_name=publication.table_name,
                run_id=publication.run_id,
            )
            logger.info(f"Pointer updated. table={publication.schema_name}.{publication.table_name}, run_id={publication.run_id}")

        return table_publications

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
