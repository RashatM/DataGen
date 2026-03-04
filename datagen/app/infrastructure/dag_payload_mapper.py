from typing import Any, Dict, List

from app.core.application.dto import TablePublication


class DagPayloadMapper:
    @staticmethod
    def build_payload(
        run_id: str,
        table_publications: List[TablePublication],
    ) -> Dict[str, Any]:
        tables: List[Dict[str, Any]] = []
        for table_publication in table_publications:
            tables.append(
                {
                    "schema_name": table_publication.schema_name,
                    "table_name": table_publication.table_name,
                    "storage_type": table_publication.storage_type,
                    "storage": table_publication.storage,
                }
            )

        return {
            "contract_version": "3",
            "run_id": run_id,
            "tables": tables,
        }
