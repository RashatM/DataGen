import re
from typing import Dict, List

from app.core.application.dto.publication import TablePublication
from app.core.application.ports.comparison_query_renderer_port import ComparisonQueryRendererPort


class TargetTableComparisonQueryRenderer(ComparisonQueryRendererPort):
    STUB_QUERY = """
    SELECT
        analytics.company_groups.id,
        analytics.subjects.id
    FROM analytics.company_groups
    JOIN analytics.subjects ON analytics.company_groups.id = analytics.subjects.group_id
    """

    @staticmethod
    def replace_table_name(query: str, logical_table_name: str, target_table_name: str) -> str:
        pattern = rf"(?<![A-Za-z0-9_]){re.escape(logical_table_name)}(?![A-Za-z0-9_])"
        return re.sub(pattern, target_table_name, query)

    @staticmethod
    def collect_engine_names(publications: List[TablePublication]) -> List[str]:
        if not publications:
            return []

        engine_names = set(publications[0].artifacts.engines)
        for publication in publications[1:]:
            current_engine_names = set(publication.artifacts.engines)
            if current_engine_names != engine_names:
                raise ValueError(
                    "Table publications must expose the same engine set for comparison query rendering"
                )
        return sorted(engine_names)

    def render_for_engine(self, engine_name: str, publications: List[TablePublication]) -> str:
        rendered_query = self.STUB_QUERY.strip()
        for publication in publications:
            logical_table_name = f"{publication.schema_name}.{publication.table_name}"
            target_table_name = publication.artifacts.engines[engine_name].target_table_name
            rendered_query = self.replace_table_name(
                query=rendered_query,
                logical_table_name=logical_table_name,
                target_table_name=target_table_name,
            )
        return rendered_query

    def render(self, publications: List[TablePublication]) -> Dict[str, str]:
        rendered_queries: Dict[str, str] = {}
        for engine_name in self.collect_engine_names(publications):
            rendered_queries[engine_name] = self.render_for_engine(engine_name, publications)
        return rendered_queries
