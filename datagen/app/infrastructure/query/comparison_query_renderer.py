import re

from app.core.application.constants import EngineName
from app.core.application.dto.publication import EnginePair, TablePublication
from app.core.application.ports.comparison_query_renderer_port import ComparisonQueryRendererPort


class TargetTableComparisonQueryRenderer(ComparisonQueryRendererPort):
    STUB_QUERY = """
    SELECT
        cg.GROUP_ID_DM,
        cg.MAIN_COMPANY_FLG_DM,
        cg.VALUE_DAY,
        COUNT(s.IDSUBJECT) AS subject_count,
        COUNT(s.SOGRN) AS with_ogrn_count
    FROM analytics.company_groups cg
    JOIN analytics.subjects s ON cg.INN = s.SINN
    GROUP BY cg.GROUP_ID_DM, cg.MAIN_COMPANY_FLG_DM, cg.VALUE_DAY
    """

    @staticmethod
    def replace_table_name(query: str, logical_table_name: str, target_table_name: str) -> str:
        pattern = rf"(?<![A-Za-z0-9_]){re.escape(logical_table_name)}(?![A-Za-z0-9_])"
        return re.sub(pattern, target_table_name, query)

    def render_for_engine(self, publications: list[TablePublication], engine_name: EngineName) -> str:
        rendered_query = self.STUB_QUERY.strip()

        for publication in publications:
            logical_table_name = f"{publication.schema_name}.{publication.table_name}"
            rendered_query = self.replace_table_name(
                query=rendered_query,
                logical_table_name=logical_table_name,
                target_table_name=publication.artifacts.engines.get_value(engine_name).target_table_name,
            )
        return rendered_query

    def render(self, publications: list[TablePublication]) -> EnginePair[str]:
        return EnginePair(
            hive=self.render_for_engine(publications, EngineName.HIVE),
            iceberg=self.render_for_engine(publications, EngineName.ICEBERG),
        )
