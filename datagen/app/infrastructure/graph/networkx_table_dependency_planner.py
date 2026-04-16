import networkx as nx

from app.application.ports.table_dependency_planner_port import TableDependencyPlannerPort
from app.domain.entities import TableColumnSpec, TableSpec
from app.domain.validation_errors import InvalidReferenceError


class NetworkXTableDependencyPlanner(TableDependencyPlannerPort):
    """Строит порядок генерации таблиц по графу reference-зависимостей."""
    @staticmethod
    def can_skip_planning(tables: list[TableSpec]) -> bool:
        return len(tables) == 1 and all(column.reference is None for column in tables[0].columns)

    @staticmethod
    def collect_invalid_references(
        tables: list[TableSpec],
        table_by_name: dict[str, TableSpec],
        table_columns_map: dict[str, set[str]],
        table_column_specs: dict[str, dict[str, TableColumnSpec]],
        graph: nx.DiGraph,
    ) -> list[str]:
        """Проверяет reference-ссылки до сортировки и одновременно наполняет граф только валидными рёбрами."""
        invalid_references: list[str] = []

        for table in tables:
            graph.add_node(table)
            for column in table.columns:
                reference_info = column.reference
                if not reference_info:
                    continue

                ref_table_name = reference_info.table_name
                source_column_name = f"{table.table_name}.{column.name}"
                referenced_column_name = f"{ref_table_name}.{reference_info.column_name}"
                ref_table = table_by_name.get(ref_table_name)

                if not ref_table:
                    invalid_references.append(
                        f"{source_column_name} -> {referenced_column_name} "
                        f"(missing table '{ref_table_name}')"
                    )
                    continue

                if reference_info.column_name not in table_columns_map[ref_table_name]:
                    invalid_references.append(
                        f"{source_column_name} -> {referenced_column_name} "
                        f"(missing column '{reference_info.column_name}' in table '{ref_table_name}')"
                    )
                    continue

                ref_column = table_column_specs[ref_table_name][reference_info.column_name]
                if column.output_data_type != ref_column.output_data_type:
                    invalid_references.append(
                        f"{source_column_name} -> {referenced_column_name} "
                        f"(output type mismatch: {column.output_data_type.value} != "
                        f"{ref_column.output_data_type.value})"
                    )
                    continue

                graph.add_edge(ref_table, table)

        return invalid_references

    def plan(self, tables: list[TableSpec]) -> list[TableSpec]:
        """Возвращает topological order таблиц либо падает на некорректных reference-ссылках и циклах."""
        if self.can_skip_planning(tables):
            return tables

        table_by_name: dict[str, TableSpec] = {table.table_name: table for table in tables}
        table_columns_map = {
            table.table_name: {column.name for column in table.columns} for table in tables
        }
        table_column_specs = {
            table.table_name: {column.name: column for column in table.columns} for table in tables
        }
        graph = nx.DiGraph()
        invalid_references = self.collect_invalid_references(
            tables=tables,
            table_by_name=table_by_name,
            table_columns_map=table_columns_map,
            table_column_specs=table_column_specs,
            graph=graph,
        )

        if invalid_references:
            reference_list = "\n".join(f"- {error}" for error in invalid_references)
            raise InvalidReferenceError(f"Invalid references:\n{reference_list}")

        try:
            return list(nx.topological_sort(graph))
        except nx.NetworkXUnfeasible as exc:
            raise nx.NetworkXUnfeasible("Graph contains a cycle or graph changed during iteration") from exc
