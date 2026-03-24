import networkx as nx

from app.core.application.ports.table_dependency_planner_port import TableDependencyPlannerPort
from app.core.domain.entities import TableColumnSpec, TableSpec
from app.core.domain.enums import RelationType
from app.core.domain.validation_errors import InvalidForeignKeyError


class NetworkXTableDependencyPlanner(TableDependencyPlannerPort):
    @staticmethod
    def can_skip_planning(tables: list[TableSpec]) -> bool:
        return len(tables) == 1 and all(column.foreign_key is None for column in tables[0].columns)

    @staticmethod
    def collect_invalid_references(
        tables: list[TableSpec],
        table_by_name: dict[str, TableSpec],
        table_columns_map: dict[str, set[str]],
        table_column_specs: dict[str, dict[str, TableColumnSpec]],
        graph: nx.DiGraph,
    ) -> list[str]:
        invalid_references: list[str] = []

        for table in tables:
            graph.add_node(table)
            for column in table.columns:
                fk_info = column.foreign_key
                if not fk_info:
                    continue

                ref_table_name = fk_info.full_table_name
                source_column_name = f"{table.full_table_name}.{column.name}"
                referenced_column_name = f"{ref_table_name}.{fk_info.column_name}"
                ref_table = table_by_name.get(ref_table_name)

                if not ref_table:
                    invalid_references.append(
                        f"{source_column_name} -> {referenced_column_name} "
                        f"(missing table '{ref_table_name}')"
                    )
                    continue

                if fk_info.column_name not in table_columns_map[ref_table_name]:
                    invalid_references.append(
                        f"{source_column_name} -> {referenced_column_name} "
                        f"(missing column '{fk_info.column_name}' in table '{ref_table_name}')"
                    )
                    continue

                ref_column = table_column_specs[ref_table_name][fk_info.column_name]
                if not getattr(ref_column, "is_primary_key", False) and not getattr(
                    ref_column.output_constraints, "is_unique", False
                ):
                    invalid_references.append(
                        f"{source_column_name} -> {referenced_column_name} "
                        f"(referenced column must be unique or primary key)"
                    )
                    continue

                if ref_column.output_constraints.null_ratio != 0:
                    invalid_references.append(
                        f"{source_column_name} -> {referenced_column_name} "
                        f"(referenced column must be non-nullable)"
                    )
                    continue

                if column.output_data_type != ref_column.output_data_type:
                    invalid_references.append(
                        f"{source_column_name} -> {referenced_column_name} "
                        f"(output type mismatch: {column.output_data_type.value} != "
                        f"{ref_column.output_data_type.value})"
                    )
                    continue

                if fk_info.relation_type == RelationType.ONE_TO_ONE:
                    non_null_child_rows = table.total_rows - int(
                        table.total_rows * (column.output_constraints.null_ratio / 100)
                    )
                    if non_null_child_rows > ref_table.total_rows:
                        invalid_references.append(
                            f"{source_column_name} -> {referenced_column_name} "
                            f"(one-to-one requires child non-null rows <= parent rows: "
                            f"{non_null_child_rows} > {ref_table.total_rows})"
                        )
                        continue

                graph.add_edge(ref_table, table)

        return invalid_references

    def plan(self, tables: list[TableSpec]) -> list[TableSpec]:
        if self.can_skip_planning(tables):
            return tables

        table_by_name: dict[str, TableSpec] = {table.full_table_name: table for table in tables}
        table_columns_map = {
            table.full_table_name: {column.name for column in table.columns} for table in tables
        }
        table_column_specs = {
            table.full_table_name: {column.name: column for column in table.columns} for table in tables
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
            raise InvalidForeignKeyError(f"Invalid foreign key references:\n{reference_list}")

        try:
            return list(nx.topological_sort(graph))
        except nx.NetworkXUnfeasible as exc:
            raise nx.NetworkXUnfeasible("Graph contains a cycle or graph changed during iteration") from exc
