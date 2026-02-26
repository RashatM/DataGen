from typing import Dict, List
import networkx as nx

from app.core.application.ports.dependency_graph_builder_port import IDependencyGraphBuilder
from app.core.domain.entities import MockDataEntity
from app.core.domain.validation_errors import InvalidForeignKeyError


class NetworkXDependencyGraphBuilder(IDependencyGraphBuilder):
    @staticmethod
    def can_skip_graph_build(entities: List[MockDataEntity]) -> bool:
        return len(entities) == 1 and all(column.foreign_key is None for column in entities[0].columns)

    @staticmethod
    def collect_invalid_references(
        entities: List[MockDataEntity],
        entity_dict: Dict[str, MockDataEntity],
        entity_columns_map: Dict[str, set[str]],
        graph: nx.DiGraph,
    ) -> List[str]:
        invalid_references: List[str] = []

        for entity in entities:
            graph.add_node(entity)
            for column in entity.columns:
                fk_info = column.foreign_key
                if not fk_info:
                    continue

                ref_table_name = fk_info.full_table_name
                source_column_name = f"{entity.full_table_name}.{column.name}"
                referenced_column_name = f"{ref_table_name}.{fk_info.column_name}"
                ref_entity = entity_dict.get(ref_table_name)

                if not ref_entity:
                    invalid_references.append(
                        f"{source_column_name} -> {referenced_column_name} "
                        f"(missing table '{ref_table_name}')"
                    )
                    continue

                if fk_info.column_name not in entity_columns_map[ref_table_name]:
                    invalid_references.append(
                        f"{source_column_name} -> {referenced_column_name} "
                        f"(missing column '{fk_info.column_name}' in table '{ref_table_name}')"
                    )
                    continue

                graph.add_edge(ref_entity, entity)

        return invalid_references

    def build_graph(self, entities: List[MockDataEntity]) -> List[MockDataEntity]:
        if self.can_skip_graph_build(entities):
            return entities

        entity_dict: Dict[str, MockDataEntity] = {entity.full_table_name: entity for entity in entities}
        entity_columns_map = {
            entity.full_table_name: {column.name for column in entity.columns} for entity in entities
        }
        graph = nx.DiGraph()
        invalid_references = self.collect_invalid_references(
            entities=entities,
            entity_dict=entity_dict,
            entity_columns_map=entity_columns_map,
            graph=graph,
        )

        if invalid_references:
            reference_list = "\n".join(f"- {error}" for error in invalid_references)
            raise InvalidForeignKeyError(f"Invalid foreign key references:\n{reference_list}")

        try:
            return list(nx.topological_sort(graph))
        except nx.NetworkXUnfeasible as exc:
            raise nx.NetworkXUnfeasible("Graph contains a cycle or graph changed during iteration") from exc
