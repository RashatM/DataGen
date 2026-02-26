from typing import Dict, List

import networkx as nx

from app.core.application.ports.dependency_graph_builder_port import IDependencyGraphBuilder
from app.core.domain.entities import MockDataEntity


class NetworkXDependencyGraphBuilder(IDependencyGraphBuilder):
    def build_graph(self, entities: List[MockDataEntity]) -> List[MockDataEntity]:
        entity_dict: Dict[str, MockDataEntity] = {entity.full_table_name: entity for entity in entities}
        graph = nx.DiGraph()

        for entity in entities:
            graph.add_node(entity)
            for column in entity.columns:
                if column.foreign_key:
                    ref_entity = entity_dict.get(column.foreign_key.full_table_name)
                    if ref_entity:
                        graph.add_edge(ref_entity, entity)

        if not nx.is_directed_acyclic_graph(graph):
            raise nx.NetworkXUnfeasible("Graph contains a cycle or graph changed during iteration")

        return list(nx.topological_sort(graph))
