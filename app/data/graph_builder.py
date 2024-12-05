from typing import List, Dict
import networkx as nx


from app.dto.entities import MockEntity
from app.interfaces.graph_builder import IDependencyGraphBuilder


class DependencyGraphBuilder(IDependencyGraphBuilder):

    def build_graph(self, entities: List[MockEntity]) -> List[MockEntity]:
        entity_dict: Dict[str, MockEntity] = {entity.table_name: entity for entity in entities}
        graph = nx.DiGraph()

        for entity in entities:
            graph.add_node(entity)
            for column in entity.columns:
                if column.foreign_key:
                    ref_entity = entity_dict.get(column.foreign_key.table_name)
                    if ref_entity:
                        graph.add_edge(ref_entity, entity)

        if not nx.is_directed_acyclic_graph(graph):
            raise nx.NetworkXUnfeasible("Graph contains a cycle or graph changed during iteration")

        return list(nx.topological_sort(graph))