import random
from typing import Any, List


from app.dto.mock_data import MockDataEntity, MockDataColumn, MockDataEntityResult
from app.enums import RelationType
from app.interfaces.graph_builder import IDependencyGraphBuilder
from app.interfaces.mock_service import IMockDataService
from app.interfaces.mock_factory import IMockFactory
from app.utils import shuffle_values_with_nulls


class MockDataService(IMockDataService):

    def __init__(self, dependency_order_builder: IDependencyGraphBuilder, mock_factory: IMockFactory):
        self.dependency_order_builder = dependency_order_builder
        self.mock_factory = mock_factory

    def generate_column_values(self, total_rows: int, entity_column: MockDataColumn) -> List[Any]:
        total_nulls = int(total_rows * (entity_column.constraints.null_ratio / 100))
        total_non_nulls = total_rows - total_nulls

        values = self.mock_factory.get(entity_column.data_type).generate_values(
            total_rows=total_non_nulls,
            constraints=entity_column.constraints
        )

        if entity_column.constraints.null_ratio > 0:
            values = shuffle_values_with_nulls(target_count=total_nulls, values=values)

        return values

    def generate_entity_values(self, entities: List[MockDataEntity]) -> List[MockDataEntityResult]:
        entity_order_list = self.dependency_order_builder.build_graph(entities) if len(entities) > 1 else entities

        generated_entity_data = {}
        mock_results = []

        for entity in entity_order_list:
            generated_column_data = {}

            for entity_column in entity.columns:
                fk_info = entity_column.foreign_key
                if fk_info:
                    fk_data = generated_entity_data[fk_info.table_name][fk_info.column_name]

                    if fk_info.relation_type == RelationType.MANY_TO_ONE:
                        generated_column_data[entity_column.name] = random.choices(fk_data, k=entity.total_rows)
                    elif fk_info.relation_type == RelationType.ONE_TO_ONE:
                        generated_column_data[entity_column.name] = random.sample(fk_data, entity.total_rows)
                else:
                    generated_column_data[entity_column.name] = self.generate_column_values(
                        total_rows=entity.total_rows,
                        entity_column=entity_column
                    )

            generated_entity_data[entity.table_name] = generated_column_data
            mock_entity_result = MockDataEntityResult(
                table_name=entity.table_name,
                entity=entity,
                generated_data=generated_column_data
            )
            mock_results.append(mock_entity_result)

        return mock_results



