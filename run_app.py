import json

import pandas as pd

from app.data.converters import convert_to_mock_data_entity, convert_to_mock_data_schema
from app.data.graph_builder import DependencyGraphBuilder
from app.dto.mock_data import MockDataSchema
from app.enums import DataType, DataBaseType
from app.mocks.factories.mock_factory import MockFactory
from app.mocks.generators.boolean_mock import BooleanGeneratorMock
from app.mocks.generators.date_mock import DateGeneratorMock
from app.mocks.generators.float_mock import FloatGeneratorMock
from app.mocks.generators.int_mock import IntGeneratorMock
from app.mocks.generators.string_mock import StringGeneratorMock
from app.mocks.generators.timestamp_mock import TimestampGeneratorMock
from app.services.ddl_query_builder.postgres_query_service import PostgresQueryBuilderService
from app.services.mock_service import MockDataService
from app.services.storage_service import StorageService

if __name__ == "__main__":
    with open("./params/entity_schema.json") as f:
        data = json.load(f)
        print(data)

    mock_data_schema = convert_to_mock_data_schema(data)
    entities = mock_data_schema.entities

    mock_factory = MockFactory()
    mock_factory.register(data_type=DataType.STRING, mock_generator=StringGeneratorMock())
    mock_factory.register(data_type=DataType.INT, mock_generator=IntGeneratorMock())
    mock_factory.register(data_type=DataType.FLOAT, mock_generator=FloatGeneratorMock())
    mock_factory.register(data_type=DataType.DATE, mock_generator=DateGeneratorMock())
    mock_factory.register(data_type=DataType.TIMESTAMP, mock_generator=TimestampGeneratorMock())
    mock_factory.register(data_type=DataType.BOOLEAN, mock_generator=BooleanGeneratorMock())


    graph_order_builder = DependencyGraphBuilder()
    mock_service = MockDataService(mock_factory=mock_factory, dependency_order_builder=graph_order_builder)
    mock_results = mock_service.generate_entity_values(entities)


    ddl_query_service = PostgresQueryBuilderService()
    storage_service = StorageService()

    for mock_result in mock_results:
        df = pd.DataFrame(mock_result.generated_data)
        print(f"Таблица: {mock_result.table_name}")
        print(df.head(1000), "\n")

        ddl_query = ddl_query_service.create_ddl(entity=mock_result.entity)
        print(ddl_query)

        storage_service.create_table(ddl_query=ddl_query)
        storage_service.save_to_source(mock_data=mock_result)


