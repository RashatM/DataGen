import json
import pandas as pd

from app.config.logger import logger
from app.data.converters.converters import convert_to_mock_data_schema
from app.data.converters.excel_converter import convert_excel_to_json
from app.data.graph_builder import DependencyGraphBuilder
from app.data.repository import MockRepository
from app.enums import DataType, SourceType
from app.mocks.generators.date.date_in_string_mock import DateInStringGeneratorMock
from app.mocks.generators.date.date_mock import DateGeneratorMock
from app.mocks.generators.timestamp.timestamp_in_string_mock import TimestampInStringGeneratorMock
from app.mocks.generators.timestamp.timestamp_mock import TimestampGeneratorMock
from app.mocks.mock_factory import MockFactory
from app.mocks.generators.boolean_mock import BooleanGeneratorMock
from app.mocks.generators.float_mock import FloatGeneratorMock
from app.mocks.generators.int_mock import IntGeneratorMock
from app.mocks.generators.string_mock import StringGeneratorMock
from app.services.ddl_query_builder.postgres_query_service import PostgresQueryBuilderService
from app.services.mock_service import MockDataService
from app.services.storage_service import MockStorageService


def provide_mock_factory():
    mock_factory = MockFactory()
    mock_factory.register(data_type=DataType.STRING, mock_generator=StringGeneratorMock())
    mock_factory.register(data_type=DataType.INT, mock_generator=IntGeneratorMock())
    mock_factory.register(data_type=DataType.FLOAT, mock_generator=FloatGeneratorMock())
    mock_factory.register(data_type=DataType.DATE, mock_generator=DateGeneratorMock())
    mock_factory.register(data_type=DataType.DATE_IN_STRING, mock_generator=DateInStringGeneratorMock())
    mock_factory.register(data_type=DataType.TIMESTAMP, mock_generator=TimestampGeneratorMock())
    mock_factory.register(data_type=DataType.TIMESTAMP_IN_STRING, mock_generator=TimestampInStringGeneratorMock())
    mock_factory.register(data_type=DataType.BOOLEAN, mock_generator=BooleanGeneratorMock())

    return mock_factory

def provide_mock_service(mock_factory):
    graph_order_builder = DependencyGraphBuilder()
    return MockDataService(mock_factory=mock_factory, dependency_order_builder=graph_order_builder)

def provide_ddl_query_service(db_type: SourceType):
    return PostgresQueryBuilderService()


def run():
    data = convert_excel_to_json("params/data_gen_v2.xlsx")

    mock_data_schema = convert_to_mock_data_schema(data)
    entities = mock_data_schema.entities
    db_type = mock_data_schema.source_type

    mock_factory = provide_mock_factory()
    mock_service = provide_mock_service(mock_factory)
    ddl_query_service = provide_ddl_query_service(db_type)

    mock_results = mock_service.generate_entity_values(entities)

    mock_repository = MockRepository()

    with mock_repository:
        storage_service = MockStorageService(mock_repository=mock_repository)

        db_schemas = set()
        for mock_result in mock_results:
            if mock_result.entity.schema_name not in db_schemas:
                storage_service.create_db_schema(mock_result.entity.schema_name)
                db_schemas.add(mock_result.entity.schema_name)
            # df = pd.DataFrame(mock_result.generated_data)
            # print(f"Таблица: {mock_result.entity.table_name}")
            # print(df.head(1000), "\n")

            ddl_query = ddl_query_service.create_ddl(entity=mock_result.entity)
            logger.info(f"DDL was successfully built: {ddl_query}")

            storage_service.create_and_save_to_source(ddl_query=ddl_query, mock_data=mock_result)





if __name__ == "__main__":
    run()


