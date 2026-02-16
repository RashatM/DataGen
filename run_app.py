import pandas as pd

from app.shared.logger import logger
from app.data.converters.converters import convert_to_mock_data_schema
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
    # data = convert_excel_to_json("params/data_gen_v2.xlsx")
    data = {
        "entities": [
            {
                "schema_name": "analytics",
                "table_name": "company_groups",
                "total_rows": 5000,
                "columns": [
                    {
                        "name": "INN",
                        "data_type": "int",
                        "is_primary_key": True,
                        "foreign_key": None,
                        "constraints": {
                            "null_ratio": 0,
                            "is_unique": True,
                            "min_value": 1000000000,
                            "max_value": 9999999999
                        }
                    },
                    {
                        "name": "GROUP_ID_DM",
                        "data_type": "int",
                        "is_primary_key": False,
                        "foreign_key": None,
                        "constraints": {
                            "null_ratio": 0,
                            "is_unique": False,
                            "min_value": 1,
                            "max_value": 6000000
                        }
                    },
                    {
                        "name": "MAIN_COMPANY_FLG_DM",
                        "data_type": "int",
                        "is_primary_key": False,
                        "foreign_key": None,
                        "constraints": {
                            "null_ratio": 0,
                            "is_unique": False,
                            "allowed_values": [0, 1]
                        }
                    },
                    {
                        "name": "VALUE_DAY",
                        "data_type": "timestamp",
                        "is_primary_key": False,
                        "foreign_key": None,
                        "constraints": {
                            "null_ratio": 0,
                            "is_unique": False,
                            "allowed_values": ["2024-04-15 03:00:00"],
                            "timestamp_format": "%Y-%m-%d %H:%M:%S"
                        }
                    }
                ]
            },
            {
                "schema_name": "analytics",
                "table_name": "subjects",
                "total_rows": 10000,
                "columns": [
                    {
                        "name": "STYPE",
                        "data_type": "string",
                        "is_primary_key": False,
                        "foreign_key": None,
                        "constraints": {
                            "null_ratio": 0,
                            "is_unique": False,
                            "allowed_values": ["c", "cpr"]
                        }
                    },
                    {
                        "name": "SINN",
                        "data_type": "int",
                        "is_primary_key": False,
                        "foreign_key": {
                            "table_name": "company_groups",
                            "column_name": "INN",
                            "relation_type": "one_to_many"
                        },
                        "constraints": {
                            "null_ratio": 0,
                            "is_unique": False
                        }
                    },
                    {
                        "name": "SOGRN",
                        "data_type": "int",
                        "is_primary_key": False,
                        "foreign_key": None,
                        "constraints": {
                            "null_ratio": 10,
                            "is_unique": False,
                            "min_value": 10 ** 12,
                            "max_value": 10 ** 15
                        }
                    },
                    {
                        "name": "SPIN",
                        "data_type": "string",
                        "is_primary_key": False,
                        "foreign_key": None,
                        "constraints": {
                            "null_ratio": 10,
                            "is_unique": False,
                            "length": 6,
                            "uppercase": True,
                            "regular_expr": "^[A-Z0-9]{6}$"
                        }
                    },
                    {
                        "name": "SABSCODE",
                        "data_type": "string",
                        "is_primary_key": False,
                        "foreign_key": None,
                        "constraints": {
                            "null_ratio": 0,
                            "is_unique": False,
                            "allowed_values": ["SFAProd"]
                        }
                    },
                    {
                        "name": "SPINSFA",
                        "data_type": "string",
                        "is_primary_key": False,
                        "foreign_key": None,
                        "constraints": {
                            "null_ratio": 0,
                            "is_unique": False,
                            "regular_expr": "^ABR-FW-SCRMFW-WORK-ACCOUNT ACB-\\d+$"
                        }
                    },
                    {
                        "name": "IDSUBJECT",
                        "data_type": "int",
                        "is_primary_key": False,
                        "foreign_key": None,
                        "constraints": {
                            "null_ratio": 0,
                            "is_unique": False,
                            "min_value": 1,
                            "max_value": 20000000
                        }
                    }
                ]
            }
        ],
        "source_name": "example_source",
        "source_type": "postgres"
    }

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

        with pd.ExcelWriter("results.xlsx", engine="openpyxl") as writer:
            for i, mock_result in enumerate(mock_results):

                if mock_result.entity.schema_name not in db_schemas:
                    storage_service.create_db_schema(mock_result.entity.schema_name)
                    db_schemas.add(mock_result.entity.schema_name)

                df = pd.DataFrame(mock_result.generated_data)

                sheet_name = f"{mock_result.entity.schema_name}_{mock_result.entity.table_name}"

                df.to_excel(
                    writer,
                    sheet_name=sheet_name,
                    index=False,
                    na_rep=""
                )

                ddl_query = ddl_query_service.create_ddl(entity=mock_result.entity)
                logger.info(f"DDL was successfully built: {ddl_query}")

                storage_service.create_and_save_to_source(
                    ddl_query=ddl_query,
                    mock_data=mock_result
                )





if __name__ == "__main__":
    run()
