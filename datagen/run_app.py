import pandas as pd

from app.core.application.services import MockDataService
from app.core.domain.enums import DataType
from app.infrastructure.converters.schema_converter import convert_to_mock_data_entity
from app.infrastructure.converters.value_converter_factory import ValueConverterFactory
from app.infrastructure.ddl.postgres_query_builder import PostgresQueryBuilderService
from app.infrastructure.generators.boolean_generator import BooleanGeneratorMock
from app.infrastructure.generators.date_generator import DateGeneratorMock
from app.infrastructure.generators.float_generator import FloatGeneratorMock
from app.infrastructure.generators.int_generator import IntGeneratorMock
from app.infrastructure.generators.mock_factory import MockFactory
from app.infrastructure.generators.string_generator import StringGeneratorMock
from app.infrastructure.generators.timestamp_generator import TimestampGeneratorMock
from app.infrastructure.graph.networkx_dependency_graph_builder import NetworkXDependencyGraphBuilder
from app.infrastructure.repositories.postgres_repository import MockRepository
from app.infrastructure.converters.source_value_converters.boolean_source_value_converter import BooleanSourceValueConverter
from app.infrastructure.converters.source_value_converters.date_source_value_converter import DateSourceValueConverter
from app.infrastructure.converters.source_value_converters.float_source_value_converter import FloatSourceValueConverter
from app.infrastructure.converters.source_value_converters.int_source_value_converter import IntSourceValueConverter
from app.infrastructure.converters.source_value_converters.string_source_value_converter import StringSourceValueConverter
from app.infrastructure.converters.source_value_converters.timestamp_source_value_converter import TimestampSourceValueConverter
from app.infrastructure.writers.sql_entity_writer import SqlEntityWriter


def provide_mock_factory():
    mock_factory = MockFactory()
    mock_factory.register(data_type=DataType.STRING, mock_generator=StringGeneratorMock())
    mock_factory.register(data_type=DataType.INT, mock_generator=IntGeneratorMock())
    mock_factory.register(data_type=DataType.FLOAT, mock_generator=FloatGeneratorMock())
    mock_factory.register(data_type=DataType.DATE, mock_generator=DateGeneratorMock())
    mock_factory.register(data_type=DataType.TIMESTAMP, mock_generator=TimestampGeneratorMock())
    mock_factory.register(data_type=DataType.BOOLEAN, mock_generator=BooleanGeneratorMock())
    return mock_factory


def provide_mock_service(mock_factory):
    graph_order_builder = NetworkXDependencyGraphBuilder()
    return MockDataService(
        mock_factory=mock_factory,
        dependency_order_builder=graph_order_builder,
        value_converter=provide_value_converter(),
    )

def provide_value_converter():
    value_converter_factory = ValueConverterFactory()
    value_converter_factory.register(source_type=DataType.STRING, source_converter=StringSourceValueConverter())
    value_converter_factory.register(source_type=DataType.INT, source_converter=IntSourceValueConverter())
    value_converter_factory.register(source_type=DataType.FLOAT, source_converter=FloatSourceValueConverter())
    value_converter_factory.register(source_type=DataType.DATE, source_converter=DateSourceValueConverter())
    value_converter_factory.register(source_type=DataType.TIMESTAMP, source_converter=TimestampSourceValueConverter())
    value_converter_factory.register(source_type=DataType.BOOLEAN, source_converter=BooleanSourceValueConverter())
    return value_converter_factory.create()


def provide_ddl_query_service():
    return PostgresQueryBuilderService()

def provide_entity_writer(mock_repository):
    return SqlEntityWriter(
        mock_repository=mock_repository,
        ddl_query_builder=provide_ddl_query_service(),
    )


def run():
    data = {
        "entities": [
            {
                "schema_name": "analytics",
                "table_name": "company_groups",
                "total_rows": 5000,
                "columns": [
                    {
                        "name": "INN",
                        "gen_data_type": "string",
                        "output_data_type": "int",
                        "is_primary_key": True,
                        "constraints": {
                            "null_ratio": 0,
                            "is_unique": True,
                            "length": 10,
                            "character_set": "digits"
                        }
                    },
                    {
                        "name": "GROUP_ID_DM",
                        "gen_data_type": "int",
                        "constraints": {
                            "null_ratio": 0,
                            "min_value": 1,
                            "max_value": 6000000
                        }
                    },
                    {
                        "name": "MAIN_COMPANY_FLG_DM",
                        "gen_data_type": "int",
                        "constraints": {
                            "allowed_values": [0, 1]
                        }
                    },
                    {
                        "name": "VALUE_DAY",
                        "gen_data_type": "date",
                        "output_data_type": "int",
                        "constraints": {
                            "date_format": "%Y%m%d"
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
                        "gen_data_type": "string",
                        "constraints": {
                            "allowed_values": ["c", "cpr"]
                        }
                    },
                    {
                        "name": "SINN",
                        "gen_data_type": "int",
                        "foreign_key": {
                            "schema_name": "analytics",
                            "table_name": "company_groups",
                            "column_name": "INN",
                            "relation_type": "one_to_many"
                        },
                    },
                    {
                        "name": "SOGRN",
                        "gen_data_type": "int",
                        "constraints": {
                            "null_ratio": 10,
                            "min_value": 10 ** 12,
                            "max_value": 10 ** 15
                        }
                    },
                    {
                        "name": "SPIN",
                        "gen_data_type": "string",
                        "constraints": {
                            "null_ratio": 10,
                            "length": 6,
                            "case_mode": "upper",
                            "regular_expr": "^[A-Z0-9]{6}$"
                        }
                    },
                    {
                        "name": "SABSCODE",
                        "gen_data_type": "string",
                        "constraints": {
                            "allowed_values": ["SFAProd"]
                        }
                    },
                    {
                        "name": "SPINSFA",
                        "gen_data_type": "string",
                        "constraints": {
                            "regular_expr": "^ABR-FW-SCRMFW-WORK-ACCOUNT ACB-\\d+$"
                        }
                    },
                    {
                        "name": "IDSUBJECT",
                        "gen_data_type": "int",
                        "constraints": {
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

    entities = [convert_to_mock_data_entity(entity_data) for entity_data in data["entities"]]

    mock_factory = provide_mock_factory()
    mock_service = provide_mock_service(mock_factory)
    mock_results = mock_service.generate_entity_values(entities)

    mock_repository = MockRepository()

    with mock_repository:
        entity_writer = provide_entity_writer(mock_repository)

        with pd.ExcelWriter("results.xlsx", engine="openpyxl") as writer:
            for mock_result in mock_results:
                df = pd.DataFrame(mock_result.generated_data)
                sheet_name = f"{mock_result.entity.schema_name}_{mock_result.entity.table_name}"
                df.to_excel(writer, sheet_name=sheet_name, index=False, na_rep="")
                entity_writer.persist_entity_result(mock_result)


if __name__ == "__main__":
    run()
