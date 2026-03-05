import boto3
from botocore.client import BaseClient

from app.core.application.ports.publication_repository_port import IPublicationRepository
from app.core.application.services.publication_service import PublicationService
from app.core.application.services.mock_data_service import MockDataService
from app.core.domain.enums import DataType
from app.infrastructure.converters.source_value_converters.boolean_source_value_converter import BooleanSourceValueConverter
from app.infrastructure.converters.source_value_converters.date_source_value_converter import DateSourceValueConverter
from app.infrastructure.converters.source_value_converters.float_source_value_converter import FloatSourceValueConverter
from app.infrastructure.converters.source_value_converters.int_source_value_converter import IntSourceValueConverter
from app.infrastructure.converters.source_value_converters.string_source_value_converter import StringSourceValueConverter
from app.infrastructure.converters.source_value_converters.timestamp_source_value_converter import TimestampSourceValueConverter
from app.infrastructure.converters.value_converter_factory import ValueConverterFactory
from app.infrastructure.ddl.hive_query_builder import HiveQueryBuilder
from app.infrastructure.ddl.iceberg_query_builder import IcebergQueryBuilder
from app.infrastructure.generators.boolean_generator import BooleanGeneratorMock
from app.infrastructure.generators.date_generator import DateGeneratorMock
from app.infrastructure.generators.float_generator import FloatGeneratorMock
from app.infrastructure.generators.int_generator import IntGeneratorMock
from app.infrastructure.generators.mock_factory import MockFactory
from app.infrastructure.generators.string_generator import StringGeneratorMock
from app.infrastructure.generators.timestamp_generator import TimestampGeneratorMock
from app.infrastructure.graph.networkx_dependency_graph_builder import NetworkXDependencyGraphBuilder
from app.infrastructure.ports.object_storage_port import IObjectStorage
from app.infrastructure.repositories.s3_publication_repository import S3PublicationRepository
from app.infrastructure.storage.s3_object_storage import S3StorageAdapter
from app.shared.settings import S3TargetSettings


def provide_mock_factory() -> MockFactory:
    f = MockFactory()
    f.register(DataType.STRING, StringGeneratorMock())
    f.register(DataType.INT, IntGeneratorMock())
    f.register(DataType.FLOAT, FloatGeneratorMock())
    f.register(DataType.DATE, DateGeneratorMock())
    f.register(DataType.TIMESTAMP, TimestampGeneratorMock())
    f.register(DataType.BOOLEAN, BooleanGeneratorMock())
    return f


def provide_value_converter():
    factory = ValueConverterFactory()
    factory.register(DataType.STRING, StringSourceValueConverter())
    factory.register(DataType.INT, IntSourceValueConverter())
    factory.register(DataType.FLOAT, FloatSourceValueConverter())
    factory.register(DataType.DATE, DateSourceValueConverter())
    factory.register(DataType.TIMESTAMP, TimestampSourceValueConverter())
    factory.register(DataType.BOOLEAN, BooleanSourceValueConverter())
    return factory.create()


def provide_mock_service(mock_factory: MockFactory) -> MockDataService:
    return MockDataService(
        mock_factory=mock_factory,
        dependency_order_builder=NetworkXDependencyGraphBuilder(),
        value_converter=provide_value_converter(),
    )


def provide_s3_client(s3_target: S3TargetSettings) -> BaseClient:
    kwargs = {"verify": s3_target.verify_ssl}
    if s3_target.endpoint_url:
        kwargs["endpoint_url"] = s3_target.endpoint_url
    if s3_target.region_name:
        kwargs["region_name"] = s3_target.region_name
    return boto3.client("s3", **kwargs)


def provide_s3_object_storage(bucket: str, prefix: str, s3_client: BaseClient) -> IObjectStorage:
    return S3StorageAdapter(bucket=bucket, prefix=prefix, s3_client=s3_client)


def provide_publication_repository(
    object_storage: IObjectStorage,
) -> IPublicationRepository:
    return S3PublicationRepository(object_storage)


def provide_publication_service(s3_target: S3TargetSettings) -> PublicationService:
    s3_client = provide_s3_client(s3_target)
    object_storage = provide_s3_object_storage(
        bucket=s3_target.bucket,
        prefix=s3_target.prefix,
        s3_client=s3_client,
    )

    publication_repository = provide_publication_repository(object_storage)
    return PublicationService(
        repository=publication_repository,
        ddl_builders={
            "hive": HiveQueryBuilder(),
            "iceberg": IcebergQueryBuilder(),
        },
    )
