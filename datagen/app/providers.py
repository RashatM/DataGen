import boto3
from mypy_boto3_s3 import S3Client

from app.core.application.ports.comparison_query_renderer_port import ComparisonQueryRendererPort
from app.core.application.ports.comparison_repository_port import IComparisonReportRepository
from app.core.application.ports.dag_runner_port import DagRunnerPort
from app.core.application.ports.publication_repository_port import IArtifactPublicationRepository
from app.core.application.services.comparison_service import ComparisonService
from app.core.application.services.publication_service import ArtifactPublicationService
from app.core.application.services.generation_service import DataGenerationService
from app.core.domain.enums import DataType
from app.infrastructure.airflow.airflow_client import AirflowClient
from app.infrastructure.airflow.airflow_dag_payload_builder import AirflowDagPayloadBuilder
from app.infrastructure.airflow.airflow_dag_runner import AirflowDagRunner
from app.infrastructure.converters.source_value_converters.boolean_source_value_converter import BooleanSourceValueConverter
from app.infrastructure.converters.source_value_converters.date_source_value_converter import DateSourceValueConverter
from app.infrastructure.converters.source_value_converters.float_source_value_converter import FloatSourceValueConverter
from app.infrastructure.converters.source_value_converters.int_source_value_converter import IntSourceValueConverter
from app.infrastructure.converters.source_value_converters.string_source_value_converter import \
    StringSourceValueConverter
from app.infrastructure.converters.source_value_converters.timestamp_source_value_converter import \
    TimestampSourceValueConverter
from app.infrastructure.converters.value_converter_factory import ValueConverterFactory
from app.infrastructure.ddl.hive_query_builder import HiveQueryBuilder
from app.infrastructure.ddl.iceberg_query_builder import IcebergQueryBuilder
from app.infrastructure.generators.boolean_generator import BooleanDataGenerator
from app.infrastructure.generators.date_generator import DateDataGenerator
from app.infrastructure.generators.float_generator import FloatDataGenerator
from app.infrastructure.generators.int_generator import IntDataGenerator
from app.infrastructure.generators.generator_factory import DataGeneratorFactory
from app.infrastructure.generators.string_generator import StringDataGenerator
from app.infrastructure.generators.timestamp_generator import TimestampDataGenerator
from app.infrastructure.graph.networkx_dependency_graph_builder import NetworkXDependencyGraphBuilder
from app.infrastructure.query.comparison_query_renderer import TargetTableComparisonQueryRenderer
from app.core.application.ports.object_storage_port import IObjectStorage
from app.infrastructure.parquet.arrow_schema_builder import ArrowSchemaBuilder
from app.infrastructure.repositories.s3_comparison_repository import S3ComparisonReportRepository
from app.infrastructure.repositories.s3_publication_repository import S3PublicationRepository
from app.infrastructure.s3.s3_object_storage import S3StorageAdapter
from app.core.application.ports.value_converter_port import IValueConverter
from app.shared.config import S3Config, AirflowConfig, TargetStorageConfig


def provide_generator_factory() -> DataGeneratorFactory:
    factory = DataGeneratorFactory()
    factory.register(DataType.STRING, StringDataGenerator())
    factory.register(DataType.INT, IntDataGenerator())
    factory.register(DataType.FLOAT, FloatDataGenerator())
    factory.register(DataType.DATE, DateDataGenerator())
    factory.register(DataType.TIMESTAMP, TimestampDataGenerator())
    factory.register(DataType.BOOLEAN, BooleanDataGenerator())
    return factory


def provide_value_converter() -> IValueConverter:
    factory = ValueConverterFactory()
    factory.register(DataType.STRING, StringSourceValueConverter())
    factory.register(DataType.INT, IntSourceValueConverter())
    factory.register(DataType.FLOAT, FloatSourceValueConverter())
    factory.register(DataType.DATE, DateSourceValueConverter())
    factory.register(DataType.TIMESTAMP, TimestampSourceValueConverter())
    factory.register(DataType.BOOLEAN, BooleanSourceValueConverter())
    return factory.create()


def provide_generation_service() -> DataGenerationService:
    data_generator_factory = provide_generator_factory()
    return DataGenerationService(
        data_generator_factory=data_generator_factory,
        dependency_order_builder=NetworkXDependencyGraphBuilder(),
        value_converter=provide_value_converter(),
    )


def provide_s3_client(s3_config: S3Config) -> S3Client:
    client = boto3.client(
        "s3",
        endpoint_url=s3_config.endpoint_url,
        aws_access_key_id=s3_config.aws_access_key_id,
        aws_secret_access_key=s3_config.aws_secret_access_key,
        use_ssl=s3_config.use_ssl,
        verify=s3_config.ssl_cert
    )
    return client


def provide_s3_object_storage(bucket: str, s3_client: S3Client) -> IObjectStorage:
    return S3StorageAdapter(bucket=bucket, s3_client=s3_client)


def provide_artifact_publication_repository(object_storage: IObjectStorage) -> IArtifactPublicationRepository:
    schema_builder = ArrowSchemaBuilder()
    return S3PublicationRepository(object_storage=object_storage, schema_builder=schema_builder)


def provide_comparison_report_repository(object_storage: IObjectStorage) -> IComparisonReportRepository:
    return S3ComparisonReportRepository(object_storage=object_storage)


def provide_comparison_query_renderer() -> ComparisonQueryRendererPort:
    return TargetTableComparisonQueryRenderer()


def provide_artifact_publication_service(
        object_storage: IObjectStorage,
        target_storage: TargetStorageConfig,
) -> ArtifactPublicationService:
    artifact_publication_repository = provide_artifact_publication_repository(object_storage)
    return ArtifactPublicationService(
        repository=artifact_publication_repository,
        comparison_query_renderer=provide_comparison_query_renderer(),
        query_builders={
            "hive": HiveQueryBuilder(
                database_name=target_storage.hive.database_name,
            ),
            "iceberg": IcebergQueryBuilder(
                database_name=target_storage.iceberg.database_name,
            ),
        },
    )


def provide_dag_runner(
        airflow_config: AirflowConfig,
        object_storage: IObjectStorage,
) -> DagRunnerPort:
    return AirflowDagRunner(
        client=AirflowClient(airflow_config),
        payload_builder=AirflowDagPayloadBuilder(object_storage=object_storage),
    )


def provide_comparison_service(object_storage: IObjectStorage) -> ComparisonService:
    comparison_report_repository = provide_comparison_report_repository(object_storage)
    return ComparisonService(repository=comparison_report_repository)
