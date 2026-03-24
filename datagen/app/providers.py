import boto3
import random
from mypy_boto3_s3 import S3Client

from app.core.application.ports.comparison_query_renderer_port import ComparisonQueryRendererPort
from app.core.application.ports.comparison_repository_port import ComparisonReportRepositoryPort
from app.core.application.ports.execution_runner_port import ExecutionRunnerPort
from app.core.application.ports.publication_repository_port import ArtifactPublicationRepositoryPort
from app.core.application.services.comparison_report_service import ComparisonReportService
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
from app.infrastructure.graph.networkx_table_dependency_planner import NetworkXTableDependencyPlanner
from app.infrastructure.query.comparison_query_renderer import TargetTableComparisonQueryRenderer
from app.infrastructure.parquet.arrow_schema_builder import ArrowSchemaBuilder
from app.infrastructure.repositories.s3_comparison_repository import S3ComparisonReportRepository
from app.infrastructure.repositories.s3_publication_repository import S3PublicationRepository
from app.infrastructure.s3.s3_object_storage import S3StorageAdapter
from app.core.application.ports.value_converter_port import ValueConverterPort
from app.shared.config import S3Config, AirflowConfig, TargetStorageConfig


def provide_generator_factory(rng: random.Random) -> DataGeneratorFactory:
    factory = DataGeneratorFactory()
    factory.register(DataType.STRING, StringDataGenerator(rng))
    factory.register(DataType.INT, IntDataGenerator(rng))
    factory.register(DataType.FLOAT, FloatDataGenerator(rng))
    factory.register(DataType.DATE, DateDataGenerator(rng))
    factory.register(DataType.TIMESTAMP, TimestampDataGenerator(rng))
    factory.register(DataType.BOOLEAN, BooleanDataGenerator(rng))
    return factory


def provide_value_converter() -> ValueConverterPort:
    factory = ValueConverterFactory()
    factory.register(DataType.STRING, StringSourceValueConverter())
    factory.register(DataType.INT, IntSourceValueConverter())
    factory.register(DataType.FLOAT, FloatSourceValueConverter())
    factory.register(DataType.DATE, DateSourceValueConverter())
    factory.register(DataType.TIMESTAMP, TimestampSourceValueConverter())
    factory.register(DataType.BOOLEAN, BooleanSourceValueConverter())
    return factory.create()


def provide_generation_service() -> DataGenerationService:
    rng = random.Random()
    data_generator_factory = provide_generator_factory(rng)
    return DataGenerationService(
        data_generator_factory=data_generator_factory,
        table_dependency_planner=NetworkXTableDependencyPlanner(),
        value_converter=provide_value_converter(),
        rng=rng,
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


def provide_s3_object_storage(bucket: str, s3_client: S3Client) -> S3StorageAdapter:
    return S3StorageAdapter(bucket=bucket, s3_client=s3_client)


def provide_artifact_publication_repository(
        object_storage: S3StorageAdapter,
) -> ArtifactPublicationRepositoryPort:
    schema_builder = ArrowSchemaBuilder()
    return S3PublicationRepository(object_storage=object_storage, schema_builder=schema_builder)


def provide_comparison_report_repository(
        object_storage: S3StorageAdapter,
) -> ComparisonReportRepositoryPort:
    return S3ComparisonReportRepository(object_storage=object_storage)


def provide_comparison_query_renderer() -> ComparisonQueryRendererPort:
    return TargetTableComparisonQueryRenderer()


def provide_artifact_publication_service(
        object_storage: S3StorageAdapter,
        target_storage: TargetStorageConfig,
) -> ArtifactPublicationService:
    artifact_publication_repository = provide_artifact_publication_repository(object_storage)
    return ArtifactPublicationService(
        repository=artifact_publication_repository,
        comparison_query_renderer=provide_comparison_query_renderer(),
        hive_load_payload_builder=HiveQueryBuilder(
            database_name=target_storage.hive.database_name,
        ),
        iceberg_load_payload_builder=IcebergQueryBuilder(
            database_name=target_storage.iceberg.database_name,
        ),
    )


def provide_execution_runner(
        airflow_config: AirflowConfig,
        object_storage: S3StorageAdapter,
) -> ExecutionRunnerPort:
    return AirflowDagRunner(
        client=AirflowClient(airflow_config),
        payload_builder=AirflowDagPayloadBuilder(object_storage=object_storage),
    )


def provide_comparison_report_service(object_storage: S3StorageAdapter) -> ComparisonReportService:
    comparison_report_repository = provide_comparison_report_repository(object_storage)
    return ComparisonReportService(repository=comparison_report_repository)
