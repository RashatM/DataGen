from datetime import datetime
from typing import Any
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import os

from plugins.platform_services.operators.templated_spark_operator import PlatformTemplatedSparkOperator

DAG_ID = "datagen_synth_load"
DAG_DESCRIPTION = "DataGen: loads synthetic data from S3 to Iceberg and Hive"
DAG_TAGS = ["datagen", "synthetic"]
SCHEDULE_INTERVAL = None
EMAIL_LIST = []
ALLOWED_WRITE_MODES = {
    "OVERWRITE_TABLE",
    "OVERWRITE_PARTITIONS",
    "APPEND",
}
ENGINE_NAMES = ("hive", "iceberg")

BASE_LOADER_SCRIPT = "/opt/airflow/dags/repo/scripts/platform_services/datagen/base_loader.py"
JOB_COMMON_SCRIPT = "/opt/airflow/dags/repo/scripts/platform_services/datagen/job_common.py"
ICEBERG_LOADER_SCRIPT = "/opt/airflow/dags/repo/scripts/platform_services/datagen/iceberg_loader.py"
HIVE_LOADER_SCRIPT = "/opt/airflow/dags/repo/scripts/platform_services/datagen/hive_loader.py"
RESULT_COMPARATOR_SCRIPT = "/opt/airflow/dags/repo/scripts/platform_services/datagen/result_comparator.py"
HADOOP_CLUSTER_CONFIG_VARIABLE = "datagen_hadoop_cluster_config"

SPARK_CONF_DIRS = {
    "BDA51": "/opt/airflow/dags/repo/configs/platform_services/s3_to_hadoop/bda51/spark-conf",
    "BDA61": "/opt/airflow/dags/repo/configs/platform_services/s3_to_hadoop/bda61/spark-conf",
    "BDA71": "/opt/airflow/dags/repo/configs/platform_services/s3_to_hadoop/bda71/spark-conf",
}


def require_non_empty_string(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Contract is missing non-empty '{field_name}'")
    return value


def require_object(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"Contract is missing object '{field_name}'")
    return value


def validate_table_contracts(conf: dict[str, Any]) -> None:
    tables = conf.get("tables")
    if not isinstance(tables, list) or not tables:
        raise ValueError("Contract is missing non-empty 'tables'")

    for index, table_payload in enumerate(tables):
        table = require_object(table_payload, f"tables[{index}]")
        require_non_empty_string(
            table.get("table_name"),
            f"tables[{index}].table_name",
        )
        require_non_empty_string(table.get("data_uri"), f"tables[{index}].data_uri")
        load = require_object(table.get("load"), f"tables[{index}].load")

        for engine in ENGINE_NAMES:
            field_prefix = f"tables[{index}].load.{engine}"
            engine_load = require_object(load.get(engine), field_prefix)
            require_non_empty_string(
                engine_load.get("target_table_name"),
                f"{field_prefix}.target_table_name",
            )
            write_mode = require_non_empty_string(
                engine_load.get("write_mode"),
                f"{field_prefix}.write_mode",
            )
            if write_mode not in ALLOWED_WRITE_MODES:
                allowed = ", ".join(sorted(ALLOWED_WRITE_MODES))
                raise ValueError(
                    f"Unsupported write_mode in {field_prefix}: {write_mode}. "
                    f"Allowed: {allowed}"
                )
            columns = engine_load.get("columns")
            if not isinstance(columns, list) or not columns:
                raise ValueError(
                    f"Contract is missing non-empty list '{field_prefix}.columns'"
                )


def validate_comparison_contract(conf: dict[str, Any]) -> None:
    comparison = require_object(conf.get("comparison"), "comparison")

    for uri_field in ("query_uris", "result_uris"):
        engine_uris = require_object(
            comparison.get(uri_field),
            f"comparison.{uri_field}",
        )
        for engine in ENGINE_NAMES:
            require_non_empty_string(
                engine_uris.get(engine),
                f"comparison.{uri_field}.{engine}",
            )

    require_non_empty_string(comparison.get("report_uri"), "comparison.report_uri")

    exclude_columns = comparison.get("exclude_columns")
    if exclude_columns is None:
        return

    exclude_columns = require_object(exclude_columns, "comparison.exclude_columns")
    for engine in ENGINE_NAMES:
        if not isinstance(exclude_columns.get(engine, []), list):
            raise ValueError(f"comparison.exclude_columns.{engine} must be a list")


def validate_contract(**context) -> None:
    conf = context["dag_run"].conf or {}

    require_non_empty_string(conf.get("run_id"), "run_id")
    validate_table_contracts(conf)
    validate_comparison_contract(conf)


def get_iceberg_spark_config(**context) -> dict[str, str]:
    return {
        **Variable.get("load_table_test", deserialize_json=True),
        **Variable.get("yc_keys", deserialize_json=True),
        "spark.sql.session.timeZone": "UTC",
        "spark.dynamicAllocation.enabled": "false",
        "spark.executor.instances": "2",
        "spark.executor.cores": "4",
        "spark.driver.memory": "8G",
        "spark.executor.memory": "8G",
    }


def get_hadoop_cluster_config() -> dict[str, Any]:
    cluster_config = Variable.get(HADOOP_CLUSTER_CONFIG_VARIABLE, deserialize_json=True)
    required_keys = ["cluster_name", "kerberos_principal", "kerberos_keytab_path"]
    missing_keys = [key for key in required_keys if not cluster_config.get(key)]

    if missing_keys:
        raise ValueError(
            f"Variable {HADOOP_CLUSTER_CONFIG_VARIABLE} is missing required keys: {', '.join(missing_keys)}"
        )

    return cluster_config


def get_hive_spark_config(**context) -> dict[str, str]:
    cluster_config = get_hadoop_cluster_config()
    cluster_name = cluster_config["cluster_name"]

    if cluster_name not in SPARK_CONF_DIRS:
        raise ValueError(f"Unsupported cluster_name={cluster_name}. Expected: {', '.join(SPARK_CONF_DIRS)}")

    krb5_conf = "/tmp/krb5.conf"
    os.environ["SPARK_CONF_DIR"] = SPARK_CONF_DIRS[cluster_name]
    os.environ["JAVA_TOOL_OPTIONS"] = (
        "-Djavax.net.ssl.trustStore=/tmp/truststore.jks "
        "-Djavax.net.ssl.trustStorePassword=changeit "
        f"-Djava.security.krb5.conf={krb5_conf}"
    )

    return {
        **Variable.get("load_table_test", deserialize_json=True),
        **Variable.get("yc_keys", deserialize_json=True),
        "spark.dynamicAllocation.shuffleTracking.enabled": "true",
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.initialExecutors": "2",
        "spark.dynamicAllocation.minExecutors": "1",
        "spark.dynamicAllocation.maxExecutors": "5",
        "spark.driver.memory": "8G",
        "spark.executor.memory": "6G",
        "spark.executor.cores": "5",
        "spark.executor.maxNumFailures": "5",
        "spark.kerberos.principal": cluster_config["kerberos_principal"],
        "spark.kerberos.keytab": cluster_config["kerberos_keytab_path"],
        "spark.kubernetes.kerberos.krb5.path": krb5_conf,
    }


def get_compare_spark_config(**context) -> dict[str, str]:
    return {
        **Variable.get("load_table_test", deserialize_json=True),
        **Variable.get("yc_keys", deserialize_json=True),
        "spark.sql.session.timeZone": "Europe/Moscow",
        "spark.dynamicAllocation.enabled": "false",
        "spark.executor.instances": "2",
        "spark.executor.cores": "2",
        "spark.driver.memory": "4G",
        "spark.executor.memory": "4G",
    }


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 3, 13),
    schedule_interval=SCHEDULE_INTERVAL,
    is_paused_upon_creation=True,
    description=DAG_DESCRIPTION,
    catchup=False,
    tags=DAG_TAGS,
    default_args={
        "email": EMAIL_LIST,
        "email_on_failure": True,
    },
) as dag:

    start_task = EmptyOperator(task_id="start")

    validate_contract_task = PythonOperator(
        task_id="validate_contract",
        python_callable=validate_contract,
    )

    # Один Spark job на все таблицы — скрипт итерирует таблицы сам.
    # Loader пишет напрямую в существующие target tables согласно write_mode из runtime-контракта.
    load_iceberg_tables_task = PlatformTemplatedSparkOperator(
        task_id="load_iceberg_tables",
        name="datagen_load_iceberg_tables",
        conn_id="spark_k8s",
        config_callable=get_iceberg_spark_config,
        retries=0,
        application=ICEBERG_LOADER_SCRIPT,
        py_files=f"{BASE_LOADER_SCRIPT},{JOB_COMMON_SCRIPT}",
        application_args=[
            "--app_name", "datagen_load_iceberg_{{ dag_run.conf['run_id'] }}",
            "--contract", "{{ dag_run.conf | tojson }}",
        ],
    )

    load_hive_tables_task = PlatformTemplatedSparkOperator(
        task_id="load_hive_tables",
        name="datagen_load_hive_tables",
        conn_id="spark_k8s",
        config_callable=get_hive_spark_config,
        retries=0,
        application=HIVE_LOADER_SCRIPT,
        py_files=f"{BASE_LOADER_SCRIPT},{JOB_COMMON_SCRIPT}",
        application_args=[
            "--app_name", "datagen_load_hive_{{ dag_run.conf['run_id'] }}",
            "--contract", "{{ dag_run.conf | tojson }}",
        ],
    )

    compare_query_results_task = PlatformTemplatedSparkOperator(
        task_id="compare_query_results",
        name="datagen_compare_query_results",
        conn_id="spark_k8s",
        config_callable=get_compare_spark_config,
        retries=0,
        application=RESULT_COMPARATOR_SCRIPT,
        py_files=JOB_COMMON_SCRIPT,
        application_args=[
            "--app_name", "datagen_compare_{{ dag_run.conf['run_id'] }}",
            "--contract", "{{ dag_run.conf | tojson }}",
        ],
    )

    job_succeeded = EmptyOperator(
        task_id="sys_job_succeeded",
        trigger_rule="all_success",
    )

    job_failed = EmptyOperator(
        task_id="sys_job_failed",
        trigger_rule="one_failed",
    )

    # Загрузка таблиц по engine идёт параллельно, потом сравниваются подготовленные query results.
    start_task >> validate_contract_task >> [load_iceberg_tables_task, load_hive_tables_task]
    [load_iceberg_tables_task, load_hive_tables_task] >> compare_query_results_task >> job_succeeded
    [load_iceberg_tables_task, load_hive_tables_task, compare_query_results_task] >> job_failed
