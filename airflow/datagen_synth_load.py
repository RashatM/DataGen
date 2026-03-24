from datetime import datetime
from typing import Any
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import os

from plugins.platform_services.operators.templated_spark_operator import PlatformTemplatedSparkOperator

DAG_ID = "datagen_synth_load"
DAG_DESCRIPTION = "DataGen: loads synthetic data from S3 to Iceberg and Hadoop"
DAG_TAGS = ["datagen", "synthetic"]
SCHEDULE_INTERVAL = None
EMAIL_LIST = []

BASE_LOADER_SCRIPT = "/opt/airflow/dags/repo/scripts/platform_services/datagen/base_loader.py"
JOB_COMMON_SCRIPT = "/opt/airflow/dags/repo/scripts/platform_services/datagen/job_common.py"
ICEBERG_LOADER_SCRIPT = "/opt/airflow/dags/repo/scripts/platform_services/datagen/iceberg_load.py"
HADOOP_LOADER_SCRIPT = "/opt/airflow/dags/repo/scripts/platform_services/datagen/hadoop_load.py"
COMPARE_RESULTS_SCRIPT = "/opt/airflow/dags/repo/scripts/platform_services/datagen/compare_results.py"
HADOOP_CLUSTER_CONFIG_VARIABLE = "datagen_hadoop_cluster_config"

SPARK_CONF_DIRS = {
    "BDA51": "/opt/airflow/dags/repo/configs/platform_services/s3_to_hadoop/bda51/spark-conf",
    "BDA61": "/opt/airflow/dags/repo/configs/platform_services/s3_to_hadoop/bda61/spark-conf",
    "BDA71": "/opt/airflow/dags/repo/configs/platform_services/s3_to_hadoop/bda71/spark-conf"
}


def require_non_empty_string(value: Any, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Contract is missing non-empty '{field_name}'")


def validate_table_contracts(conf: dict[str, Any]) -> None:
    tables = conf.get("tables")
    if not isinstance(tables, list) or not tables:
        raise ValueError("Contract is missing non-empty 'tables'")

    for index, table in enumerate(tables):
        if not isinstance(table, dict):
            raise ValueError(f"Contract table at index={index} must be an object")

        require_non_empty_string(table.get("schema_name"), f"tables[{index}].schema_name")
        require_non_empty_string(table.get("table_name"), f"tables[{index}].table_name")

        artifacts = table.get("artifacts")
        if not isinstance(artifacts, dict):
            raise ValueError(f"Contract is missing 'tables[{index}].artifacts'")

        require_non_empty_string(artifacts.get("data_uri"), f"tables[{index}].artifacts.data_uri")

        engines = artifacts.get("engines")
        if not isinstance(engines, dict):
            raise ValueError(f"Contract is missing 'tables[{index}].artifacts.engines'")

        for engine in ("hive", "iceberg"):
            engine_artifacts = engines.get(engine)
            if not isinstance(engine_artifacts, dict):
                raise ValueError(
                    f"Contract is missing 'tables[{index}].artifacts.engines.{engine}'"
                )

            require_non_empty_string(
                engine_artifacts.get("ddl_uri"),
                f"tables[{index}].artifacts.engines.{engine}.ddl_uri",
            )
            require_non_empty_string(
                engine_artifacts.get("target_table_name"),
                f"tables[{index}].artifacts.engines.{engine}.target_table_name",
            )


def validate_contract(**context) -> None:
    conf = context["dag_run"].conf or {}

    require_non_empty_string(conf.get("run_id"), "run_id")
    validate_table_contracts(conf)

    comparison = conf.get("comparison")
    if not isinstance(comparison, dict):
        raise ValueError("Contract is missing 'comparison'")

    query_uris = comparison.get("query_uris")
    if not isinstance(query_uris, dict):
        raise ValueError("Contract is missing 'comparison.query_uris'")
    for engine in ("hive", "iceberg"):
        require_non_empty_string(
            query_uris.get(engine),
            f"comparison.query_uris.{engine}",
        )

    require_non_empty_string(comparison.get("report_uri"), "comparison.report_uri")

    result_uris = comparison.get("result_uris")
    if not isinstance(result_uris, dict):
        raise ValueError("Contract is missing 'comparison.result_uris'")
    for engine in ("hive", "iceberg"):
        require_non_empty_string(
            result_uris.get(engine),
            f"comparison.result_uris.{engine}",
        )


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
    # cluster_config = Variable.get(HADOOP_CLUSTER_CONFIG_VARIABLE, deserialize_json=True)
    cluster_config = {
        "cluster_name": "BDA71",
        "kerberos_principal": "t_bdp_bda_repl@MSK.AD2012.LOC",
        "kerberos_keytab_path": "/tmp/t_bdp_bda_repl.keytab"
    }
    required_keys = ["cluster_name", "kerberos_principal", "kerberos_keytab_path"]
    missing_keys = [key for key in required_keys if not cluster_config.get(key)]

    if missing_keys:
        raise ValueError(
            f"Variable {HADOOP_CLUSTER_CONFIG_VARIABLE} is missing required keys: {', '.join(missing_keys)}"
        )

    return cluster_config


def get_hadoop_spark_config(**context) -> dict[str, str]:
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
        "spark.driver.memory": "16G",
        "spark.executor.memory": "20G",
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
        "spark.sql.session.timeZone": "UTC",
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

    # Один Spark job на все таблицы — скрипт итерирует таблицы сам
    # Rollback через tmp таблицу: создать _tmp → загрузить → rename
    iceberg_load_task = PlatformTemplatedSparkOperator(
        task_id="iceberg_load",
        name="datagen_iceberg_load",
        conn_id="spark_k8s",
        config_callable=get_iceberg_spark_config,
        retries=0,
        application=ICEBERG_LOADER_SCRIPT,
        py_files=f"{BASE_LOADER_SCRIPT},{JOB_COMMON_SCRIPT}",
        application_args=[
            "--app_name", "datagen_iceberg_{{ dag_run.conf['run_id'] }}",
            "--contract", "{{ dag_run.conf | tojson }}",
        ],
    )

    hadoop_load_task = PlatformTemplatedSparkOperator(
        task_id="hadoop_load",
        name="datagen_hadoop_load",
        conn_id="spark_k8s",
        config_callable=get_hadoop_spark_config,
        application=HADOOP_LOADER_SCRIPT,
        py_files=f"{BASE_LOADER_SCRIPT},{JOB_COMMON_SCRIPT}",
        application_args=[
            "--app_name", "datagen_hive_{{ dag_run.conf['run_id'] }}",
            "--contract", "{{ dag_run.conf | tojson }}",
        ],
    )

    compare_results_task = PlatformTemplatedSparkOperator(
        task_id="compare_results",
        name="datagen_compare_results",
        conn_id="spark_k8s",
        config_callable=get_compare_spark_config,
        retries=0,
        application=COMPARE_RESULTS_SCRIPT,
        py_files=f"{BASE_LOADER_SCRIPT},{JOB_COMMON_SCRIPT}",
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

    # iceberg и hadoop параллельно после валидации
    start_task >> validate_contract_task >> [iceberg_load_task, hadoop_load_task]
    [iceberg_load_task, hadoop_load_task] >> compare_results_task >> job_succeeded
    [iceberg_load_task, hadoop_load_task, compare_results_task] >> job_failed
