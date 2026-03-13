from datetime import datetime
from typing import Any, Dict
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import os

from plugins.platform_services.operators.templated_spark_operator import PlatformTemplatedSparkOperator

DAG_ID = "datagen__synth_load"
DAG_DESCRIPTION = "DataGen: loads synthetic data from S3 to Iceberg and Hadoop"
DAG_TAGS = ["datagen", "synthetic"]
SCHEDULE_INTERVAL = None
EMAIL_LIST = []

BASE_LOADER_SCRIPT = "/opt/airflow/dags/repo/scripts/datagen/base_loader.py"
ICEBERG_LOADER_SCRIPT = "/opt/airflow/dags/repo/scripts/datagen/iceberg_load.py"
HADOOP_LOADER_SCRIPT = "/opt/airflow/dags/repo/scripts/datagen/hadoop_load.py"
HADOOP_CLUSTER_CONFIG_VARIABLE = "datagen_hadoop_cluster_config"

SPARK_CONF_DIRS = {
    "BDA51": "/opt/airflow/dags/repo/configs/datagen/bda51/spark-conf",
    "BDA61": "/opt/airflow/dags/repo/configs/datagen/bda61/spark-conf",
    "BDA71": "/opt/airflow/dags/repo/configs/datagen/bda71/spark-conf",
}


def validate_contract(**context) -> None:
    conf = context["dag_run"].conf or {}

    if not conf.get("run_id"):
        raise ValueError("Contract is missing 'run_id'")
    if not conf.get("tables"):
        raise ValueError("Contract is missing 'tables'")


def get_iceberg_spark_config() -> Dict[str, str]:
    return {
        **Variable.get("datagen_iceberg_spark_conf", deserialize_json=True),
        **Variable.get("yc_keys", deserialize_json=True),
        "spark.dynamicAllocation.enabled": "false",
        "spark.executor.instances": "2",
        "spark.executor.cores": "4",
        "spark.driver.memory": "8G",
        "spark.executor.memory": "8G",
    }


def get_hadoop_cluster_config() -> Dict[str, Any]:
    cluster_config = Variable.get(HADOOP_CLUSTER_CONFIG_VARIABLE, deserialize_json=True)
    required_keys = ["cluster_name", "kerberos_principal", "kerberos_keytab_path"]
    missing_keys = [key for key in required_keys if not cluster_config.get(key)]

    if missing_keys:
        raise ValueError(
            f"Variable {HADOOP_CLUSTER_CONFIG_VARIABLE} is missing required keys: {', '.join(missing_keys)}"
        )

    return cluster_config


def get_hadoop_spark_config() -> Dict[str, str]:
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
        **Variable.get("datagen_hadoop_spark_conf", deserialize_json=True),
        **Variable.get("yc_keys", deserialize_json=True),
        "spark.dynamicAllocation.enabled": "true",
        "spark.dynamicAllocation.initialExecutors": "2",
        "spark.dynamicAllocation.minExecutors": "1",
        "spark.dynamicAllocation.maxExecutors": "5",
        "spark.driver.memory": "16G",
        "spark.executor.memory": "20G",
        "spark.executor.cores": "5",
        "spark.kerberos.principal": cluster_config["kerberos_principal"],
        "spark.kerberos.keytab": cluster_config["kerberos_keytab_path"],
        "spark.kubernetes.kerberos.krb5.path": krb5_conf,
    }


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
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
        py_files=[BASE_LOADER_SCRIPT],
        application_args=[
            "--run_id", "{{ dag_run.conf['run_id'] }}",
            "--contract", "{{ dag_run.conf | tojson }}",
        ],
    )

    hadoop_load_task = PlatformTemplatedSparkOperator(
        task_id="hadoop_load",
        name="datagen_hadoop_load",
        conn_id="spark_k8s",
        config_callable=get_hadoop_spark_config,
        application=HADOOP_LOADER_SCRIPT,
        py_files=[BASE_LOADER_SCRIPT],
        application_args=[
            "--run_id", "{{ dag_run.conf['run_id'] }}",
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
    [iceberg_load_task, hadoop_load_task] >> job_succeeded
    [iceberg_load_task, hadoop_load_task] >> job_failed
