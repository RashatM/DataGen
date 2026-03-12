import time
from typing import Any, Dict, List

from app.core.application.constants import DagRunStatus
from app.core.application.dto import TablePublication, DagRunResult
from app.core.application.ports.dag_runner_port import DagRunnerPort
from app.infrastructure.airflow.airflow_client import AirflowClient
from app.infrastructure.dto import DagRunState
from app.shared.logger import logger


class AirflowDagRunner(DagRunnerPort):

    def __init__(self, client: AirflowClient) -> None:
        self.client = client

    @staticmethod
    def build_payload(
        run_id: str,
        publications: List[TablePublication],
    ) -> Dict[str, Any]:
        tables = [
            {
                "schema_name": pub.schema_name,
                "table_name": pub.table_name,
                "storage_type": pub.storage_type,
                "storage": pub.storage,
            }
            for pub in publications
        ]
        return {
            "run_id": run_id,
            "tables": tables,
        }

    def to_dag_run_result(self, dag_run_state: DagRunState) -> DagRunResult:
        return DagRunResult(
            run_id=dag_run_state.dag_run_id,
            dag_id=self.client.dag_id(),
            status=DagRunStatus.SUCCESS if dag_run_state.is_success() else DagRunStatus.FAILED,
            raw_response=dag_run_state.raw,
        )

    def poll_until_terminal(
        self,
        dag_run_id: str,
        timeout_seconds: int,
    ) -> DagRunResult:
        elapsed = 0
        poll_interval = self.client.poll_interval()

        while elapsed < timeout_seconds:
            dag_run_state = self.client.get_dag_run_state(dag_run_id)

            if dag_run_state.is_terminal():
                logger.info(f"DAG reached terminal state dag_run_id={dag_run_id} state={dag_run_state.state}")
                return self.to_dag_run_result(dag_run_state)

            logger.info(f"Polling DAG dag_run_id={dag_run_id} state={dag_run_state.state} elapsed={elapsed}s")
            time.sleep(poll_interval)
            elapsed += poll_interval

        logger.info(f"DAG timed out dag_run_id={dag_run_id} timeout={timeout_seconds}s")
        return DagRunResult(
            run_id=dag_run_id,
            dag_id=self.client.dag_id(),
            status=DagRunStatus.TIMEOUT,
        )

    def trigger_and_wait(
        self,
        run_id: str,
        publications: List[TablePublication],
        timeout_seconds: int,
    ) -> DagRunResult:
        dag_run_id = self.client.build_dag_run_id(run_id)
        payload = self.build_payload(run_id, publications)

        logger.info(
            f"Triggering DAG dag_id={self.client.dag_id()} dag_run_id={dag_run_id} "
            f"tables={len(publications)}"
        )
        self.client.trigger_dag(dag_run_id=dag_run_id, payload=payload)

        return self.poll_until_terminal(
            dag_run_id=dag_run_id,
            timeout_seconds=timeout_seconds,
        )
