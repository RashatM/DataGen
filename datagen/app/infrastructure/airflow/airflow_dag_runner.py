import time
from dataclasses import asdict
from typing import Any, Dict, List

from app.core.application.constants import DagRunStatus
from app.core.application.dto import TablePublication, DagRunResult
from app.core.application.ports.dag_runner_port import DagRunnerPort
from app.infrastructure.airflow.airflow_client import AirflowClient
from app.infrastructure.dto import DagRunState
from app.shared.logger import airflow_logger

logger = airflow_logger


class AirflowDagRunner(DagRunnerPort):

    def __init__(self, client: AirflowClient) -> None:
        self.client = client

    @staticmethod
    def build_table_entry(pub: TablePublication) -> Dict[str, Any]:
        return {
            "schema_name": pub.schema_name,
            "table_name": pub.table_name,
            "artifacts": asdict(pub.artifacts),
        }

    def build_payload(
            self,
            run_id: str,
            publications: List[TablePublication],
    ) -> Dict[str, Any]:
        return {
            "run_id": run_id,
            "tables": [self.build_table_entry(pub) for pub in publications],
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
        poll_interval = self.client.poll_interval()
        deadline = time.monotonic() + timeout_seconds
        previous_state = None

        while time.monotonic() < deadline:
            dag_run_state = self.client.get_dag_run_state(dag_run_id)

            if dag_run_state.is_terminal():
                logger.info(f"DAG reached terminal state. dag_run_id={dag_run_id}, state={dag_run_state.state}")
                return self.to_dag_run_result(dag_run_state)

            if dag_run_state.state != previous_state:
                elapsed = int(timeout_seconds - (deadline - time.monotonic()))
                logger.info(
                    f"DAG state updated. dag_run_id={dag_run_id}, state={dag_run_state.state}, elapsed_seconds={elapsed}"
                )
                previous_state = dag_run_state.state
            time.sleep(poll_interval)

        logger.warning(f"DAG polling timed out. dag_run_id={dag_run_id}, timeout_seconds={timeout_seconds}")
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
            f"DAG trigger requested. dag_id={self.client.dag_id()}, dag_run_id={dag_run_id}, tables_count={len(publications)}"
        )
        self.client.trigger_dag(dag_run_id=dag_run_id, payload=payload)
        logger.info(f"DAG trigger accepted. dag_id={self.client.dag_id()}, dag_run_id={dag_run_id}")

        return self.poll_until_terminal(
            dag_run_id=dag_run_id,
            timeout_seconds=timeout_seconds,
        )
