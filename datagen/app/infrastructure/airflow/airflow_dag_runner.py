import time
from typing import Dict, List

from app.core.application.constants import DagRunStatus
from app.core.application.dto import DagRunResult, RunArtifactLayout, TablePublication
from app.infrastructure.airflow.airflow_dag_payload_builder import AirflowDagPayloadBuilder
from app.core.application.ports.dag_runner_port import DagRunnerPort
from app.infrastructure.airflow.airflow_client import AirflowClient
from app.infrastructure.dto import DagRunState
from app.shared.logger import airflow_logger

logger = airflow_logger


class AirflowDagRunner(DagRunnerPort):

    def __init__(
        self,
        client: AirflowClient,
        payload_builder: AirflowDagPayloadBuilder,
    ) -> None:
        self.client = client
        self.payload_builder = payload_builder

    def to_dag_run_result(
        self,
        run_id: str,
        dag_run_state: DagRunState,
    ) -> DagRunResult:
        if dag_run_state.is_success():
            status = DagRunStatus.SUCCESS
        else:
            status = DagRunStatus.FAILED
            logger.error(
                f"DAG finished with error: dag_run_id={dag_run_state.dag_run_id}, "
                f"state={dag_run_state.state}, raw_response={dag_run_state.raw}"
            )
        return DagRunResult(
            run_id=run_id,
            dag_id=self.client.dag_id(),
            dag_run_id=dag_run_state.dag_run_id,
            status=status,
            raw_response=dag_run_state.raw,
        )

    def poll_until_terminal(
        self,
        run_id: str,
        dag_run_id: str,
        timeout_seconds: int,
    ) -> DagRunResult:
        poll_interval = self.client.poll_interval()
        start = time.monotonic()
        deadline = start + timeout_seconds
        previous_state = None

        while time.monotonic() < deadline:
            dag_run_state = self.client.get_dag_run_state(dag_run_id)

            if dag_run_state.is_terminal():
                total = int(time.monotonic() - start)
                logger.info(
                    f"DAG reached terminal state: dag_run_id={dag_run_id}, "
                    f"state={dag_run_state.state}, total={total}s"
                )
                return self.to_dag_run_result(run_id, dag_run_state)

            if dag_run_state.state != previous_state:
                elapsed = int(time.monotonic() - start)
                logger.info(
                    f"DAG state updated: dag_run_id={dag_run_id}, "
                    f"state={dag_run_state.state}, elapsed={elapsed}s"
                )
                previous_state = dag_run_state.state
            time.sleep(poll_interval)

        logger.error(
            f"DAG polling timed out: dag_run_id={dag_run_id}, timeout={timeout_seconds}s"
        )
        return DagRunResult(
            run_id=run_id,
            dag_id=self.client.dag_id(),
            dag_run_id=dag_run_id,
            status=DagRunStatus.TIMEOUT,
        )

    def trigger_and_wait(
        self,
        layout: RunArtifactLayout,
        publications: List[TablePublication],
        comparison_query_uris: Dict[str, str],
        timeout_seconds: int,
    ) -> DagRunResult:
        dag_run_id = self.client.build_dag_run_id(layout.run_id)
        payload = self.payload_builder.build(
            layout=layout,
            publications=publications,
            comparison_query_uris=comparison_query_uris,
        )

        logger.info(
            f"DAG trigger requested: dag_id={self.client.dag_id()}, "
            f"dag_run_id={dag_run_id}, tables_count={len(publications)}"
        )
        self.client.trigger_dag(dag_run_id=dag_run_id, payload=payload)
        logger.info(f"DAG trigger accepted: dag_run_id={dag_run_id}")

        return self.poll_until_terminal(
            run_id=layout.run_id,
            dag_run_id=dag_run_id,
            timeout_seconds=timeout_seconds,
        )
