import time

from app.core.application.constants import ExecutionStatus
from app.core.application.layouts.storage_layout import RunArtifactKeyLayout
from app.core.application.dto.execution import ExecutionResult
from app.core.application.dto.publication import EnginePair, TablePublication
from app.infrastructure.airflow.airflow_dag_payload_builder import AirflowDagPayloadBuilder
from app.core.application.ports.execution_runner_port import ExecutionRunnerPort
from app.infrastructure.airflow.airflow_client import AirflowClient
from app.infrastructure.dto import DagRunState
from app.shared.logger import airflow_logger

logger = airflow_logger


class AirflowDagRunner(ExecutionRunnerPort):
    HEARTBEAT_LOG_INTERVAL_SECONDS = 120

    def __init__(
        self,
        client: AirflowClient,
        payload_builder: AirflowDagPayloadBuilder,
    ) -> None:
        self.client = client
        self.payload_builder = payload_builder

    def to_execution_result(
        self,
        run_id: str,
        dag_run_state: DagRunState,
    ) -> ExecutionResult:
        if dag_run_state.is_success():
            status = ExecutionStatus.SUCCESS
        else:
            status = ExecutionStatus.FAILED
            try:
                failed_task_ids = self.client.get_failed_task_ids(dag_run_state.dag_run_id)
            except Exception:
                logger.exception(
                    f"Failed to fetch failed task instances: dag_run_id={dag_run_state.dag_run_id}"
                )
                failed_task_ids = []
            failed_tasks_text = f", failed_tasks={failed_task_ids}" if failed_task_ids else ""
            logger.error(
                f"DAG finished with error: dag_run_id={dag_run_state.dag_run_id}, "
                f"state={dag_run_state.state}{failed_tasks_text}"
            )
        return ExecutionResult(
            run_id=run_id,
            execution_id=dag_run_state.dag_run_id,
            status=status,
            execution_url=self.client.build_dag_run_url(dag_run_state.dag_run_id),
        )

    def poll_until_terminal(
        self,
        run_id: str,
        dag_run_id: str,
        timeout_seconds: int,
    ) -> ExecutionResult:
        poll_interval = self.client.poll_interval()
        start = time.monotonic()
        deadline = start + timeout_seconds
        previous_state = None
        last_heartbeat_at = start

        while time.monotonic() < deadline:
            dag_run_state = self.client.get_dag_run_state(dag_run_id)
            now = time.monotonic()

            if dag_run_state.is_terminal():
                total = int(now - start)
                logger.info(
                    f"DAG reached terminal state: dag_run_id={dag_run_id}, "
                    f"state={dag_run_state.state}, total={total}s"
                )
                return self.to_execution_result(run_id, dag_run_state)

            if dag_run_state.state != previous_state:
                elapsed = int(now - start)
                logger.info(
                    f"DAG state updated: dag_run_id={dag_run_id}, "
                    f"state={dag_run_state.state}, elapsed={elapsed}s"
                )
                previous_state = dag_run_state.state
                last_heartbeat_at = now
            elif now - last_heartbeat_at >= self.HEARTBEAT_LOG_INTERVAL_SECONDS:
                elapsed = int(now - start)
                logger.info(
                    f"DAG still running: dag_run_id={dag_run_id}, "
                    f"state={dag_run_state.state}, elapsed={elapsed}s"
                )
                last_heartbeat_at = now
            time.sleep(poll_interval)

        execution_url = self.client.build_dag_run_url(dag_run_id)
        logger.warning(
            f"DAG wait timeout reached. The DAG may still be running in Airflow: "
            f"dag_run_id={dag_run_id}, timeout={timeout_seconds}s, airflow_url={execution_url}"
        )
        return ExecutionResult(
            run_id=run_id,
            execution_id=dag_run_id,
            status=ExecutionStatus.WAIT_TIMEOUT,
            execution_url=execution_url,
        )

    def trigger_and_wait(
        self,
        artifact_layout: RunArtifactKeyLayout,
        publications: list[TablePublication],
        comparison_query_uris: EnginePair[str],
        timeout_seconds: int,
    ) -> ExecutionResult:
        dag_run_id = self.client.build_dag_run_id(artifact_layout.run_id)
        payload = self.payload_builder.build(
            artifact_layout=artifact_layout,
            publications=publications,
            comparison_query_uris=comparison_query_uris,
        )

        logger.info(
            f"DAG trigger requested: dag_id={self.client.dag_id()}, "
            f"dag_run_id={dag_run_id}, tables_count={len(publications)}"
        )
        self.client.trigger_dag(dag_run_id=dag_run_id, payload=payload)
        logger.info(f"DAG trigger accepted: dag_run_id={dag_run_id}")
        logger.info(
            f"DAG submitted successfully. Waiting for terminal state: "
            f"dag_run_id={dag_run_id}, timeout={timeout_seconds}s"
        )

        return self.poll_until_terminal(
            run_id=artifact_layout.run_id,
            dag_run_id=dag_run_id,
            timeout_seconds=timeout_seconds,
        )
