import time

from app.core.application.constants import ExecutionStatus
from app.core.application.dto.pipeline import ComparisonQuerySpec
from app.core.application.layouts.storage_layout import RunArtifactKeyLayout
from app.core.application.dto.execution_result import ExecutionResult
from app.core.application.dto.publication import EnginePair, TablePublication
from app.infrastructure.airflow.airflow_dag_payload_builder import AirflowDagPayloadBuilder
from app.core.application.ports.execution_runner_port import ExecutionRunnerPort
from app.infrastructure.airflow.airflow_client import AirflowClient
from app.infrastructure.dto import DagRunState
from app.shared.logger import airflow_logger

logger = airflow_logger


class AirflowDagRunner(ExecutionRunnerPort):
    HEARTBEAT_LOG_INTERVAL_SECONDS = 120
    QUEUED_STATE = "queued"
    RUNNING_STATE = "running"

    def __init__(
        self,
        client: AirflowClient,
        payload_builder: AirflowDagPayloadBuilder,
    ) -> None:
        self.client = client
        self.payload_builder = payload_builder

    @staticmethod
    def log_running_started(dag_run_id: str, elapsed_seconds: int) -> None:
        if elapsed_seconds > 0:
            logger.info(
                f"DAG started running: dag_run_id={dag_run_id}, startup_wait={elapsed_seconds}s"
            )
            return
        logger.info(f"DAG started running: dag_run_id={dag_run_id}")

    def to_execution_result(
        self,
        run_id: str,
        dag_run_state: DagRunState,
        total_seconds: int | None = None,
    ) -> ExecutionResult:
        total_text = f", total={total_seconds}s" if total_seconds is not None else ""
        if dag_run_state.is_success():
            status = ExecutionStatus.SUCCESS
            logger.info(
                f"DAG completed successfully: dag_run_id={dag_run_state.dag_run_id}, "
                f"status={dag_run_state.state}{total_text}"
            )
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
                f"status={dag_run_state.state}{total_text}{failed_tasks_text}"
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
                return self.to_execution_result(
                    run_id=run_id,
                    dag_run_state=dag_run_state,
                    total_seconds=int(now - start),
                )

            elapsed = int(now - start)
            if previous_state is None:
                if dag_run_state.state == self.RUNNING_STATE:
                    self.log_running_started(dag_run_id=dag_run_id, elapsed_seconds=elapsed)
                previous_state = dag_run_state.state
                last_heartbeat_at = now
            elif dag_run_state.state != previous_state:
                if previous_state == self.QUEUED_STATE and dag_run_state.state == self.RUNNING_STATE:
                    self.log_running_started(dag_run_id=dag_run_id, elapsed_seconds=elapsed)
                else:
                    logger.info(
                        f"DAG status updated: dag_run_id={dag_run_id}, "
                        f"status={dag_run_state.state}, elapsed={elapsed}s"
                    )
                previous_state = dag_run_state.state
                last_heartbeat_at = now
            elif now - last_heartbeat_at >= self.HEARTBEAT_LOG_INTERVAL_SECONDS:
                if dag_run_state.state == self.QUEUED_STATE:
                    logger.info(
                        f"DAG still queued: dag_run_id={dag_run_id}, elapsed={elapsed}s"
                    )
                else:
                    logger.info(
                        f"DAG still in progress: dag_run_id={dag_run_id}, "
                        f"status={dag_run_state.state}, elapsed={elapsed}s"
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
        comparison_spec: ComparisonQuerySpec,
        comparison_query_uris: EnginePair[str],
        timeout_seconds: int,
    ) -> ExecutionResult:
        dag_run_id = self.client.build_dag_run_id(artifact_layout.run_id)
        payload = self.payload_builder.build(
            artifact_layout=artifact_layout,
            publications=publications,
            comparison_spec=comparison_spec,
            comparison_query_uris=comparison_query_uris,
        )

        logger.info(
            f"DAG trigger requested: dag_id={self.client.dag_id()}, "
            f"dag_run_id={dag_run_id}, tables_count={len(publications)}"
        )
        self.client.trigger_dag(dag_run_id=dag_run_id, payload=payload)
        logger.info(
            f"DAG trigger accepted. Waiting for DAG to start and complete: "
            f"dag_run_id={dag_run_id}, timeout={timeout_seconds}s"
        )

        return self.poll_until_terminal(
            run_id=artifact_layout.run_id,
            dag_run_id=dag_run_id,
            timeout_seconds=timeout_seconds,
        )
