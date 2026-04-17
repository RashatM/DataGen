import time

from app.application.dto.execution_result import PipelineExecutionResult
from app.application.dto.pipeline import PipelineExecutionSpec
from app.application.layouts.storage_layout import RunArtifactKeyLayout
from app.application.ports.diagnostic_repository_port import ExecutionDiagnosticRepositoryPort
from app.application.ports.execution_runner_port import ExecutionRunnerPort
from app.application.services.comparison_service import ComparisonReportService
from app.application.services.comparison_report_formatter import format_report_summary
from app.application.services.execution_result_formatter import format_failed_execution_summary
from app.application.services.generation_service import DataGenerationService
from app.application.services.publication_service import ArtifactPublicationService
from app.domain.entities import GenerationRun
from app.domain.validation_errors import DomainError
from app.shared.logger import pipeline_logger

logger = pipeline_logger


class ExecutePipelineUseCase:
    """Склеивает генерацию, staging, запуск внешнего DAG и чтение итогового comparison-report.

    Это главный orchestration use case проекта. Он отвечает за границы этапов:
    - генерирует и публикует артефакты только один раз на run
    - при ошибке staging чистит опубликованные артефакты
    - запускает внешний runtime через ExecutionRunnerPort
    - коммитит per-table pointers только после технически успешного выполнения DAG
    - читает и валидирует comparison-report перед возвратом результата вызывающему коду
    """
    def __init__(
        self,
        generation_service: DataGenerationService,
        artifact_publication_service: ArtifactPublicationService,
        comparison_report_service: ComparisonReportService,
        diagnostic_repository: ExecutionDiagnosticRepositoryPort,
        execution_runner: ExecutionRunnerPort,
        execution_timeout_seconds: int,
        min_retained_runs: int,
    ) -> None:
        self.generation_service = generation_service
        self.artifact_publication_service = artifact_publication_service
        self.comparison_report_service = comparison_report_service
        self.diagnostic_repository = diagnostic_repository
        self.execution_runner = execution_runner
        self.execution_timeout_seconds = execution_timeout_seconds
        self.min_retained_runs = min_retained_runs

    def execute(self, run_id: str, pipeline_spec: PipelineExecutionSpec) -> PipelineExecutionResult:
        """Выполняет pipeline end-to-end от доменной генерации до чтения финального comparison-report."""
        generation_run = GenerationRun(
            run_id=run_id,
            tables=[table_execution.table for table_execution in pipeline_spec.tables],
        )
        execution_tables_by_name = {
            table_execution.table.table_name: table_execution
            for table_execution in pipeline_spec.tables
        }
        artifact_layout = RunArtifactKeyLayout(run_id=run_id)
        start = time.monotonic()
        logger.info(f"Pipeline started: run_id={run_id}")

        table_publications = []
        try:
            for table_data in self.generation_service.generate_tables(generation_run):
                table_execution = execution_tables_by_name[table_data.table.table_name]
                table_publications.append(
                    self.artifact_publication_service.stage_table(
                        artifact_layout=artifact_layout,
                        table_execution=table_execution,
                        table_data=table_data,
                    )
                )

            comparison_query_uris = self.artifact_publication_service.stage_comparison_queries(
                artifact_layout=artifact_layout,
                comparison_spec=pipeline_spec.comparison,
            )
        except DomainError as exc:
            logger.error(f"Pipeline artifact staging failed: run_id={run_id}, error={exc}")
            self.artifact_publication_service.cleanup_run_artifacts(artifact_layout=artifact_layout)
            raise
        except Exception:
            logger.exception(f"Pipeline artifact staging failed: run_id={run_id}")
            self.artifact_publication_service.cleanup_run_artifacts(artifact_layout=artifact_layout)
            raise

        execution_result = self.execution_runner.trigger_and_wait(
            artifact_layout=artifact_layout,
            publications=table_publications,
            comparison_spec=pipeline_spec.comparison,
            comparison_query_uris=comparison_query_uris,
            timeout_seconds=self.execution_timeout_seconds,
        )

        total = int(time.monotonic() - start)
        if not execution_result.is_success():
            if execution_result.is_wait_timeout():
                execution_url_text = f", execution_url={execution_result.execution_url}" if execution_result.execution_url else ""
                logger.warning(
                    f"Pipeline wait timeout reached: run_id={run_id}, "
                    f"status={execution_result.status.value}, execution_id={execution_result.execution_id}, "
                    f"total={total}s{execution_url_text}"
                )
                return PipelineExecutionResult(run_id=run_id, execution_result=execution_result)
            diagnostics = []
            try:
                diagnostics = self.diagnostic_repository.load_diagnostics(
                    diagnostics_prefix=artifact_layout.diagnostics_prefix,
                    expected_run_id=artifact_layout.run_id,
                )
                if diagnostics:
                    diagnostic_tasks = ", ".join(diagnostic.task_id for diagnostic in diagnostics)
                    logger.info(
                        f"Execution diagnostics found: "
                        f"diagnostics_count={len(diagnostics)}, tasks={diagnostic_tasks}"
                    )
            except Exception:
                logger.exception(f"Failed to load execution diagnostics: run_id={run_id}")

            failed_summary = format_failed_execution_summary(
                run_id=run_id,
                execution_result=execution_result,
                total_seconds=total,
                diagnostics=diagnostics,
            )
            logger.error(failed_summary)
            return PipelineExecutionResult(
                run_id=run_id,
                execution_result=execution_result,
                diagnostics=diagnostics,
            )

        self.artifact_publication_service.commit_pointers(table_publications)
        comparison_report = self.comparison_report_service.load_report(
            report_key=artifact_layout.comparison_report_key,
            run_id=run_id,
        )
        comparison_summary = format_report_summary(comparison_report)

        if comparison_report.is_match():
            logger.info(
                f"Pipeline completed successfully.\n"
                f"run_id: {run_id}\n"
                f"execution_status: {execution_result.status.name}\n"
                f"total_seconds: {total}\n"
                f"{comparison_summary}"
            )
        else:
            logger.warning(
                f"Pipeline completed with mismatch.\n"
                f"run_id: {run_id}\n"
                f"execution_status: {execution_result.status.name}\n"
                f"total_seconds: {total}\n"
                f"{comparison_summary}"
            )

        try:
            self.artifact_publication_service.cleanup_old_run_artifacts(
                min_retained_runs=self.min_retained_runs
            )
        except Exception:
            logger.exception(
                f"Run artifact retention cleanup failed: "
                f"run_id={run_id}, min_retained_runs={self.min_retained_runs}"
            )

        return PipelineExecutionResult(
            run_id=run_id,
            execution_result=execution_result,
            comparison_report=comparison_report,
        )
