import time

from app.core.application.dto.pipeline import PipelineExecutionResult
from app.core.application.layouts.storage_layout import RunArtifactKeyLayout
from app.core.application.ports.execution_runner_port import ExecutionRunnerPort
from app.core.application.services.comparison_service import ComparisonReportService
from app.core.application.services.generation_service import DataGenerationService
from app.core.application.services.publication_service import ArtifactPublicationService
from app.core.domain.entities import GenerationRun
from app.shared.logger import pipeline_logger

logger = pipeline_logger


class ExecutePipelineUseCase:
    def __init__(
        self,
        generation_service: DataGenerationService,
        artifact_publication_service: ArtifactPublicationService,
        comparison_report_service: ComparisonReportService,
        execution_runner: ExecutionRunnerPort,
        execution_timeout_seconds: int,
    ) -> None:
        self.generation_service = generation_service
        self.artifact_publication_service = artifact_publication_service
        self.comparison_report_service = comparison_report_service
        self.execution_runner = execution_runner
        self.execution_timeout_seconds = execution_timeout_seconds

    def execute(self, generation_run: GenerationRun) -> PipelineExecutionResult:
        run_id = generation_run.run_id
        artifact_layout = RunArtifactKeyLayout(run_id=run_id)
        start = time.monotonic()
        logger.info(f"Pipeline started: run_id={run_id}")

        table_publications = []
        try:
            for table_data in self.generation_service.generate_tables(generation_run):
                table_publications.append(
                    self.artifact_publication_service.stage_table(
                        artifact_layout=artifact_layout,
                        table_data=table_data,
                    )
                )

            comparison_query_uris = self.artifact_publication_service.stage_comparison_queries(
                artifact_layout=artifact_layout,
                table_publications=table_publications,
            )
            self.artifact_publication_service.commit_pointers(table_publications)
        except Exception:
            logger.exception(f"Pipeline artifact staging failed: run_id={run_id}")
            self.artifact_publication_service.cleanup_run_artifacts(artifact_layout=artifact_layout)
            raise

        execution_result = self.execution_runner.trigger_and_wait(
            artifact_layout=artifact_layout,
            publications=table_publications,
            comparison_query_uris=comparison_query_uris,
            timeout_seconds=self.execution_timeout_seconds,
        )

        total = int(time.monotonic() - start)
        if not execution_result.is_success():
            logger.error(
                f"Pipeline failed: run_id={run_id}, "
                f"status={execution_result.status.value}, total={total}s"
            )
            return PipelineExecutionResult(run_id=run_id, execution_result=execution_result)

        comparison_report = self.comparison_report_service.load_report(
            report_key=artifact_layout.comparison_report_key,
            run_id=run_id,
        )
        comparison_summary = self.comparison_report_service.format_report_summary(comparison_report)

        if comparison_report.is_match():
            logger.info(
                f"Pipeline completed.\n"
                f"run_id: {run_id}\n"
                f"execution_status: {execution_result.status.value}\n"
                f"total_seconds: {total}\n"
                f"{comparison_summary}"
            )
        else:
            logger.warning(
                f"Pipeline completed with mismatch.\n"
                f"run_id: {run_id}\n"
                f"execution_status: {execution_result.status.value}\n"
                f"total_seconds: {total}\n"
                f"{comparison_summary}"
            )

        return PipelineExecutionResult(
            run_id=run_id,
            execution_result=execution_result,
            comparison_report=comparison_report,
        )
