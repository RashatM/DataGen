from app.application.dto.execution_result import ExecutionDiagnostic, ExecutionResult


def format_diagnostics(diagnostics: list[ExecutionDiagnostic]) -> str:
    if not diagnostics:
        return "diagnostics: none"

    lines = ["diagnostics:"]
    for diagnostic in diagnostics:
        lines.append(f"  - task: {diagnostic.task_id}")
        lines.append(f"    message: {diagnostic.message}")
        lines.append(f"    code: {diagnostic.code}")
    return "\n".join(lines)


def format_failed_execution_summary(
    run_id: str,
    execution_result: ExecutionResult,
    total_seconds: int,
    diagnostics: list[ExecutionDiagnostic],
) -> str:
    execution_url_text = ""
    if execution_result.execution_url:
        execution_url_text = f"\n\nexecution_url: {execution_result.execution_url}"

    failed_tasks_text = ""
    if not diagnostics:
        failed_tasks_text = f"\nfailed_tasks: {execution_result.failed_task_ids}"

    return (
        f"Pipeline failed.\n"
        f"run_id: {run_id}\n"
        f"execution_status: {execution_result.status.name}\n"
        f"total_seconds: {total_seconds}\n\n"
        f"{format_diagnostics(diagnostics)}"
        f"{failed_tasks_text}"
        f"{execution_url_text}"
    )
