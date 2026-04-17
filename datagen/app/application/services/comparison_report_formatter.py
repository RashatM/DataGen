from app.application.dto.comparison import ComparisonReport


def format_report_summary(report: ComparisonReport) -> str:
    excluded_columns_lines = []
    if report.excluded_columns.hive:
        excluded_columns_lines.append(f"    hive: {report.excluded_columns.hive}")
    if report.excluded_columns.iceberg:
        excluded_columns_lines.append(f"    iceberg: {report.excluded_columns.iceberg}")
    if report.excluded_columns.temporal:
        excluded_columns_lines.append(f"    temporal: {report.excluded_columns.temporal}")
    excluded_columns_text = ""
    if excluded_columns_lines:
        excluded_columns_text = "\n  excluded_columns:\n" + "\n".join(excluded_columns_lines)

    return (
        f"comparison:\n"
        f"  status: {report.status.value}\n"
        f"  row_count:\n"
        f"    hive: {report.summary.row_count.hive}\n"
        f"    iceberg: {report.summary.row_count.iceberg}\n"
        f"  row_count_delta: {report.summary.row_count_delta}\n"
        f"  exclusive_row_count:\n"
        f"    hive: {report.summary.exclusive_row_count.hive}\n"
        f"    iceberg: {report.summary.exclusive_row_count.iceberg}\n"
        f"  exclusive_row_ratio:\n"
        f"    hive: {report.summary.exclusive_row_ratio.hive}\n"
        f"    iceberg: {report.summary.exclusive_row_ratio.iceberg}"
        f"{excluded_columns_text}"
    )
