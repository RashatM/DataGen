from dataclasses import dataclass

from app.core.application.constants import ComparisonStatus


@dataclass(slots=True)
class EngineCountSummary:
    """Числовая сводка по Hive и Iceberg для целочисленных метрик сравнения."""
    hive: int
    iceberg: int


@dataclass(slots=True)
class EngineRatioSummary:
    """Числовая сводка по Hive и Iceberg для долей и коэффициентов сравнения."""
    hive: float
    iceberg: float


@dataclass(slots=True)
class ComparisonReportArtifacts:
    """Ссылки на materialized parquet-результаты comparison query по двум engine."""
    hive_result_uri: str
    iceberg_result_uri: str


@dataclass(slots=True)
class ComparisonReportSummary:
    """Сводная часть comparison_result.json.

    row_count:
        Сколько строк материализовал каждый движок.
    row_count_delta:
        Абсолютная разница между объёмами результатов Hive и Iceberg.
    exclusive_row_count:
        Число строк, оставшихся только на одной стороне после двустороннего exceptAll.
    exclusive_row_ratio:
        exclusive_row_count / row_count для каждого движка, либо 0.0 при пустом результате.
    """

    row_count: EngineCountSummary
    row_count_delta: int
    exclusive_row_count: EngineCountSummary
    exclusive_row_ratio: EngineRatioSummary


@dataclass(slots=True)
class ComparisonReport:
    """Нормализованное представление итогового comparison-report, прочитанного из S3."""
    run_id: str
    checked_at: str
    status: ComparisonStatus
    summary: ComparisonReportSummary
    artifacts: ComparisonReportArtifacts

    def is_match(self) -> bool:
        return self.status == ComparisonStatus.MATCH
