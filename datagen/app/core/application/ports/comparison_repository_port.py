from abc import ABC, abstractmethod

from app.core.application.dto.comparison import ComparisonReport


class IComparisonReportRepository(ABC):

    @abstractmethod
    def load_report(self, report_key: str, expected_run_id: str) -> ComparisonReport:
        pass
