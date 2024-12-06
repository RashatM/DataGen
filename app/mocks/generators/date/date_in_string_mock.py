from datetime import timedelta, date

from app.mocks.generators.date.base_mock import BaseDateGeneratorMock


class DateInStringGeneratorMock(BaseDateGeneratorMock[str]):
    @staticmethod
    def calculate_offset_date(start_date: date, offset_days: int, date_format: str = None) -> str:
        return (start_date + timedelta(days=offset_days)).strftime(date_format)
