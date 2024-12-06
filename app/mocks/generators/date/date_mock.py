from datetime import timedelta, date

from app.mocks.generators.date.base_mock import BaseDateGeneratorMock


class DateGeneratorMock(BaseDateGeneratorMock[date]):
    @staticmethod
    def calculate_offset_date(start_date: date, offset_days: int, date_format: str = None) -> date:
        return start_date + timedelta(days=offset_days)
