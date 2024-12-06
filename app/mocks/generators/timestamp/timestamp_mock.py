from datetime import timedelta, datetime

from app.mocks.generators.timestamp.base_mock import BaseTimestampGeneratorMock


class TimestampGeneratorMock(BaseTimestampGeneratorMock[datetime]):

    @staticmethod
    def calculate_offset_timestamp(start_ts: datetime, offset_days: int, ts_format: str = None) -> datetime:
        return start_ts + timedelta(seconds=offset_days)
