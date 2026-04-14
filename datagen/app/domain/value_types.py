from datetime import date, datetime
from typing import TypeAlias

NonNullColumnValue: TypeAlias = str | int | float | bool | date | datetime
NonNullColumnValues: TypeAlias = list[NonNullColumnValue]
ColumnValues: TypeAlias = list[NonNullColumnValue | None]
GeneratedColumnsByName: TypeAlias = dict[str, ColumnValues]
