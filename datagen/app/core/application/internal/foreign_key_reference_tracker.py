from dataclasses import dataclass, field
from typing import Any

from app.core.domain.entities import TableColumnSpec, TableSpec

# Псевдонимы ниже нужны только для читаемости сигнатур и состояния трекера.
FullTableName = str
ColumnName = str
DependentTableName = str
GeneratedColumnValues = list[Any]
CachedColumnValues = dict[ColumnName, GeneratedColumnValues]


@dataclass(slots=True)
class ParentTableBuildState:
    """Временное состояние родительской таблицы на этапе подготовки трекера.

    required_parent_columns:
        Имена колонок родительской таблицы, значения которых потом будут использованы
        для генерации внешних ключей в дочерних таблицах.
    dependent_tables:
        Полные имена дочерних таблиц, которые зависят от этой родительской таблицы
        хотя бы по одному внешнему ключу.
    """

    required_parent_columns: set[ColumnName] = field(default_factory=set)
    dependent_tables: set[DependentTableName] = field(default_factory=set)


@dataclass(slots=True)
class ParentTableReferenceState:
    """Состояние родительской таблицы во время генерации данных.

    required_columns:
        Какие колонки нужно держать в кэше, потому что на них ссылаются внешние ключи.
    remaining_dependent_table_count:
        Сколько дочерних таблиц ещё впереди будут читать значения из этого кэша.
    cached_values_by_column:
        Уже сгенерированные значения нужных колонок родительской таблицы.
    """

    required_columns: frozenset[ColumnName]
    remaining_dependent_table_count: int
    cached_values_by_column: CachedColumnValues = field(default_factory=dict)


class ForeignKeyReferenceTracker:
    """Управляет кэшем значений для внешних ключей во время генерации.

    Трекер работает на уровне родительской таблицы:
    - кэширует только колонки, используемые во внешних ключах
    - хранит кэш до обработки всех зависимых дочерних таблиц
    - удаляет кэш целиком, когда последняя зависимая дочерняя таблица уже обработана
    """

    def __init__(self, state_by_parent_table: dict[FullTableName, ParentTableReferenceState]) -> None:
        # Ключ словаря — полное имя родительской таблицы.
        # Значение — runtime-состояние её FK-кэша.
        self.state_by_parent_table = state_by_parent_table

    @classmethod
    def from_ordered_tables(cls, ordered_tables: list[TableSpec]) -> "ForeignKeyReferenceTracker":
        """Подготавливает начальное состояние трекера до старта генерации.

        Метод проходит по таблицам в уже рассчитанном порядке зависимостей и заранее
        определяет для каждой родительской таблицы:
        - какие её колонки нужно будет сохранить для внешних ключей
        - сколько дочерних таблиц позже будут читать эти значения
        """

        # Временное состояние для каждой родительской таблицы,
        # которое собирается до начала генерации.
        build_state_by_parent_table: dict[FullTableName, ParentTableBuildState] = {}

        for table in ordered_tables:
            for table_column in table.columns:
                foreign_key_spec = table_column.foreign_key
                if not foreign_key_spec:
                    continue

                parent_build_state = build_state_by_parent_table.setdefault(
                    foreign_key_spec.full_table_name,
                    ParentTableBuildState(),
                )
                parent_build_state.required_parent_columns.add(foreign_key_spec.column_name)
                parent_build_state.dependent_tables.add(table.full_table_name)

        state_by_parent_table = {
            parent_table_name: ParentTableReferenceState(
                required_columns=frozenset(parent_build_state.required_parent_columns),
                remaining_dependent_table_count=len(parent_build_state.dependent_tables),
            )
            for parent_table_name, parent_build_state in build_state_by_parent_table.items()
        }
        return cls(state_by_parent_table)

    def get_cached_parent_values(self, table_column: TableColumnSpec) -> GeneratedColumnValues:
        """Возвращает значения родительской колонки для одной FK-колонки дочерней таблицы.

        Используется в момент генерации внешнего ключа, когда дочерней колонке нужно
        выбрать значения из уже сгенерированной родительской таблицы.
        """

        foreign_key_spec = table_column.foreign_key
        parent_state = self.state_by_parent_table.get(foreign_key_spec.full_table_name)
        if not parent_state:
            raise RuntimeError(f"Missing foreign key tracker state for table {foreign_key_spec.full_table_name}")

        cached_parent_values = parent_state.cached_values_by_column.get(foreign_key_spec.column_name)
        if cached_parent_values is not None:
            return cached_parent_values

        raise RuntimeError(
            f"Missing cached foreign key values for {foreign_key_spec.full_table_name}.{foreign_key_spec.column_name}"
        )

    def cache_parent_values(
            self,
            table: TableSpec,
            generated_columns_by_name: dict[str, GeneratedColumnValues],
    ) -> None:
        """Сохраняет в кэше только нужные колонки только что сгенерированной родительской таблицы.

        Если на таблицу никто не ссылается по внешнему ключу, метод ничего не делает.
        Если ссылаются, в кэш попадают не все её колонки, а только те, которые реально
        используются как источник значений для FK.
        """

        parent_state = self.state_by_parent_table.get(table.full_table_name)
        if not parent_state:
            return

        parent_state.cached_values_by_column = {
            column_name: generated_columns_by_name[column_name]
            for column_name in parent_state.required_columns
        }

    def release_parent_cache_for_child(self, table: TableSpec) -> None:
        """Обновляет счётчики зависимостей после обработки дочерней таблицы.

        Метод уменьшает число оставшихся зависимых таблиц у всех родительских таблиц,
        на которые ссылалась текущая таблица. Если зависимых таблиц больше не осталось,
        кэш такой родительской таблицы удаляется целиком.
        """

        parent_tables_referenced_by_child = {
            table_column.foreign_key.full_table_name
            for table_column in table.columns
            if table_column.foreign_key
        }

        for parent_table_name in parent_tables_referenced_by_child:
            parent_state = self.state_by_parent_table.get(parent_table_name)
            if not parent_state:
                raise RuntimeError(f"Missing foreign key tracker state for table {parent_table_name}")

            if parent_state.remaining_dependent_table_count > 1:
                parent_state.remaining_dependent_table_count -= 1
                continue

            del self.state_by_parent_table[parent_table_name]
