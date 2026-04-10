from dataclasses import dataclass, field
from typing import TypeGuard

from app.core.application.errors import GenerationInvariantError
from app.core.domain.entities import TableColumnSpec, TableSpec
from app.core.domain.value_types import ColumnValues, GeneratedColumnsByName, NonNullColumnValues


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

    required_parent_columns: set[str] = field(default_factory=set)
    dependent_tables: set[str] = field(default_factory=set)


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

    required_columns: frozenset[str]
    remaining_dependent_table_count: int
    cached_values_by_column: dict[str, NonNullColumnValues] = field(default_factory=dict)


class ForeignKeyReferenceTracker:
    """Управляет кэшем значений для внешних ключей во время генерации.

    Трекер работает на уровне родительской таблицы:
    - кэширует только колонки, используемые во внешних ключах
    - хранит кэш до обработки всех зависимых дочерних таблиц
    - удаляет кэш целиком, когда последняя зависимая дочерняя таблица уже обработана
    """

    def __init__(self, state_by_parent_table: dict[str, ParentTableReferenceState]) -> None:
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
        build_state_by_parent_table: dict[str, ParentTableBuildState] = {}

        for table in ordered_tables:
            for table_column in table.columns:
                foreign_key_spec = table_column.foreign_key
                if not foreign_key_spec:
                    continue

                parent_build_state = build_state_by_parent_table.setdefault(
                    foreign_key_spec.table_name,
                    ParentTableBuildState(),
                )
                parent_build_state.required_parent_columns.add(foreign_key_spec.column_name)
                parent_build_state.dependent_tables.add(table.table_name)

        state_by_parent_table = {
            parent_table_name: ParentTableReferenceState(
                required_columns=frozenset(parent_build_state.required_parent_columns),
                remaining_dependent_table_count=len(parent_build_state.dependent_tables),
            )
            for parent_table_name, parent_build_state in build_state_by_parent_table.items()
        }
        return cls(state_by_parent_table)

    def get_cached_parent_values(self, table_column: TableColumnSpec) -> NonNullColumnValues:
        """Возвращает значения родительской колонки для одной FK-колонки дочерней таблицы.

        Используется в момент генерации внешнего ключа, когда дочерней колонке нужно
        выбрать значения из уже сгенерированной родительской таблицы.
        """

        foreign_key_spec = table_column.foreign_key
        if foreign_key_spec is None:
            raise GenerationInvariantError(f"Column {table_column.name} is not a foreign key")

        parent_state = self.state_by_parent_table.get(foreign_key_spec.table_name)
        if not parent_state:
            raise GenerationInvariantError(f"Missing foreign key tracker state for table {foreign_key_spec.table_name}")

        cached_parent_values = parent_state.cached_values_by_column.get(foreign_key_spec.column_name)
        if cached_parent_values is not None:
            return cached_parent_values

        raise GenerationInvariantError(
            f"Missing cached foreign key values for {foreign_key_spec.table_name}.{foreign_key_spec.column_name}"
        )

    @staticmethod
    def is_non_null_column_values(values: ColumnValues) -> TypeGuard[NonNullColumnValues]:
        return all(value is not None for value in values)

    def cache_parent_values(
            self,
            table: TableSpec,
            generated_columns_by_name: GeneratedColumnsByName,
    ) -> None:
        """Сохраняет в кэше только нужные колонки только что сгенерированной родительской таблицы.

        Если на таблицу никто не ссылается по внешнему ключу, метод ничего не делает.
        Если ссылаются, в кэш попадают не все её колонки, а только те, которые реально
        используются как источник значений для FK.
        """

        parent_state = self.state_by_parent_table.get(table.table_name)
        if not parent_state:
            return

        cached_values_by_column: dict[str, NonNullColumnValues] = {}
        for column_name in parent_state.required_columns:
            column_values = generated_columns_by_name[column_name]
            if not self.is_non_null_column_values(column_values):
                raise GenerationInvariantError(
                    f"Foreign key parent cache contains nulls for {table.table_name}.{column_name}"
                )
            cached_values_by_column[column_name] = column_values

        parent_state.cached_values_by_column = cached_values_by_column

    def release_parent_cache_for_child(self, table: TableSpec) -> None:
        """Обновляет счётчики зависимостей после обработки дочерней таблицы.

        Метод уменьшает число оставшихся зависимых таблиц у всех родительских таблиц,
        на которые ссылалась текущая таблица. Если зависимых таблиц больше не осталось,
        кэш такой родительской таблицы удаляется целиком.
        """

        parent_tables_referenced_by_child: set[str] = set()
        for table_column in table.columns:
            foreign_key_spec = table_column.foreign_key
            if foreign_key_spec is None:
                continue
            parent_tables_referenced_by_child.add(foreign_key_spec.table_name)

        for parent_table_name in parent_tables_referenced_by_child:
            parent_state = self.state_by_parent_table.get(parent_table_name)
            if not parent_state:
                raise GenerationInvariantError(f"Missing foreign key tracker state for table {parent_table_name}")

            if parent_state.remaining_dependent_table_count > 1:
                parent_state.remaining_dependent_table_count -= 1
                continue

            del self.state_by_parent_table[parent_table_name]
