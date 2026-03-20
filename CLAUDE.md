# CLAUDE.md

## Правила работы

- Перед работой сначала прочитай этот файл.
- Отвечай на русском. Код, идентификаторы и названия файлов остаются на английском.
- Сначала ищи нужные пути здесь, только потом сканируй проект.
- Думай как архитектор: clean architecture, низкая связность, минимум лишних сущностей.
- Не соглашайся автоматически с идеями пользователя. Ищи слабые места и противоречия.
- Перед изменением контракта или orchestration сначала проверь все точки стыка: `run_app -> AirflowDagRunner -> DAG -> Spark scripts -> S3 report`.


## Что делает проект

`DataGen`:
- генерирует синтетические данные
- пишет parquet и DDL в S3
- триггерит Airflow DAG
- читает итоговый report сверки из S3

DataGen не ходит напрямую в Hive или Iceberg. Доступ к системам хранения есть только у Airflow/Spark job.

## Быстрая карта файлов

### Entry / composition root

| Путь | Назначение |
|---|---|
| `datagen/run_app.py` | bootstrap, composition root, input conversion |
| `app/core/application/use_cases/execute_pipeline.py` | `ExecutePipelineUseCase`, orchestration |
| `datagen/app/providers.py` | composition root, сборка зависимостей |
| `datagen/app/shared/config.py` | конфигурация приложения |
| `datagen/app/shared/logger.py` | логгеры |
| `datagen/configuration/config.yaml` | основной конфиг |

### Application layer

| Путь | Назначение |
|---|---|
| `datagen/app/core/application/constants.py` | `ExecutionStatus`, `ComparisonStatus` |
| `datagen/app/core/application/dto/` | DTO package: publication, execution, comparison, pipeline, run_artifacts |
| `datagen/app/core/application/layouts/storage_layout.py` | storage key policy for run artifacts and table pointer state |
| `datagen/app/core/application/ports/comparison_query_renderer_port.py` | рендеринг engine-specific comparison queries |
| `datagen/app/core/application/ports/execution_runner_port.py` | запуск и ожидание внешнего execution workflow |
| `datagen/app/core/application/ports/publication_repository_port.py` | публикация parquet/DDL/pointer |
| `datagen/app/core/application/ports/comparison_repository_port.py` | чтение report сверки |
| `datagen/app/core/application/ports/table_load_payload_builder_port.py` | сборка `EngineLoadPayload` для целевого engine |
| `datagen/app/core/application/services/generation_service.py` | генерация данных |
| `datagen/app/core/application/services/publication_service.py` | публикация артефактов |
| `datagen/app/core/application/services/comparison_report_service.py` | чтение и интерпретация comparison report |
| `datagen/app/core/application/use_cases/execute_pipeline.py` | orchestration use case |

### Infrastructure layer

| Путь | Назначение |
|---|---|
| `datagen/app/infrastructure/airflow/airflow_client.py` | HTTP клиент Airflow |
| `datagen/app/infrastructure/airflow/airflow_dag_payload_builder.py` | сборка runtime payload для DAG |
| `datagen/app/infrastructure/airflow/airflow_dag_runner.py` | trigger + polling DAG |
| `datagen/app/infrastructure/query/comparison_query_renderer.py` | текущий renderer comparison query |
| `datagen/app/infrastructure/repositories/s3_publication_repository.py` | parquet + DDL + pointer в S3 |
| `datagen/app/infrastructure/repositories/s3_comparison_repository.py` | чтение `comparison_result.json` |
| `datagen/app/infrastructure/s3/s3_object_storage.py` | S3 adapter |
| `datagen/app/infrastructure/ddl/hive_query_builder.py` | DDL для Hive |
| `datagen/app/infrastructure/ddl/iceberg_query_builder.py` | DDL для Iceberg |
| `datagen/app/infrastructure/parquet/arrow_schema_builder.py` | Arrow schema для parquet |

### Airflow / Spark

| Путь | Назначение |
|---|---|
| `airflow/datagen_synth_load.py` | DAG |
| `airflow/scripts/job_common.py` | полный runtime-contract Spark jobs, parser, logger, JSON writer |
| `airflow/scripts/base_loader.py` | общий код для engine loaders + `ComparisonDataNormalizer` |
| `airflow/scripts/hadoop_load.py` | загрузка в Hive + materialize query result |
| `airflow/scripts/iceberg_load.py` | загрузка в Iceberg + materialize query result |
| `airflow/scripts/compare_results.py` | чтение parquet-результатов, schema check, выравнивание колонок и сверка |

### Входные артефакты

| Путь | Назначение |
|---|---|
| `datagen/params/data_schema.json` | пример входной схемы |
| `datagen/params/dag_run_config.template.json` | шаблон DAG runtime-contract |

## Архитектурные границы

Зависимости слоёв:

```text
domain <- application <- infrastructure
             ^
       run_app.py / providers.py
```

Правила:
- `domain` не знает про `application` и `infrastructure`
- `application` знает только domain, DTO и порты
- `infrastructure` реализует порты application
- `run_app.py` и `providers.py` остаются composition root

## Текущий pipeline

1. `DataGenerationService.generate()` генерирует `GeneratedTableData`.
2. `ExecutePipelineUseCase` создаёт `RunArtifactKeyLayout(run_id)`.
3. `ArtifactPublicationService.publish()` пишет parquet, DDL и engine-specific comparison queries в S3.
4. `AirflowDagRunner.trigger_and_wait()` собирает runtime-contract и запускает DAG.
5. DAG:
   - грузит parquet в Hive
   - грузит parquet в Iceberg
   - выполняет comparison-query отдельно в каждой системе
   - пишет `hive.parquet` и `iceberg.parquet`
   - отдельной compare-task сравнивает результаты и пишет report
6. `ComparisonReportService` читает report из S3 по `report_key` и возвращает application-level результат.

## Семантика генерации

- `gen_data_type` описывает, как значение генерируется.
- `output_data_type` описывает тип финального значения, которое попадёт в parquet и target tables.
- Для обычных колонок критичные инварианты должны выдерживаться на финальном output, а не только на промежуточном generator-value.
- `primary key` всегда не nullable: `null_ratio=0` обязателен.
- `foreign key` колонка считается derived from parent column, а не independently generated.
- Для `foreign key` сейчас разрешён только `null_ratio`; остальные generator constraints (`allowed_values`, `min/max`, `regular_expr`, `is_unique`) запрещены.
- `foreign key` должен ссылаться на non-null unique/primary-key column с тем же `output_data_type`.
- Для `ONE_TO_ONE` число non-null child rows не может превышать число parent rows.

## Имена целевых таблиц

DataGen не создаёт базы данных.

Используются фиксированные БД из конфига:
- Hive: `target_storage.hive.database_name`
- Iceberg: `target_storage.iceberg.database_name`

Формат имени таблицы:

```text
{database_name}.{schema_name}__{table_name}
```

Источник истины для имени target table: `BaseSqlQueryBuilder.build_target_table_name()`.

`ITableLoadPayloadBuilder` должен отдавать уже готовый `EngineLoadPayload`, а повторно `build_target_table_name()` для comparison renderer вызываться не должен.
Renderer должен использовать уже рассчитанные `publication.artifacts.engines.hive.target_table_name` и
`publication.artifacts.engines.iceberg.target_table_name`.

## Runtime-contract для DAG

Spark scripts получают один полный runtime-contract и парсят его целиком через `job_common.parse_job_contract()`.
Loader jobs затем строят `TableContract` для нужного engine через `JobContract.build_table_contracts(engine)`,
а compare-job использует `contract.comparison`.

```json
{
  "run_id": "...",
  "tables": [
    {
      "schema_name": "analytics",
      "table_name": "company_groups",
      "artifacts": {
        "data_uri": "s3a://bucket/runs/{run_id}/analytics/company_groups/data/data.parquet",
        "engines": {
          "hive": {
            "ddl_uri": "s3a://bucket/runs/{run_id}/analytics/company_groups/ddl/hive.sql",
            "target_table_name": "hive_db.analytics__company_groups"
          },
          "iceberg": {
            "ddl_uri": "s3a://bucket/runs/{run_id}/analytics/company_groups/ddl/iceberg.sql",
            "target_table_name": "iceberg_db.analytics__company_groups"
          }
        }
      }
    }
  ],
  "comparison": {
    "query_uris": {
      "hive": "s3a://bucket/runs/{run_id}/comparison/hive.sql",
      "iceberg": "s3a://bucket/runs/{run_id}/comparison/iceberg.sql"
    },
    "report_uri": "s3a://bucket/runs/{run_id}/result/comparison_result.json",
    "result_uris": {
      "hive": "s3a://bucket/runs/{run_id}/result/query/hive.parquet",
      "iceberg": "s3a://bucket/runs/{run_id}/result/query/iceberg.parquet"
    }
  }
}
```

Правила контракта:
- `comparison.query_uris.hive` и `.iceberg` обязательны
- `comparison.report_uri` обязателен
- `comparison.result_uris.hive` и `.iceberg` обязательны
- `tables[*].artifacts.data_uri`, `tables[*].artifacts.engines.hive.*`, `tables[*].artifacts.engines.iceberg.*` обязательны
- в application model engines зафиксированы как пара `hive/iceberg`, а не произвольный словарь
- в runtime-contract передаются URI на pre-rendered engine-specific queries
- пути к `metadata.json` Iceberg в контракт не передаются
- отдельный файл schema для compare не вводится

## S3 layout

```text
/runs/{run_id}/{schema}/{table}/data/data.parquet
/runs/{run_id}/{schema}/{table}/ddl/{engine}.sql
/runs/{run_id}/comparison/hive.sql
/runs/{run_id}/comparison/iceberg.sql
/tables/{schema}/{table}/pointer.json
/runs/{run_id}/result/query/hive.parquet
/runs/{run_id}/result/query/iceberg.parquet
/runs/{run_id}/result/comparison_result.json
```

`pointer.json` обновляется последним.

## Метод сверки

В DAG передаются URI на два pre-rendered engine query:

```text
hive.sql + iceberg.sql -> query_uris
```

Текущее временное состояние:
- source query пока не приходит ни от пользователя, ни из repo
- текущий stub живёт в `TargetTableComparisonQueryRenderer`
- DAG contract при этом уже финализирован вокруг `comparison.query_uris`

Сравниваются не physical tables, а результаты этих двух эквивалентных запросов:

```text
query_result(hive) vs query_result(iceberg)
```

Принятая схема:
1. `hadoop_load.py` грузит таблицы в Hive, выполняет comparison query и нормализует результат в canonical типы перед записью в parquet.
2. `iceberg_load.py` грузит таблицы в Iceberg, выполняет comparison query и нормализует результат в canonical типы перед записью в parquet.
3. `compare_results.py` читает оба parquet, валидирует схемы, выравнивает порядок колонок и сравнивает результаты.

Основной метод:

```python
hive_exclusive_row_count = hive_result.exceptAll(iceberg_result).count()
iceberg_exclusive_row_count = iceberg_result.exceptAll(hive_result).count()
```

Правила:
- `MATCH`, если оба счётчика равны `0`
- иначе `MISMATCH`
- используется `exceptAll`, а не checksum
- дубликаты учитываются
- порядок строк не влияет
- перед `exceptAll` оба DataFrame приводятся к одному порядку колонок, потому что `exceptAll` сравнивает по позиции, а не по имени

## Нормализация перед сравнением

Нормализация выполняется при записи (write-time) в `ComparisonDataNormalizer.normalize()` внутри `BaseSynthLoader`, а не при чтении в `compare_results.py`. Каждый loader независимо приводит DataFrame к canonical типам перед записью в parquet.

Canonical типы:
- `timestamp` и `timestamp_ntz` → `date_format(col.cast("timestamp"), "yyyy-MM-dd HH:mm:ss.SSSSSS")`
- `date` → `date_format(col.cast("date"), "yyyy-MM-dd")`
- integral types (`byte`, `short`, `int`, `long`) → `bigint`
- fractional types (`float`, `double`, `decimal`) → `decimal(38,18)`
- `boolean` → `lower(col.cast("string"))`
- `string` → без изменений

Ограничения:
- complex types (`array`, `map`, `struct`) не поддерживаются — `ComparisonDataNormalizer.normalize()` падает с `ValueError`
- `compare_results.py` валидирует что схемы обоих parquet идентичны после нормализации

## Статусы

Технический и бизнес-статус не смешиваются:

- `ExecutionStatus`: `SUCCESS | FAILED | TIMEOUT`
- `ComparisonStatus`: `MATCH | MISMATCH`

Валидный сценарий:
- DAG завершился успешно
- comparison report вернул `MISMATCH`

Это не техническая ошибка DAG, а бизнес-результат сверки.

## Формат comparison report

```json
{
  "run_id": "...",
  "checked_at": "2026-03-18T14:20:00Z",
  "status": "MATCH | MISMATCH",
  "summary": {
    "row_count": {
      "hive": 10000,
      "iceberg": 10000
    },
    "row_count_delta": 0,
    "exclusive_row_count": {
      "hive": 0,
      "iceberg": 0
    },
    "exclusive_row_ratio": {
      "hive": 0.0,
      "iceberg": 0.0
    }
  },
  "artifacts": {
    "hive_result_uri": "s3a://bucket/runs/{run_id}/result/query/hive.parquet",
    "iceberg_result_uri": "s3a://bucket/runs/{run_id}/result/query/iceberg.parquet"
  }
}
```

Смысл `exclusive_row_count`:
- `hive`: сколько строк осталось только в результате Hive после `exceptAll`
- `iceberg`: сколько строк осталось только в результате Iceberg после `exceptAll`

Смысл `row_count_delta`:
- абсолютная разница между `row_count.hive` и `row_count.iceberg`

Смысл `exclusive_row_ratio`:
- `hive`: доля exclusive-строк в результате Hive: `exclusive_row_count.hive / row_count.hive`
- `iceberg`: доля exclusive-строк в результате Iceberg: `exclusive_row_count.iceberg / row_count.iceberg`
- если `row_count == 0`, доля считается `0.0`

В `v1`:
- checksum не используется
- diff rows не сохраняются
- одного report-файла на `run_id` достаточно

## Что уже реализовано

- `comparison` секция в DAG runtime-contract
- `job_common.parse_job_contract()` как единый parser полного Spark runtime-contract
- materialize query result в `hadoop_load.py` и `iceberg_load.py`
- `compare_results.py`
- `ComparisonDataNormalizer`
- `ComparisonStatus`
- `ComparisonReport` и `PipelineExecutionResult`
- `ComparisonReportService`
- `RunArtifactKeyLayout`
- `ComparisonQueryRendererPort`
- `TargetTableComparisonQueryRenderer`
- `IComparisonReportRepository`
- `S3ComparisonReportRepository`
- staging `comparison.query_uris` до DAG trigger
- чтение report после успешного DAG-run в `run_app.py`

## Что ещё не покрыто

- сохранение sample diff-строк
- поддержка complex types в compare
- поддержка Postgres
- отдельная политика ротации comparison artifacts

## Практические соглашения

- Методы и поля не начинаются с `_`, если это не требуется API библиотеки.
- Главный публичный метод класса держи ближе к концу класса, если это не ухудшает читаемость.
- Новый тип данных требует обновления `enums.py`, генераторов, converters, query builders и providers.
- Если меняется report schema, синхронно проверь:
  - `airflow/scripts/compare_results.py`
  - `datagen/app/infrastructure/repositories/s3_comparison_repository.py`
  - `datagen/app/core/application/dto/`
  - `datagen/run_app.py`
- Если меняется DAG runtime-contract, синхронно проверь:
  - `airflow/scripts/job_common.py`
  - `airflow/scripts/base_loader.py`
  - `airflow/scripts/compare_results.py`
  - `airflow/datagen_synth_load.py`
  - `datagen/app/infrastructure/airflow/airflow_dag_payload_builder.py`
