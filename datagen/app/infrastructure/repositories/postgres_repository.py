from io import StringIO
from typing import Any, Dict, List

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

from app.core.application.ports.repository_port import IMockRepository
from app.shared.logger import logger


class MockRepository(IMockRepository):
    def __init__(self):
        self.database_url = "postgresql+psycopg2://admin:admin@localhost:5433/mocks"
        self._engine = None
        self.session_pool = None

    def connect(self):
        self._engine = create_engine(self.database_url, pool_size=10)
        self.session_pool = sessionmaker(bind=self._engine, autocommit=False, autoflush=False)
        logger.info("Postgres open connection")

    def create_db_schema(self, schema_name: str):
        with self.session_pool() as session:
            session.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            session.commit()
            logger.info(f"Schema {schema_name} created successfully")

    def create_as_table(self, ddl_query: str, full_table_name: str, generated_data: Dict[str, List[Any]]):
        buffer = StringIO()
        columns = list(generated_data.keys())
        total_rows = len(next(iter(generated_data.values())))

        for i in range(total_rows):
            row = []
            for col in columns:
                val = generated_data[col][i]
                row.append("" if val is None else str(val))
            buffer.write(";".join(row) + "\n")

        buffer.seek(0)

        copy_sql = f"""
               COPY {full_table_name}
               FROM STDIN
               WITH (
                   FORMAT CSV,
                   DELIMITER ';',
                   NULL ''
               )
           """

        with self.session_pool() as session:
            session.execute(text(f"DROP TABLE IF EXISTS {full_table_name}"))
            session.execute(text(ddl_query))

            cursor = session.connection().connection.cursor()
            cursor.copy_expert(sql=copy_sql, file=buffer)

            session.commit()
            logger.info(f"Table {full_table_name} created and loaded successfully")

    def disconnect(self):
        try:
            self.session_pool.close_all()
            self._engine.dispose()
            logger.info("Postgres close connection")
        except Exception as exc:
            logger.error(f"Ошибка закрытия БД: {exc}")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._engine:
            self.disconnect()
