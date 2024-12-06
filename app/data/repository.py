from datetime import datetime
from io import StringIO
from typing import Dict, List, Any

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


from app.config.logger import logger
from app.interfaces.repository import IMockRepository

class MockRepository(IMockRepository):
    def __init__(self):
        self.database_url = "database_url"
        self._engine = None
        self.session_pool = None

    def connect(self):
        self._engine = create_engine(
            self.database_url,
            pool_size=10
        )
        self.session_pool = sessionmaker(
            bind=self._engine,
            autocommit=False,
            autoflush=False
        )
        logger.info("Postgres open connection")

    def create_db_schema(self, schema_name: str):
        with self.session_pool() as session:
            session.execute(f"CREATE SCHEMA {schema_name} IF NOT EXISTS schema_name")
            session.commit()
            logger.info(f"Schema {schema_name} created successfully")


    def create_and_save(self, ddl_query: str, full_table_name: str , generated_data: Dict[str, List[Any]]):
        df = pd.DataFrame(generated_data)
        # df["created_at"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False, sep=";")
        buffer.seek(0)

        query = f"COPY {full_table_name} FROM STDIN WITH CSV DELIMITER ';' NULL 'NULL'"
        with self.session_pool() as session:
            session.execute(ddl_query)
            cursor = session.connection().connection.cursor()
            cursor.copy_expert(sql=query, file=buffer)
            session.commit()

        logger.info(f"Loading into the table {full_table_name} completed successfully")

    def disconnect(self):
        try:
            self.session_pool.close_all()
            self._engine.dispose()
            logger.info("Postgres close connection")
        except Exception as e:
            logger.error(f"Ошибка закрытия БД: {e}")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._engine:
            self.disconnect()


