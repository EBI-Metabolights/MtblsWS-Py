from contextlib import contextmanager
from typing import Iterator, Union
from urllib.parse import quote

from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config import get_settings
from app.config.model.database import DatabaseConnection


class DBManager(object):
    def __init__(self, db_config: Union[None, DatabaseConnection] = None):
        self.db_config: DatabaseConnection = db_config
        self.raw_schema = "postgresql"
        self.sqlachemy_schema = "postgresql+psycopg"
        self.db_url = self._build_db_url()
        self.db_sqlalchemy_url = self._build_sqlachemy_db_url()

        self._engine = create_engine(self.db_sqlalchemy_url)
        self.session_maker = sessionmaker(
            autocommit=False, autoflush=False, bind=self._engine
        )

    instance = None

    @classmethod
    def get_instance(cls):
        if not cls.instance:
            db_settings = get_settings().database.connection

            cls.instance = DBManager(db_config=db_settings)
        return cls.instance

    @contextmanager
    def get_db_session(self) -> Iterator[Session]:
        session = self.session_maker()
        try:
            yield session
        finally:
            session.close()

    def get_db_url(self):
        return self.db_url

    def _build_db_url(self):
        db_config = self.db_config
        user = db_config.user
        password = db_config.password
        host = db_config.host
        port = db_config.port
        db_name = db_config.database
        return f"{self.raw_schema}://{quote(user)}:{quote(password)}@{host}:{port}/{db_name}"

    def _build_sqlachemy_db_url(self):
        db_config = self.db_config
        user = db_config.user
        password = db_config.password
        host = db_config.host
        port = db_config.port
        db_name = db_config.database
        return f"{self.sqlachemy_schema}://{quote(user)}:{quote(password)}@{host}:{port}/{db_name}"

    def execute_select_sql(self, sql: str, params: dict = None) -> list:
        with self.session_maker() as session:
            result = session.execute(text(sql), params)
            return result.mappings().all() or []

    def execute_update_sql(self, sql: str, params: dict = None) -> bool:
        with self.session_maker() as session:
            result = session.execute(text(sql), params)
            return result.mappings().all() or []
        return True

    def execute_insert_sql(self, sql: str, params: dict = None) -> bool:
        with self.session_maker() as session:
            session.execute(text(sql), params)
        return True

    def execute_delete_sql(self, sql: str, params: dict = None) -> bool:
        with self.session_maker() as session:
            session.execute(text(sql), params)
        return True


class Base(DeclarativeBase):
    pass
