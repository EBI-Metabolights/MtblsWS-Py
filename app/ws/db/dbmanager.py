from urllib.parse import quote

from flask import current_app as app
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from app.ws.db.settings import DatabaseSettings, get_database_settings


class DBManager(object):
    def __init__(self, db_config: DatabaseSettings = None):
        self.db_config = db_config
        self.db_url = self._get_db_url()

        self._engine = create_engine(self.db_url)
        self.session_maker = sessionmaker(autocommit=False, autoflush=False, bind=self._engine)

    instance = None

    @classmethod
    def get_instance(cls, application=None):
        if not cls.instance:
            if not application:
                application = app
            db_settings = get_database_settings(application)

            cls.instance = DBManager(db_config=db_settings)
        return cls.instance

    def get_db_session(self):
        db = self.session_maker()
        try:
            yield db
        finally:
            db.close()

    def _get_db_url(self):
        db_config = self.db_config
        user = db_config.database_user
        password = db_config.database_password
        host = db_config.database_host
        port = db_config.database_port
        db_name = db_config.database_name

        url = f"postgresql://{quote(user)}:{quote(password)}@{quote(host)}:{port}/{db_name}"
        return url


Base = declarative_base()
