from urllib.parse import quote

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from app.ws.db.settings import DatabaseSettings, get_database_settings


class DBManager(object):
    def __init__(self, db_config: DatabaseSettings = None):
        if db_config:
            self.db_config = db_config
        else:
            self.db_config = get_database_settings()
        self.db_url = self._get_db_url()

        self._engine = create_engine(self.db_url)
        self.session_maker = sessionmaker(autocommit=False, autoflush=False, bind=self._engine)

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
