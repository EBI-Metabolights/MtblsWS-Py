from urllib.parse import quote

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app.config import get_settings
from app.config.model.database import DatabaseConnection



class DBManager(object):
    def __init__(self, db_config: DatabaseConnection = None):
        self.db_config: DatabaseConnection = db_config
        self.db_url = self._get_db_url()

        self._engine = create_engine(self.db_url)
        self.session_maker = sessionmaker(autocommit=False, autoflush=False, bind=self._engine)

    instance = None

    @classmethod
    def get_instance(cls):
        if not cls.instance:
            db_settings = get_settings().database.connection

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
        user = db_config.user
        password = db_config.password
        host = db_config.host
        port = db_config.port
        db_name = db_config.database
        url = f"postgresql://{quote(user)}:{quote(password)}@{host}:{port}/{db_name}"
        return url


Base = declarative_base()
