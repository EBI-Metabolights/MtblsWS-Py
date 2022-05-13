import unittest

from flask import Flask
from pydantic import BaseSettings

from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study
from app.ws.db.settings import DatabaseSettings, get_directory_settings
from app.ws.db.wrappers import create_study_model_from_db_study, update_study_model_from_directory
from instance import config


class DatabaseSettingsFromConfig(DatabaseSettings):
    database_name: str = config.DB_PARAMS["database"]
    database_user: str = config.DB_PARAMS["user"]
    database_password: str = config.DB_PARAMS["password"]
    database_host: str = config.DB_PARAMS["host"]
    database_port: int = config.DB_PARAMS["port"]


class TestSensitiveData(BaseSettings):
    super_user_token_001: str

    class Config:
        # read and set security settings variables from this env_file
        env_file = "./tests/ws/.test_data"


sensitive_data = TestSensitiveData()


class WrappersTest(unittest.TestCase):

    def test_create_mtbls_file_obj_01(self):
        study_id = "MTBLS1"
        app = Flask(__name__, instance_relative_config=True)
        app.config.from_object(config)
        app.config.from_pyfile('config.py', silent=True)
        with app.app_context():
            db_manager = DBManager.get_instance(app)
            with db_manager.session_maker() as db_session:
                db_study_obj = db_session.query(Study).filter(Study.acc == study_id).first()
                study = create_study_model_from_db_study(db_study_obj)
            studies_root_path = get_directory_settings(app)
            update_study_model_from_directory(study, studies_root_path.studies_folder,
                                              optimize_for_es_indexing=False,
                                              revalidate_study=True,
                                              user_token_to_revalidate=sensitive_data.super_user_token_001,
                                              include_maf_files=False)

        self.assertGreaterEqual(len(study.protocols), 1)
        self.assertGreaterEqual(len(study.users), 1)
        self.assertGreaterEqual(len(study.publications), 1)
        self.assertGreaterEqual(len(study.organism), 1)
        self.assertGreaterEqual(len(study.contacts), 1)
        self.assertGreaterEqual(len(study.descriptors), 1)
        self.assertGreaterEqual(len(study.assays), 1)
        self.assertGreaterEqual(len(study.factors), 1)
        self.assertGreaterEqual(len(study.assays[0].assayTable.data), 1)
        self.assertGreaterEqual(len(study.assays[0].assayTable.fields), 1)

    def test_create_mtbls_file_obj_01_optimized(self):
        study_id = "MTBLS1"
        app = Flask(__name__, instance_relative_config=True)
        app.config.from_object(config)
        app.config.from_pyfile('config.py', silent=True)
        with app.app_context():
            db_manager = DBManager.get_instance(app)
            with db_manager.session_maker() as db_session:
                db_study_obj = db_session.query(Study).filter(Study.acc == study_id).first()
                study = create_study_model_from_db_study(db_study_obj)
            studies_root_path = get_directory_settings(app)
            update_study_model_from_directory(study, studies_root_path.studies_folder,
                                              optimize_for_es_indexing=True,
                                              revalidate_study=True,
                                              user_token_to_revalidate=sensitive_data.super_user_token_001,
                                              include_maf_files=False)

        self.assertFalse(hasattr(study, "protocols"))
        self.assertFalse(hasattr(study, "contacts"))
        self.assertFalse(hasattr(study, "sampleTable"))
        self.assertGreaterEqual(len(study.factors), 1)

        self.assertGreaterEqual(len(study.users), 1)
        self.assertGreaterEqual(len(study.publications), 1)
        self.assertGreaterEqual(len(study.organism), 1)
        self.assertGreaterEqual(len(study.descriptors), 1)
        self.assertGreaterEqual(len(study.assays), 1)
        self.assertFalse(hasattr(study.assays[0], "assayTable"))

    def test_create_mtbls_file_obj_02_optimized(self):
        study_id = "MTBLS4654"
        app = Flask(__name__, instance_relative_config=True)
        app.config.from_object(config)
        app.config.from_pyfile('config.py', silent=True)
        with app.app_context():
            db_manager = DBManager.get_instance(app)
            with db_manager.session_maker() as db_session:
                db_study_obj = db_session.query(Study).filter(Study.acc == study_id).first()
                study = create_study_model_from_db_study(db_study_obj)
            studies_root_path = get_directory_settings(app)
            update_study_model_from_directory(study, studies_root_path.studies_folder,
                                              optimize_for_es_indexing=True,
                                              revalidate_study=True,
                                              user_token_to_revalidate=sensitive_data.super_user_token_001,
                                              include_maf_files=False)

        self.assertFalse(hasattr(study, "protocols"))
        self.assertFalse(hasattr(study, "contacts"))
        self.assertFalse(hasattr(study, "sampleTable"))
        self.assertGreaterEqual(len(study.factors), 1)

        self.assertGreaterEqual(len(study.users), 1)
        self.assertGreaterEqual(len(study.publications), 1)
        self.assertGreaterEqual(len(study.organism), 1)
        self.assertGreaterEqual(len(study.descriptors), 1)
        self.assertGreaterEqual(len(study.assays), 1)
        self.assertFalse(hasattr(study.assays[0], "assayTable"))