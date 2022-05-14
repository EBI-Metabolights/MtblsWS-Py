import os
import subprocess
import unittest

from flask import Flask
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study, Stableid
from tests.ws.check_confguration import check_configuration


class ConfigTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = Flask(__name__, instance_relative_config=True)
        cls.app.config.from_object('config')
        cls.app.config.from_pyfile('config.py', silent=True)

    @classmethod
    def tearDownClass(cls):
        pass

    def test_unused_undefined_configurations_01(self):
        with self.app.app_context():
            unused_configs, undefined_configs = check_configuration()
            self.assertIsNotNone(unused_configs)
            self.assertIsNotNone(undefined_configs)
            self.assertEqual(0, len(unused_configs))
            self.assertEqual(0, len(undefined_configs))

    def test_db_configuration_01(self):
        with self.app.app_context():
            self.assertIsNotNone(self.app.config.get("DB_PARAMS"))
            self.assertIsNotNone(self.app.config.get("CONN_POOL_MIN"))
            self.assertIsNotNone(self.app.config.get("CONN_POOL_MAX"))

    def test_db_configuration_02(self):
        with self.app.app_context():
            with DBManager.get_instance(self.app).session_maker() as db_session:
                db_study_obj = db_session.query(Study).filter(Study.acc == "MTBLS1").first()
                self.assertIsNotNone(db_study_obj)

    def test_db_mtbls_prefix_exists(self):
        with self.app.app_context():
            with DBManager.get_instance(self.app).session_maker() as db_session:
                query = db_session.query(Stableid.prefix)
                result = query.filter(Stableid.prefix == self.app.config.get("MTBLS_STABLE_ID_PREFIX")).first()
                self.assertIsNotNone(result)

    def test_folder_configuration_01(self):
        with self.app.app_context():
            self.assertIsNotNone(self.app.config.get("STUDY_QUEUE_FOLDER"))
            self.assertIsNotNone(self.app.config.get("MTBLS_STABLE_ID_PREFIX"))
            self.assertIsNotNone(self.app.config.get("MTBLS_FTP_ROOT"))
            self.assertIsNotNone(self.app.config.get("PRIVATE_FTP_SERVER"))
            self.assertIsNotNone(self.app.config.get("PRIVATE_FTP_SERVER_USER"))
            self.assertIsNotNone(self.app.config.get("PRIVATE_FTP_SERVER_PASSWORD"))
            self.assertIsNotNone(self.app.config.get("FTP_UPLOAD_HELP_DOC_URL"))
            self.assertIsNotNone(self.app.config.get("MTBLS_FILE_BASE"))
            self.assertIsNotNone(self.app.config.get("STUDY_PATH"))
            self.assertIsNotNone(self.app.config.get("DEFAULT_TEMPLATE"))

    def test_folder_configuration_02_folders_exist(self):
        with self.app.app_context():
            self.assertTrue(os.path.exists(self.app.config.get("STUDY_QUEUE_FOLDER")))
            self.assertTrue(os.path.exists(self.app.config.get("MTBLS_FTP_ROOT")))
            self.assertTrue(os.path.exists(self.app.config.get("MTBLS_FILE_BASE")))
            self.assertTrue(os.path.exists(self.app.config.get("STUDY_PATH")))

    def test_validation_settings_01(self):
        with self.app.app_context():
            self.assertIsNotNone(self.app.config.get("VALIDATIONS_FILE"))
            self.assertIsNotNone(self.app.config.get("VALIDATION_SCRIPT"))
            validation_file = self.app.config.get("VALIDATIONS_FILE")
            if not validation_file.startswith("http"):
                self.assertTrue(os.path.exists(self.app.config.get("VALIDATIONS_FILE")))
            self.assertTrue(os.path.exists(self.app.config.get("VALIDATION_SCRIPT")))

    def test_file_settings_01(self):
        with self.app.app_context():
            self.assertIsNotNone(self.app.config.get("FILE_LIST_TIMEOUT"))
            self.assertIsNotNone(self.app.config.get("FOLDER_EXCLUSION_LIST"))
            self.assertIsNotNone(self.app.config.get("EMPTY_EXCLUSION_LIST"))
            self.assertIsNotNone(self.app.config.get("IGNORE_FILE_LIST"))
            self.assertIsNotNone(self.app.config.get("RAW_FILES_LIST"))
            self.assertIsNotNone(self.app.config.get("DERIVED_FILES_LIST"))
            self.assertIsNotNone(self.app.config.get("COMPRESSED_FILES_LIST"))
            self.assertIsNotNone(self.app.config.get("INTERNAL_MAPPING_LIST"))
            self.assertIsNotNone(self.app.config.get("RESOURCES_PATH"))
            self.assertIsNotNone(self.app.config.get("FILES_LIST_JSON"))

    def test_file_configuration_01(self):

        with self.app.app_context():
            template_dir = os.path.join(self.app.config.get("STUDY_PATH"), self.app.config.get("DEFAULT_TEMPLATE"))
            self.assertTrue(os.path.exists(template_dir))
            investigation_file = os.path.join(template_dir, "i_Investigation.txt")
            sample_file = os.path.join(template_dir, "s_Sample.txt")
            raw_files_dir = os.path.join(template_dir, "RAW_FILES")
            derived_files_dir = os.path.join(template_dir, "DERIVED_FILES")
            self.assertTrue(os.path.exists(investigation_file))
            self.assertTrue(os.path.exists(sample_file))
            self.assertTrue(os.path.exists(raw_files_dir))
            self.assertTrue(os.path.exists(derived_files_dir))

    def test_email_configuration_01(self):
        with self.app.app_context():
            self.assertIsNotNone(self.app.config.get("EMAIL_NO_REPLY_ADDRESS"))
            self.assertIsNotNone(self.app.config.get("CURATION_EMAIL_ADDRESS"))
            self.assertIsNotNone(self.app.config.get("METABOLIGHTS_HOST_URL"))
            self.assertIsNotNone(self.app.config.get("MAIL_SERVER"))

    def test_chebi_configuration_01(self):
        with self.app.app_context():
            self.assertIsNotNone(self.app.config.get("CURATED_METABOLITE_LIST_FILE_LOCATION"))
            self.assertTrue(os.path.exists(self.app.config.get("CURATED_METABOLITE_LIST_FILE_LOCATION")))
            self.assertIsNotNone(self.app.config.get("CHEBI_WS_WSDL"))
            self.assertIsNotNone(self.app.config.get("CHEBI_WS_WSDL_SERVICE"))
            self.assertIsNotNone(self.app.config.get("CHEBI_WS_WSDL_SERVICE_PORT"))
            self.assertIsNotNone(self.app.config.get("CHEBI_WS_STRICT"))
            self.assertIsNotNone(self.app.config.get("CHEBI_WS_XML_HUGE_TREE"))
            self.assertIsNotNone(self.app.config.get("CHEBI_WS_WSDL_SERVICE_BINDING_LOG_LEVEL"))

    def test_elasticsearch_configuration_01(self):
        with self.app.app_context():
            self.assertIsNotNone(self.app.config.get("ELASTICSEARCH_HOST"))
            self.assertIsNotNone(self.app.config.get("ELASTICSEARCH_PORT"))
            self.assertIsNotNone(self.app.config.get("ELASTICSEARCH_USE_TLS"))
            self.assertIsNotNone(self.app.config.get("ELASTICSEARCH_USER_NAME"))
            self.assertIsNotNone(self.app.config.get("ELASTICSEARCH_USER_PASSWORD"))
