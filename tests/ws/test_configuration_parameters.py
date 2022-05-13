import os
import unittest

from flask import Flask

import config as base_config
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study
from tests.ws.chebi.search.test_chebi_search_manger import ChebiSearchManagerTest


class WebServiceClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = Flask(__name__, instance_relative_config=True)
        cls.app.config.from_object(base_config)
        cls.app.config.from_pyfile('config.py', silent=True)

    @classmethod
    def tearDownClass(cls):
        pass

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

    def test_folder_configuration_01(self):
        with self.app.app_context():
            self.assertIsNotNone(self.app.config.get("STUDY_QUEUE_FOLDER"))
            self.assertIsNotNone(self.app.config.get("MTBLS_STABLE_ID_PREFIX"))
            self.assertIsNotNone(self.app.config.get("MTBLS_FTP_ROOT"))
            self.assertIsNotNone(self.app.config.get("PRIVATE_FTP_SERVER"))
            self.assertIsNotNone(self.app.config.get("PRIVATE_FTP_SERVER_USER"))
            self.assertIsNotNone(self.app.config.get("PRIVATE_FTP_SERVER_PASSWORD"))
            self.assertIsNotNone(self.app.config.get("FTP_UPLOAD_HELP_DOC_URL"))

    def test_folder_configuration_02_folders_exist(self):
        with self.app.app_context():
            self.assertTrue(os.path.exists(self.app.config.get("STUDY_QUEUE_FOLDER")))
            self.assertTrue(os.path.exists(self.app.config.get("MTBLS_FTP_ROOT")))

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

    def test_chebi_server_test_01(self):
        chebi_test = ChebiSearchManagerTest()
        chebi_test.setUp()
        chebi_test.test_search_by_database_id_success_01()
        chebi_test.test_search_by_inchi_success_01()
        chebi_test.test_search_by_name_success_iupac_name_01()
        chebi_test.test_search_by_smiles_success_01()

    def test_elasticsearch_configuration_01(self):
        with self.app.app_context():
            self.assertIsNotNone(self.app.config.get("ELASTICSEARCH_HOST"))
            self.assertIsNotNone(self.app.config.get("ELASTICSEARCH_PORT"))
            self.assertIsNotNone(self.app.config.get("ELASTICSEARCH_USE_TLS"))
            self.assertIsNotNone(self.app.config.get("ELASTICSEARCH_USER_NAME"))
            self.assertIsNotNone(self.app.config.get("ELASTICSEARCH_USER_PASSWORD"))