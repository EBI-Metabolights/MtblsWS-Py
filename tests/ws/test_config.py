import os

from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study, Stableid
from tests.ws.check_confguration import check_configuration


class TestConfig(object):

    def test_unused_undefined_configurations_01(self, flask_app):
        with flask_app.app_context():
            unused_configs, undefined_configs = check_configuration()
            assert unused_configs is not None
            assert undefined_configs is not None
            assert 0 == len(unused_configs)
            assert 0 == len(undefined_configs)

    def test_db_configuration_01(self, flask_app):
        with flask_app.app_context():
            assert flask_app.config.get("DB_PARAMS") is not None
            assert flask_app.config.get("CONN_POOL_MIN") is not None
            assert flask_app.config.get("CONN_POOL_MAX") is not None

    def test_db_configuration_02(self, flask_app):
        with flask_app.app_context():
            with DBManager.get_instance(flask_app).session_maker() as db_session:
                db_study_obj = db_session.query(Study).filter(Study.acc == "MTBLS1").first()
                assert db_study_obj is not None

    def test_db_mtbls_prefix_exists(self, flask_app):
        with flask_app.app_context():
            with DBManager.get_instance(flask_app).session_maker() as db_session:
                query = db_session.query(Stableid.prefix)
                result = query.filter(Stableid.prefix == flask_app.config.get("MTBLS_STABLE_ID_PREFIX")).first()
                assert result is not None

    def test_folder_configuration_01(self, flask_app):
        with flask_app.app_context():
            assert flask_app.config.get("STUDY_QUEUE_FOLDER") is not None
            assert flask_app.config.get("MTBLS_STABLE_ID_PREFIX") is not None
            assert flask_app.config.get("MTBLS_FTP_ROOT") is not None
            assert flask_app.config.get("PRIVATE_FTP_SERVER") is not None
            assert flask_app.config.get("PRIVATE_FTP_SERVER_USER") is not None
            assert flask_app.config.get("PRIVATE_FTP_SERVER_PASSWORD") is not None
            assert flask_app.config.get("FTP_UPLOAD_HELP_DOC_URL") is not None
            assert flask_app.config.get("MTBLS_FILE_BASE") is not None
            assert flask_app.config.get("STUDY_PATH") is not None
            assert flask_app.config.get("DEFAULT_TEMPLATE") is not None

    def test_folder_configuration_02_folders_exist(self, flask_app):
        with flask_app.app_context():
            assert os.path.exists(flask_app.config.get("STUDY_QUEUE_FOLDER"))
            assert os.path.exists(flask_app.config.get("MTBLS_FTP_ROOT"))
            assert os.path.exists(flask_app.config.get("MTBLS_FILE_BASE"))
            assert os.path.exists(flask_app.config.get("STUDY_PATH"))

    def test_validation_settings_01(self, flask_app):
        with flask_app.app_context():
            assert flask_app.config.get("VALIDATIONS_FILE") is not None
            assert flask_app.config.get("VALIDATION_SCRIPT") is not None
            validation_file = flask_app.config.get("VALIDATIONS_FILE")
            if not validation_file.startswith("http"):
                assert os.path.exists(flask_app.config.get("VALIDATIONS_FILE"))
            assert os.path.exists(flask_app.config.get("VALIDATION_SCRIPT"))

    def test_file_settings_01(self, flask_app):
        with flask_app.app_context():
            assert flask_app.config.get("FILE_LIST_TIMEOUT") is not None
            assert flask_app.config.get("FOLDER_EXCLUSION_LIST") is not None
            assert flask_app.config.get("EMPTY_EXCLUSION_LIST") is not None
            assert flask_app.config.get("IGNORE_FILE_LIST") is not None
            assert flask_app.config.get("RAW_FILES_LIST") is not None
            assert flask_app.config.get("DERIVED_FILES_LIST") is not None
            assert flask_app.config.get("COMPRESSED_FILES_LIST") is not None
            assert flask_app.config.get("INTERNAL_MAPPING_LIST") is not None
            assert flask_app.config.get("RESOURCES_PATH") is not None
            assert flask_app.config.get("FILES_LIST_JSON") is not None

    def test_file_configuration_01(self, flask_app):
        with flask_app.app_context():
            template_dir = os.path.join(flask_app.config.get("STUDY_PATH"), flask_app.config.get("DEFAULT_TEMPLATE"))
            assert os.path.exists(template_dir)
            investigation_file = os.path.join(template_dir, "i_Investigation.txt")
            sample_file = os.path.join(template_dir, "s_Sample.txt")
            raw_files_dir = os.path.join(template_dir, "RAW_FILES")
            derived_files_dir = os.path.join(template_dir, "DERIVED_FILES")
            assert os.path.exists(investigation_file)
            assert os.path.exists(sample_file)
            assert os.path.exists(raw_files_dir)
            assert os.path.exists(derived_files_dir)

    def test_email_configuration_01(self, flask_app):
        with flask_app.app_context():
            assert flask_app.config.get("EMAIL_NO_REPLY_ADDRESS") is not None
            assert flask_app.config.get("CURATION_EMAIL_ADDRESS") is not None
            assert flask_app.config.get("METABOLIGHTS_HOST_URL") is not None
            assert flask_app.config.get("MAIL_SERVER") is not None

    def test_chebi_configuration_01(self, flask_app):
        with flask_app.app_context():
            assert flask_app.config.get("CURATED_METABOLITE_LIST_FILE_LOCATION") is not None
            assert os.path.exists(flask_app.config.get("CURATED_METABOLITE_LIST_FILE_LOCATION"))
            assert flask_app.config.get("CHEBI_WS_WSDL") is not None
            assert flask_app.config.get("CHEBI_WS_WSDL_SERVICE") is not None
            assert flask_app.config.get("CHEBI_WS_WSDL_SERVICE_PORT") is not None
            assert flask_app.config.get("CHEBI_WS_STRICT") is not None
            assert flask_app.config.get("CHEBI_WS_XML_HUGE_TREE") is not None
            assert flask_app.config.get("CHEBI_WS_WSDL_SERVICE_BINDING_LOG_LEVEL") is not None

    def test_elasticsearch_configuration_01(self, flask_app):
        with flask_app.app_context():
            assert flask_app.config.get("ELASTICSEARCH_HOST") is not None
            assert flask_app.config.get("ELASTICSEARCH_PORT") is not None
            assert flask_app.config.get("ELASTICSEARCH_USE_TLS") is not None
            assert flask_app.config.get("ELASTICSEARCH_USER_NAME") is not None
            assert flask_app.config.get("ELASTICSEARCH_USER_PASSWORD") is not None
