import unittest
from unittest.mock import Mock

from flask import Flask
from pydantic import BaseSettings

import config as base_config
from app.ws.db.settings import DatabaseSettings
from app.utils import MetabolightsException
from app.ws.elasticsearch.elastic_service import ElasticsearchService
from instance import config


class DatabaseSettingsFromConfig(DatabaseSettings):
    database_name: str = config.DB_PARAMS["database"]
    database_user: str = config.DB_PARAMS["user"]
    database_password: str = config.DB_PARAMS["password"]
    database_host: str = config.DB_PARAMS["host"]
    database_port: int = config.DB_PARAMS["port"]


class TestSensitiveData(BaseSettings):
    super_user_token_001: str
    submitter_token_001: str
    invalid_user_token_001: str
    class Config:
        # read and set security settings variables from this env_file
        env_file = "./tests/ws/.test_data"


sensitive_data = TestSensitiveData()


class ElasticServiceTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app = Flask(__name__, instance_relative_config=True)
        cls.app.config.from_object(base_config)
        cls.app.config.from_pyfile('config.py', silent=True)

    def test_reindex_study_01(self):
        with self.app.app_context():
            study_id = "MTBLS1"
            service = ElasticsearchService.get_instance(self.app)
            mock_index = Mock()
            service.client.index = mock_index
            study = service.reindex_study(study_id=study_id, user_token=sensitive_data.super_user_token_001)

            self.assertIsNotNone(study)
            self.assertIsNotNone(study)
            mock_index.assert_called()


    def test_reindex_study_02(self):
        with self.app.app_context():
            study_id = "MTBLS4654"
            service = ElasticsearchService.get_instance(self.app)
            mock_index = Mock()
            service.client.index = mock_index
            study = service.reindex_study(study_id=study_id, user_token=sensitive_data.super_user_token_001)

            self.assertIsNotNone(study)
            self.assertIsNotNone(study)
            mock_index.assert_called()

    def test_reindex_study_02_invalid_study_01(self):
        with self.app.app_context():
            study_id = "MTBLS98763211"
            service = ElasticsearchService.get_instance(self.app)
            mock_index = Mock()
            service.client.index = mock_index
            with self.assertRaises(MetabolightsException) as context:
                study = service.reindex_study(study_id=study_id, user_token=sensitive_data.submitter_token_001)

            self.assertIsNotNone(context.exception)
            mock_index.assert_not_called()

    def test_reindex_study_03_invalid_user_token_01(self):
        with self.app.app_context():
            study_id = "MTBLS1"
            service = ElasticsearchService.get_instance(self.app)
            mock_index = Mock()
            service.client.index = mock_index
            with self.assertRaises(MetabolightsException) as context:
                study = service.reindex_study(study_id=study_id, user_token="xyzs-invalid")

            self.assertIsNotNone(context.exception)
            mock_index.assert_not_called()
