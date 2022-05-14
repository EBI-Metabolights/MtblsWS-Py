import json
import logging.config
import os
import unittest

from flask import Flask
from pydantic import BaseSettings

from app.wsapp_config import initialize_app

context_path = "/metabolights/ws"


class TestSensitiveData(BaseSettings):
    super_user_token_001: str
    invalid_user_token_001: str
    submitter_token_001: str

    class Config:
        # read and set security settings variables from this env_file
        env_file = "./tests/ws/.test_data"


sensitive_data = TestSensitiveData()


class MtblsPrivateStudiesTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app = Flask(__name__, instance_relative_config=True)
        hostname = os.uname().nodename
        logging_config_file_name = 'logging_' + hostname + '.conf'
        logging.config.fileConfig(logging_config_file_name)
        initialize_app(cls.app)

    @classmethod
    def tearDownClass(cls):
        pass

    def test_get_public_studies_01_super_user(self):
        with self.app.test_client() as c:
            headers = {"user_token": sensitive_data.super_user_token_001}
            result = c.get(f"{context_path}/studies/private", headers=headers, json={})
            self.assertIsNotNone(result)
            studies = json.loads(result.data)
            self.assertIsNotNone(studies)

    def test_get_public_studies_01_invalid_token(self):
        with self.app.test_client() as c:
            headers = {"user_token": sensitive_data.invalid_user_token_001}
            result = c.get(f"{context_path}/studies", headers=headers, json={})
            self.assertIsNotNone(result)
            studies = json.loads(result.data)
            self.assertIsNotNone(studies)

    def test_get_public_studies_01_without_token(self):
        with self.app.test_client() as c:
            headers = {}
            result = c.get(f"{context_path}/studies", headers=headers, json={})
            self.assertIsNotNone(result)
            studies = json.loads(result.data)
            self.assertIsNotNone(studies)
