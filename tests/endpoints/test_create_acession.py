import json
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

class CreateAccessionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app = Flask(__name__, instance_relative_config=True)
        initialize_app(cls.app)

    @classmethod
    def tearDownClass(cls):
        pass

    def test_get_invalid_token_01(self):
        with self.app.test_client() as c:
            headers = {"user_token": sensitive_data.invalid_user_token_001}
            result = c.get(f"{context_path}/studies/create", headers=headers, json={})
            self.assertIsNotNone(result)
            self.assertEqual(401, result.status_code)

    def test_get_valid_token_01(self):
        """
        Verifies these method updates
            WsClient add_empty_study
            WsClient get_all_studies_for_user
        """
        with self.app.test_client() as c:
            headers = {"user_token": sensitive_data.super_user_token_001}
            result = c.get(f"{context_path}/studies/create", headers=headers, json={})
            self.assertIsNotNone(result)
            self.assertIn(result.status_code, (200, 201))