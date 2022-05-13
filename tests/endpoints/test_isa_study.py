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

class IsaStudyTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app = Flask(__name__, instance_relative_config=True)
        initialize_app(cls.app)

    @classmethod
    def tearDownClass(cls):
        pass

    def test_get_study_contacts_01(self):
        study_id = "MTBLS1"
        with self.app.test_client() as c:
            headers = {"user_token": sensitive_data.super_user_token_001}
            result = c.get(f"{context_path}/studies/{study_id}/contacts", headers=headers, json={})
            self.assertIsNotNone(result)
            contacts = json.loads(result.data)
            self.assertIsNotNone(contacts)

    def test_post_study_contacts_02(self):
        """
        Verifies  WsClient reindex_study method update
        """
        study_id = "MTBLS1"
        json_data = {'contact': [{'comments': [], 'firstName': 'Reza', 'lastName': 'Salek', 'email': 'rms72@cam.ac.uk',
                               'affiliation': 'University of Cambridge',
                               'address': 'The Department of Biochemistry, The Sanger Building, 80 Tennis Court Road, Cambridge, CB2 1GA, UK.',
                               'fax': '', 'midInitials': 'M', 'phone': '',
                               'roles': [{'annotationValue': 'principal investigator role'}]}]}

        json_data2 = {'contacts': [{'comments': [], 'firstName': 'Reza', 'lastName': 'Salek', 'email': 'rms72@cam.ac.uk',
                               'affiliation': 'University of Cambridge',
                               'address': 'The Department of Biochemistry, The Sanger Building, 80 Tennis Court Road, Cambridge, CB2 1GA, UK.',
                               'fax': '', 'midInitials': 'M', 'phone': '',
                               'roles': [{'annotationValue': 'principal investigator role'}]}]}

        with self.app.test_client() as c:
            headers = {"user_token": "4cece4ef-257e-4e28-993a-35fa2e4fa1c7", "save_audit_copy": True}
            result = c.post(f"{context_path}/studies/{study_id}/contacts", headers=headers, json=json_data2)
            self.assertIsNotNone(result)
            self.assertIn(result.status_code, (200, 201))
            contacts = json.loads(result.data)
            self.assertIsNotNone(contacts)

