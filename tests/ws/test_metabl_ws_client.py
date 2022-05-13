import json
import os
import pathlib
import shutil
import unittest
from unittest.mock import Mock

from flask import Flask
from flask_mail import Mail
from pydantic import BaseSettings

import config as base_config
from app.ws.chebi.search.chebi_search_manager import ChebiSearchManager
from app.ws.chebi.search.curated_metabolite_table import CuratedMetaboliteTable
from app.ws.chebi.wsproxy import ChebiWsProxy
from app.ws.db.types import MetabolightsException
from app.ws.db_connection import get_email, study_submitters, execute_query_with_parameter
from app.ws.email.email_service import EmailService
from app.ws.email.settings import get_email_service_settings
from app.ws.mtblsWSclient import WsClient
from instance import config
from tests.ws.chebi.search.test_chebi_search_manger import ChebiWsSettingsFixture

curated_file_location = config.CURATED_METABOLITE_LIST_FILE_LOCATION


class TestSensitiveData(BaseSettings):
    super_user_token_001: str
    invalid_user_token_001: str
    submitter_token_001: str

    class Config:
        # read and set security settings variables from this env_file
        env_file = "./tests/ws/.test_data"


sensitive_data = TestSensitiveData()


class WebServiceClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = Flask(__name__, instance_relative_config=True)
        cls.app.config.from_object(base_config)
        cls.app.config.from_pyfile('config.py', silent=True)
        settings = ChebiWsSettingsFixture()
        curated_table = CuratedMetaboliteTable(curated_file_location)
        proxy = ChebiWsProxy(settings=settings)
        cls.search_manager = ChebiSearchManager(ws_proxy=proxy, curated_metabolite_table=curated_table)
        with cls.app.app_context():
            mail = Mail(cls.app)
            cls.mail_service = EmailService(get_email_service_settings(cls.app), mail)

    @classmethod
    def tearDownClass(cls):
        pass

    def test_get_study_location_not_authorized_1(self):

        with self.app.app_context():
            send_mail_mock = Mock()
            self.mail_service.send_email = send_mail_mock
            ws_client = WsClient(self.search_manager, self.mail_service)

            with self.assertRaises(Exception) as context:
                ws_client.get_study_location("MTBLS9999", sensitive_data.invalid_user_token_001)

            self.assertEqual(context.exception.code, 403)
            send_mail_mock.assert_not_called()

    def test_get_study_location_public_1(self):

        with self.app.app_context():
            send_mail_mock = Mock()
            self.mail_service.send_email = send_mail_mock
            ws_client = WsClient(self.search_manager, self.mail_service)

            actual = ws_client.get_study_location("MTBLS1", sensitive_data.super_user_token_001)
            expected = os.path.join(self.app.config.get('STUDY_PATH'), "MTBLS1")
            self.assertEqual(expected, actual)

    def test_get_study_location_authorized_1(self):
        with self.app.app_context():
            send_mail_mock = Mock()
            self.mail_service.send_email = send_mail_mock
            ws_client = WsClient(self.search_manager, self.mail_service)

            actual = ws_client.get_study_location("MTBLS1", sensitive_data.super_user_token_001)
            expected = os.path.join(self.app.config.get('STUDY_PATH'), "MTBLS1")
            self.assertEqual(expected, actual)

    def test_get_maf_search_1(self):
        with self.app.app_context():
            send_mail_mock = Mock()
            self.mail_service.send_email = send_mail_mock
            ws_client = WsClient(self.search_manager, self.mail_service)

            search_type = "databaseId"
            search_value = "CHEBI:10225"
            expected_value = "CHEBI:10225"
            result = ws_client.get_maf_search(search_type, search_value)
            self.assertIsNotNone(result)
            data = json.loads(result)
            self.assertTrue("content" in data)
            self.assertEqual(1, len(data["content"]))
            self.assertTrue("databaseId" in data["content"][0])
            self.assertEqual(expected_value, data["content"][0]["databaseId"])

    def test_get_maf_search_2(self):
        with self.app.app_context():
            send_mail_mock = Mock()
            self.mail_service.send_email = send_mail_mock
            ws_client = WsClient(self.search_manager, self.mail_service)

            search_type = "inchi"
            search_value = "NLDDIKRKFXEWBK-AWEZNQCLSA-N"
            expected_value = "CHEBI:10136"
            result = ws_client.get_maf_search(search_type, search_value)
            self.assertIsNotNone(result)
            data = json.loads(result)
            self.assertTrue("content" in data)
            self.assertEqual(1, len(data["content"]))
            self.assertTrue("databaseId" in data["content"][0])
            self.assertEqual(expected_value, data["content"][0]["databaseId"])

    def test_get_maf_search_3(self):
        with self.app.app_context():
            send_mail_mock = Mock()
            self.mail_service.send_email = send_mail_mock
            ws_client = WsClient(self.search_manager, self.mail_service)

            search_type = "smiles"
            search_value = "CO"
            expected_value = "CHEBI:17790"
            result = ws_client.get_maf_search(search_type, search_value)
            self.assertIsNotNone(result)
            data = json.loads(result)
            self.assertTrue("content" in data)
            self.assertEqual(1, len(data["content"]))
            self.assertTrue("databaseId" in data["content"][0])
            self.assertEqual(expected_value, data["content"][0]["databaseId"])

    def test_get_maf_search_4_name(self):
        with self.app.app_context():
            send_mail_mock = Mock()
            self.mail_service.send_email = send_mail_mock
            ws_client = WsClient(self.search_manager, self.mail_service)

            search_type = "name"
            search_value = "(5S)-5-hydroxy-1-(4-hydroxy-3-methoxyphenyl)decan-3-one"
            expected_value = "CHEBI:10136"
            result = ws_client.get_maf_search(search_type, search_value)
            self.assertIsNotNone(result)
            data = json.loads(result)
            self.assertTrue("content" in data)
            self.assertEqual(1, len(data["content"]))
            self.assertTrue("databaseId" in data["content"][0])
            self.assertEqual(expected_value, data["content"][0]["databaseId"])

    def test_get_all_studies_for_user_1(self):
        with self.app.app_context():
            send_mail_mock = Mock()
            self.mail_service.send_email = send_mail_mock
            ws_client = WsClient(self.search_manager, self.mail_service)

            result = ws_client.get_all_studies_for_user(sensitive_data.invalid_user_token_001)
            self.assertIsNotNone(result)
            data = json.loads(result)
            self.assertEqual(len(data), 0)

    def test_get_all_studies_for_user_2(self):
        with self.app.app_context():
            send_mail_mock = Mock()
            self.mail_service.send_email = send_mail_mock
            ws_client = WsClient(self.search_manager, self.mail_service)

            result = ws_client.get_all_studies_for_user(sensitive_data.submitter_token_001)
            self.assertIsNotNone(result)
            data = json.loads(result)
            self.assertGreater(len(data), 0)

    def test_get_queue_folder_1(self):
        with self.app.app_context():
            send_mail_mock = Mock()
            self.mail_service.send_email = send_mail_mock
            ws_client = WsClient(self.search_manager, self.mail_service)

            actual = ws_client.get_queue_folder()
            expected = self.app.config.get('STUDY_QUEUE_FOLDER')
            self.assertEqual(expected, actual)

    def test_create_upload_folder_valid_study_01(self):

        with self.app.app_context():
            send_mail_mock = Mock()
            self.mail_service.send_email = send_mail_mock
            ws_client = WsClient(self.search_manager, self.mail_service)
            input_study_id = "MTBLS1"
            input_obfusucation_code = "XYZTaamss"
            expected_path = os.path.join(self.app.config.get('MTBLS_FTP_ROOT'),
                                         input_study_id.lower() + "-" + input_obfusucation_code)
            if os.path.exists(expected_path):
                created_path = pathlib.Path(expected_path)
                shutil.rmtree(created_path)
            try:
                actual = ws_client.create_upload_folder(input_study_id, input_obfusucation_code,
                                                        sensitive_data.super_user_token_001)

                self.assertIsNotNone(actual)
                self.assertEqual(expected_path, actual['os_upload_path'])

                send_mail_mock.assert_called()

            finally:
                if os.path.exists(expected_path):
                    created_path = pathlib.Path(expected_path)
                    shutil.rmtree(created_path)


    def test_create_upload_folder_invalid_study_01(self):

        with self.app.app_context():
            send_mail_mock = Mock()
            self.mail_service.send_email = send_mail_mock
            ws_client = WsClient(self.search_manager, self.mail_service)
            input_study_id = "MTBLS999991"
            input_obfusucation_code = "XYZTaamss"
            expected_path = os.path.join(self.app.config.get('MTBLS_FTP_ROOT'),
                                         input_study_id.lower() + "-" + input_obfusucation_code)

            try:
                with self.assertRaises(MetabolightsException):
                    ws_client.create_upload_folder(input_study_id, input_obfusucation_code,
                                                            sensitive_data.super_user_token_001)
                send_mail_mock.assert_not_called()

            finally:
                if os.path.exists(expected_path):
                    created_path = pathlib.Path(expected_path)
                    shutil.rmtree(created_path)

    def test_add_empty_study_1(self):

        with self.app.app_context():
            user_token = sensitive_data.super_user_token_001
            send_mail_mock = Mock()
            self.mail_service.send_email = send_mail_mock
            ws_client = WsClient(self.search_manager, self.mail_service)
            study_id = None
            try:
                actual = ws_client.add_empty_study(user_token)
                study_id = actual
                self.assertIsNotNone(actual)
                send_mail_mock.assert_called()

            finally:
                if study_id:
                    user_email = get_email(user_token)
                    study_submitters(study_id, user_email, "delete")
                    sql = "delete study where acc = %(acc)s;"
                    params = {"acc": study_id}
                    execute_query_with_parameter(sql, params)


    def test_reindex_1(self):

        with self.app.app_context():
            user_token = sensitive_data.super_user_token_001
            send_mail_mock = Mock()
            self.mail_service.send_email = send_mail_mock
            ws_client = WsClient(self.search_manager, self.mail_service)
            study_id = None
            try:
                actual = ws_client.reindex_study("MTBLS1", user_token)
                self.assertIsNotNone(actual)
                send_mail_mock.assert_not_called()

            finally:
                if study_id:
                    user_email = get_email(user_token)
                    study_submitters(study_id, user_email, "delete")

                    sql = "delete from studies where acc = %(acc)s;"
                    params = {"acc": study_id}
                    execute_query_with_parameter(sql, params)


    def test_reindex_submitter_01_permission_error(self):

        with self.app.app_context():
            user_token = sensitive_data.submitter_token_001
            send_mail_mock = Mock()
            self.mail_service.send_email = send_mail_mock
            ws_client = WsClient(self.search_manager, self.mail_service)
            study_id = None
            try:
                with self.assertRaises(MetabolightsException):
                    study_id = ws_client.reindex_study("MTBLS1", user_token)

                send_mail_mock.assert_not_called()

            finally:
                if study_id:
                    user_email = get_email(user_token)
                    study_submitters(study_id, user_email, "delete")

                    sql = "delete from studies where acc = %(acc)s;"
                    params = {"acc": study_id}
                    execute_query_with_parameter(sql, params)