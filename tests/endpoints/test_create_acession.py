import logging.config
import os
import shutil
import unittest
import uuid
from unittest.mock import patch

import flask_mail
from flask import Flask

from app.ws.db_connection import execute_query_with_parameter, create_empty_study
from app.wsapp_config import initialize_app

context_path = "/metabolights/ws"


class TestData:

    def __init__(self, user_token, email, userid, studies=None):
        self.user_token = user_token
        self.email = email
        self.userid = userid
        if not studies:
            studies = []
        self.studies = studies


test_study_list = ["MTBLS1000", "MTBLS1001"]
super_user_01 = TestData(str(uuid.uuid4()), "mtbls-test-super2@ebi.ac.uk", 9999998)
submitter_1 = TestData(str(uuid.uuid4()), "mtbls-submitter3@ebi.ac.uk", 9999999, test_study_list)
invalid_user_1 = TestData("xxxxxxxsss", "mtbls-invalid@ebi.ac.uk", 1111111111)
insert_user_sql = """
    insert into users (id, apitoken, email, password, role, status, username)
    values (%(id)s, %(apitoken)s, %(email)s, %(password)s, %(role)s, %(status)s, %(username)s);

"""


def create_test_users_in_db():
    user = super_user_01
    content1 = {"id": user.userid, "apitoken": user.user_token,
                "email": user.email, "password": "", "status": 2, "role": 1, "username": user.email}
    execute_query_with_parameter(insert_user_sql, content1)
    user = submitter_1
    content2 = {"id": user.userid, "apitoken": user.user_token,
                "email": user.email, "password": "", "status": 2, "role": 0, "username": user.email}
    execute_query_with_parameter(insert_user_sql, content2)


def create_test_studies():
    create_empty_study(submitter_1.user_token, test_study_list[0])
    create_empty_study(submitter_1.user_token, test_study_list[1])


def delete_test_data_from_db():
    sql = "delete from study_user where userid = %(userid)s;"
    params = {"userid": submitter_1.userid}
    execute_query_with_parameter(sql, params)

    sql = "delete from studies where acc = %(acc)s;"
    params = {"acc": test_study_list[0]}
    execute_query_with_parameter(sql, params)
    params = {"acc": test_study_list[1]}
    execute_query_with_parameter(sql, params)

    sql = "delete from users where email = %(email)s;"
    params = {"email": submitter_1.email}
    execute_query_with_parameter(sql, params)
    params = {"email": super_user_01.email}
    execute_query_with_parameter(sql, params)


class CreateAccessionTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        hostname = os.uname().nodename
        logging_config_file_name = 'logging_' + hostname + '.conf'
        logging.config.fileConfig(logging_config_file_name)
        cls.app = Flask(__name__, instance_relative_config=True)
        initialize_app(cls.app)

    @classmethod
    def tearDownClass(cls):
        pass

    def test_get_invalid_token_01(self):
        with self.app.test_client() as c:
            headers = {"user_token": invalid_user_1.user_token}
            result = c.get(f"{context_path}/studies/create", headers=headers, json={})
            self.assertIsNotNone(result)
            self.assertEqual(401, result.status_code)

    def test_get_valid_token_01(self):
        """
        Verifies these method updates
            WsClient add_empty_study
            WsClient create_upload_folder
            WsClient reindex_study
        """
        with self.app.app_context():
            delete_test_data_from_db()
            create_test_users_in_db()
        with self.app.test_client() as c:
            with patch.object(flask_mail.Mail, "send", return_value="") as mock_mail_send:
                headers = {"user_token": super_user_01.user_token}
                result = c.get(f"{context_path}/studies/create", headers=headers, json={})
                self.assertIsNotNone(result)
                self.assertIn(result.status_code, (200, 201))
                mock_mail_send.assert_called()
        with self.app.app_context():
            delete_test_data_from_db()

    def test_get_valid_token_with_study_id_not_in_db_01(self):
        """
        Verifies these method updates
            WsClient add_empty_study
            WsClient create_upload_folder
            WsClient reindex_study
            get_permissions
        """
        with self.app.app_context():
            delete_test_data_from_db()
            create_test_users_in_db()
        with self.app.test_client() as c:
            path = os.path.join(self.app.config.get("STUDY_PATH"), test_study_list[0])
            if os.path.exists(path):
                shutil.rmtree(path)
            with patch.object(flask_mail.Mail, "send", return_value="") as mock_mail_send:
                headers = {"user_token": super_user_01.user_token, "study_id": test_study_list[0]}
                result = c.get(f"{context_path}/studies/create", headers=headers, json={})
                self.assertIsNotNone(result)
                self.assertIn(result.status_code, (200, 201))
                mock_mail_send.assert_called()
        with self.app.app_context():
            delete_test_data_from_db()
        if os.path.exists(path):
            shutil.rmtree(path)

    def test_get_unauthorized_token_with_study_id_not_in_db_01(self):
        """
        Verifies these method updates
            WsClient add_empty_study
            WsClient create_upload_folder
            WsClient reindex_study
            get_permissions
        """
        with self.app.app_context():
            delete_test_data_from_db()
            create_test_users_in_db()
        with self.app.test_client() as c:
            path = os.path.join(self.app.config.get("STUDY_PATH"), test_study_list[0])
            if os.path.exists(path):
                shutil.rmtree(path)
            with patch.object(flask_mail.Mail, "send", return_value="") as mock_mail_send:
                headers = {"user_token": submitter_1.user_token, "study_id": test_study_list[0]}
                result = c.get(f"{context_path}/studies/create", headers=headers, json={})
                self.assertIsNotNone(result)
                self.assertIn(result.status_code, (401,))
                mock_mail_send.assert_not_called()
            with self.app.app_context():
                delete_test_data_from_db()
                create_test_users_in_db()
            if os.path.exists(path):
                shutil.rmtree(path)

    def test_get_valid_token_with_existing_study_in_db_01(self):
        """
        Verifies these method updates
            WsClient add_empty_study
            WsClient create_upload_folder
            WsClient reindex_study
            get_permissions
        """
        with self.app.app_context():
            delete_test_data_from_db()
            create_test_users_in_db()
            create_test_studies()

        with self.app.test_client() as c:
            path = os.path.join(self.app.config.get("STUDY_PATH"), test_study_list[0])
            with patch.object(flask_mail.Mail, "send", return_value="") as mock_mail_send:
                headers = {"user_token": super_user_01.user_token, "study_id": test_study_list[0]}
                result = c.get(f"{context_path}/studies/create", headers=headers, json={})
                self.assertIsNotNone(result)
                self.assertIn(result.status_code, (400,))
                mock_mail_send.assert_not_called()
        with self.app.app_context():
            delete_test_data_from_db()
            create_test_users_in_db()
            if os.path.exists(path):
                shutil.rmtree(path)
