import unittest
import uuid

from flask import Flask
from flask_mail import Mail

import config as base_config
from app.ws.db.types import MetabolightsException
from app.ws.db_connection import create_empty_study, execute_query_with_parameter
from app.ws.email.email_service import EmailService
from app.ws.email.settings import get_email_service_settings
from app.ws.study.user_service import UserService
from instance import config

curated_file_location = config.CURATED_METABOLITE_LIST_FILE_LOCATION


class TestData:

    def __init__(self, user_token, email, userid, studies=None):
        self.user_token = user_token
        self.email = email
        self.userid = userid
        if not studies:
            studies = []
        self.studies = studies


test_study_list = ["MTBLS999999", "MTBLS1000001"]
super_user_01 = TestData(str(uuid.uuid4()), "mtbls-test-super@ebi.ac.uk", 9999998)
submitter_1 = TestData(str(uuid.uuid4()), "mtbls-submitter1@ebi.ac.uk", 9999999, test_study_list)
invalid_user_1 = TestData("xxxxxxxsss", "mtbls-invalid@ebi.ac.uk", 1111111111)
insert_user_sql = """
    insert into users (id, apitoken, email, password, role, status, username)
    values (%(id)s, %(apitoken)s, %(email)s, %(password)s, %(role)s, %(status)s, %(username)s);

"""


def create_test_data_in_db():
    user = super_user_01
    content1 = {"id": user.userid, "apitoken": user.user_token,
                "email": user.email, "password": "", "status": 2, "role": 1, "username": user.email}
    execute_query_with_parameter(insert_user_sql, content1)
    user = submitter_1
    content2 = {"id": user.userid, "apitoken": user.user_token,
                "email": user.email, "password": "", "status": 2, "role": 0, "username": user.email}
    execute_query_with_parameter(insert_user_sql, content2)
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


class UserServiceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = Flask(__name__, instance_relative_config=True)
        cls.app.config.from_object(base_config)
        cls.app.config.from_pyfile('config.py', silent=True)

        with cls.app.app_context():
            delete_test_data_from_db()
            create_test_data_in_db()
            mail = Mail(cls.app)
            cls.mail_service = EmailService(get_email_service_settings(cls.app), mail)

    @classmethod
    def tearDownClass(cls):
        with cls.app.app_context():
            delete_test_data_from_db()

    def test_validate_user_01(self):
        with self.app.app_context():
            user_token = super_user_01.user_token
            user_service = UserService.get_instance(self.app)
            result = user_service.validate_user_by_token(user_token, [1])
            self.assertIsNotNone(result)
            self.assertIsNotNone(result.username)

    def test_validate_user_02(self):
        with self.app.app_context():
            user_token = submitter_1.user_token
            user_service = UserService.get_instance(self.app)
            result = user_service.validate_user_by_token(user_token, [0])
            self.assertIsNotNone(result)
            self.assertIsNotNone(result.username)

    def test_validate_user_invalid_token_01(self):
        with self.app.app_context():
            user_token = invalid_user_1.user_token
            user_service = UserService.get_instance(self.app)
            with self.assertRaises(MetabolightsException) as context:
                user_service.validate_user_by_token(user_token, [1])
            self.assertIsNotNone(context.exception.message)

    def test_validate_user_invalid_role_01(self):
        with self.app.app_context():
            user_token = submitter_1.user_token
            user_service = UserService.get_instance(self.app)
            with self.assertRaises(MetabolightsException) as context:
                user_service.validate_user_by_token(user_token, [1])
            self.assertIsNotNone(context.exception.message)

    def test_validate_user_has_write_access_invalid_01(self):
        with self.app.app_context():
            user_token = submitter_1.user_token
            user_service = UserService.get_instance(self.app)
            with self.assertRaises(MetabolightsException) as context:
                user_service.validate_user_has_write_access(user_token, "MTBLS3000")
            self.assertIsNotNone(context.exception.message)

    def test_validate_user_has_write_access_invalid_acess_token_01(self):
        with self.app.app_context():
            user_token = invalid_user_1.user_token
            user_service = UserService.get_instance(self.app)
            with self.assertRaises(MetabolightsException) as context:
                user_service.validate_user_has_write_access(user_token, "MTBLS3000")
            self.assertIsNotNone(context.exception.message)

    def test_validate_user_has_write_access_invalid_study_id_01(self):
        with self.app.app_context():
            user_token = submitter_1.user_token
            user_service = UserService.get_instance(self.app)
            with self.assertRaises(MetabolightsException) as context:
                user_service.validate_user_has_write_access(user_token, "MTBLSX3000")
            self.assertIsNotNone(context.exception.message)

    def test_validate_user_has_write_access_invalid_study_id_02(self):
        with self.app.app_context():
            user_token = super_user_01.user_token
            user_service = UserService.get_instance(self.app)
            with self.assertRaises(MetabolightsException) as context:
                user_service.validate_user_has_write_access(user_token, "MTBLSX3000")
            self.assertIsNotNone(context.exception.message)

    def test_validate_user_has_write_access_valid_01(self):
        with self.app.app_context():
            user_token = submitter_1.user_token
            user_service = UserService.get_instance(self.app)
            result = user_service.validate_user_has_write_access(user_token, submitter_1.studies[0])
            self.assertIsNotNone(result)
            self.assertIsNotNone(result.username)

    def test_validate_user_has_write_access_valid_super_user_01(self):
        with self.app.app_context():
            user_token = super_user_01.user_token
            user_service = UserService.get_instance(self.app)
            result = user_service.validate_user_has_write_access(user_token, submitter_1.studies[0])
            self.assertIsNotNone(result)
            self.assertIsNotNone(result.username)

    def test_validate_user_has_write_access_valid_super_user_02(self):
        with self.app.app_context():
            user_token = super_user_01.user_token
            user_service = UserService.get_instance(self.app)
            result = user_service.validate_user_has_write_access(user_token, submitter_1.studies[1])
            self.assertIsNotNone(result)
            self.assertIsNotNone(result.username)
