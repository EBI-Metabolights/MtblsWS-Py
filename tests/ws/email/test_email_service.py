import unittest
from unittest.mock import Mock

from flask import Flask
from flask_mail import Mail

from app.ws.email.email_service import EmailService
from app.ws.email.settings import get_email_service_settings
from instance import config


class EmailServiceTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = Flask(__name__, instance_relative_config=True)
        cls.app.config.from_object(config)
        cls.app.config.from_pyfile('config.py', silent=True)
        cls.mail = Mail(cls.app)


    @classmethod
    def tearDownClass(cls):
        pass

    def test_send_email_1(self):

        with self.app.app_context():
            mail_service = EmailService(get_email_service_settings(self.app), self.mail)
            send_mail_mock = Mock()
            mail_service.send_email = send_mail_mock
            mail_service.send_email_for_requested_ftp_folder_created("MTBLS98765432", "/test/folder",
                                                                     "test-mail@ebi.ac.uk", ["cc-mail@ebi.ac.uk"])
            send_mail_mock.assert_called_once()

    def test_send_email_2(self):
        with self.app.app_context():
            mail_service = EmailService(get_email_service_settings(self.app), self.mail)
            send_mail_mock = Mock()
            mail_service.send_email = send_mail_mock
            mail_service.send_email_for_queued_study_submitted ("MTBLS98765432", "12-01-2022",
                                                                "test-mail@ebi.ac.uk", ["cc-mail@ebi.ac.uk"])
            send_mail_mock.assert_called_once()