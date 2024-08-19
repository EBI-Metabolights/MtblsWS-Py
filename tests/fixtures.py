import os

import pytest
from flask import Flask
from flask_mail import Mail
from pydantic_settings import BaseSettings

from app.ws.email.email_service import EmailService
from app.ws.mtblsWSclient import WsClient
from app.wsapp_config import initialize_app


@pytest.fixture(scope="session")
def flask_app():
    instance_dir = os.path.join(os.getcwd(), "instance")

    if "INSTANCE_DIR" in os.environ and os.environ["INSTANCE_DIR"]:
        instance_dir = os.environ["INSTANCE_DIR"]

    flask_app = Flask(__name__, instance_relative_config=True, instance_path=instance_dir)
    initialize_app(flask_app)

    flask_app.config.update(
        {
            "TESTING": True,
        }
    )
    yield flask_app


@pytest.fixture(scope="session")
def flask_app_client(flask_app):
    yield flask_app.test_client()


@pytest.fixture
def email_service(flask_app):
    with flask_app.app_context():
        mail = Mail(flask_app)
        email_service = EmailService.get_instance(flask_app, mail)
        return email_service


@pytest.fixture
def ws_client(flask_app, chebi_search, email_service, elasticsearch_service):
    with flask_app.app_context():
        ws_client_object = WsClient(chebi_search, email_service, elasticsearch_service)

        yield ws_client_object


@pytest.fixture
def email_service_isolated_ws_client(flask_app, ws_client, mocker):
    with flask_app.app_context():
        send_mail_mock = mocker.Mock()
        ws_client.email_service.send_email = send_mail_mock

        yield ws_client


class SensitiveDatastorage(BaseSettings):
    super_user_token_001: str
    invalid_user_token_001: str
    submitter_token_001: str

    class Config:
        # read and set settings variables from this env_file
        env_file = "./tests/.test_data"


@pytest.fixture(scope="session")
def sensitive_data():
    sensitive_data = SensitiveDatastorage()
    yield sensitive_data
