from functools import lru_cache

from flask import current_app as app
from pydantic import BaseSettings


class EmailServiceSettings(BaseSettings):
    email_no_reply_address: str = None
    curation_mail_address: str = None
    private_ftp_server: str = None
    private_ftp_server_user: str = None
    private_ftp_server_password: str = None
    ftp_upload_help_doc: str = None
    metabolights_host_url: str = None


@lru_cache
def get_email_service_settings() -> EmailServiceSettings:
    settings = EmailServiceSettings()

    if app.config:
        settings.email_no_reply_address = app.config.get("EMAIL_NO_REPLY_ADDRESS")
        settings.curation_mail_address = app.config.get("CURATION_EMAIL_ADDRESS")
        settings.metabolights_host_url = app.config.get("METABOLIGHTS_HOST_URL")
        settings.private_ftp_server_user = app.config.get("PRIVATE_FTP_SERVER_USER")
        settings.private_ftp_server_password = app.config.get("PRIVATE_FTP_SERVER_PASSWORD")
        settings.private_ftp_server = app.config.get("PRIVATE_FTP_SERVER")
        settings.ftp_upload_help_doc = app.config.get("FTP_UPLOAD_HELP_DOC")

    return settings
