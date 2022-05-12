
from flask import current_app as app
from pydantic import BaseSettings


class DatabaseSettings(BaseSettings):
    database_name: str = None
    database_user: str = None
    database_password: str = None
    database_host: str = "localhost"
    database_port: int = 5432


def get_database_settings() -> DatabaseSettings:
    settings = DatabaseSettings()
    if app.config:
        settings.database_name = app.config.get("DB_PARAMS")["database"]
        settings.database_user = app.config.get("DB_PARAMS")["user"]
        settings.database_password = app.config.get("DB_PARAMS")["password"]
        settings.database_host = app.config.get("DB_PARAMS")["host"]
        settings.database_port = app.config.get("DB_PARAMS")["port"]
    return settings


class DirectorySettings(BaseSettings):
    isatab_config_folder: str = None
    studies_folder: str = None
    reference_folder: str = None
    private_ftp_folder: str = None


def get_directory_settings() -> DirectorySettings:
    settings = DirectorySettings()
    if app.config:
        settings.studies_folder = app.config.get("STUDY_PATH")
        settings.private_ftp_folder = app.config.get("MTBLS_PRIVATE_FTP_ROOT")

    return settings
