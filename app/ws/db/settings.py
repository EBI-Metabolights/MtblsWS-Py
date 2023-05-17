from pydantic import BaseSettings

from app.ws.settings.utils import get_study_settings


class DatabaseSettings(BaseSettings):
    database_name: str = None
    database_user: str = None
    database_password: str = None
    database_host: str = "localhost"
    database_port: int = 5432


def get_database_settings(app) -> DatabaseSettings:
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


def get_directory_settings(app) -> DirectorySettings:
    settings = DirectorySettings()

    if app.config:
        settings.studies_folder = get_study_settings().study_metadata_files_root_path

    return settings
