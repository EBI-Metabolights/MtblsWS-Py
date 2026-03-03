import logging
import traceback
from typing import Union

from pydantic import BaseModel

from app.config.base import (
    CONFIG_FILE_PATH,
    SECRETS_PATH,
    get_yaml_settings_source,
)
from app.config.model.auth import AuthSettings
from app.config.model.bioportal import BioportalSettings
from app.config.model.celery import CelerySettings
from app.config.model.chebi import ChebiSettings
from app.config.model.database import DatabaseSettings
from app.config.model.elasticsearch import ElasticsearchSettings
from app.config.model.email import EmailSettings
from app.config.model.external_dependencies import ExternalDependenciesSettings
from app.config.model.file_filters import FileFilters
from app.config.model.file_resources import FileResources
from app.config.model.flask import FlaskConfiguration
from app.config.model.ftp_server import FtpServerSettings
from app.config.model.google import GoogleSettings
from app.config.model.hpc_cluster import HpcClusterSettings
from app.config.model.jira import JiraSettings
from app.config.model.metaspace import MetaspaceSettings
from app.config.model.mhd import MhdSettings
from app.config.model.redis_cache import RedisSettings
from app.config.model.report import ReportSettings
from app.config.model.server import ServerSettings
from app.config.model.study import StudySettings
from app.config.model.worker import WorkerSettings

logger = logging.getLogger("wslog")


class ApplicationSettings(BaseModel):
    flask: FlaskConfiguration
    server: ServerSettings
    workers: WorkerSettings
    database: DatabaseSettings
    elasticsearch: ElasticsearchSettings
    email: EmailSettings
    hpc_cluster: HpcClusterSettings
    study: StudySettings

    auth: AuthSettings
    ftp_server: FtpServerSettings
    chebi: ChebiSettings

    file_resources: FileResources = FileResources()
    file_filters: FileFilters = FileFilters()
    report: ReportSettings

    jira: JiraSettings
    metaspace: MetaspaceSettings
    google: GoogleSettings
    bioportal: BioportalSettings
    redis_cache: RedisSettings
    celery: CelerySettings
    external_dependencies: ExternalDependenciesSettings
    mhd: MhdSettings


_application_settings: Union[None, ApplicationSettings] = None


def get_settings() -> ApplicationSettings:
    global _application_settings

    if not _application_settings:
        source = get_yaml_settings_source(CONFIG_FILE_PATH, SECRETS_PATH)
        try:
            _application_settings = ApplicationSettings.model_validate(source)
        except Exception as ex:
            print("Failed to load current configuration file.")
            logger.error(ex)
            traceback.print_exc()
            raise Exception("Config file load error: {ex}")

    return _application_settings
