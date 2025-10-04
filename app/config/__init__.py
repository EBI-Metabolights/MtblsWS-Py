import datetime
import logging
from typing import Union

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
from app.config.model.redis_cache import RedisSettings
from app.config.model.report import ReportSettings
from app.config.model.server import ServerSettings
from app.config.model.study import StudySettings
from app.config.base import ApplicationBaseSettings
from app.config.model.worker import WorkerSettings

logger = logging.getLogger('wslog')

class ApplicationSettings(ApplicationBaseSettings):
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

    file_resources: FileResources
    file_filters: FileFilters
    report: ReportSettings

    jira: JiraSettings
    metaspace: MetaspaceSettings
    google: GoogleSettings
    bioportal: BioportalSettings
    redis_cache: RedisSettings
    celery: CelerySettings
    external_dependencies: ExternalDependenciesSettings


_application_settings: Union[None, ApplicationSettings] = None
_last_update_check_timestamp: int = 0 


def get_settings() -> ApplicationSettings:
    global _application_settings
    global _last_update_check_timestamp
    
    update_check_time_delta = 60
    if _application_settings:
        update_check_time_delta = _application_settings.server.service.config_file_check_period_in_seconds    
    now = int(datetime.datetime.now(datetime.UTC).timestamp())
    current_settings = _application_settings
    if now - _last_update_check_timestamp > update_check_time_delta:
        _application_settings = None
        _last_update_check_timestamp = now
    
    if not _application_settings:
        # print("Configuration file will be updated.")
        try:
            _application_settings = ApplicationSettings()
        except Exception as ex:
            print("Failed to load current configuration file.")
            print(ex)
            logger.error(ex)
            _application_settings = current_settings
    
    return _application_settings
