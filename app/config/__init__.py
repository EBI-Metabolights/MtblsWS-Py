from functools import lru_cache

from app.config.model.auth import AuthSettings
from app.config.model.bioportal import BioportalSettings
from app.config.model.celery import CelerySettings
from app.config.model.chebi import ChebiSettings
from app.config.model.database import DatabaseSettings
from app.config.model.elasticsearch import ElasticsearchSettings
from app.config.model.email import EmailSettings
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
from app.config.model.twitter import TwitterSettings
from app.config.base import ApplicationBaseSettings


class ApplicationSettings(ApplicationBaseSettings):
    flask: FlaskConfiguration
    server: ServerSettings
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
    twitter: TwitterSettings
    metaspace: MetaspaceSettings
    google: GoogleSettings
    bioportal: BioportalSettings

    redis_cache: RedisSettings

    celery: CelerySettings


@lru_cache(1)
def get_settings():
    return ApplicationSettings()
