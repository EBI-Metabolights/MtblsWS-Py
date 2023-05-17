from functools import lru_cache

from app.ws.settings.celery import CelerySettings
from app.ws.settings.hpc_cluster import HpcClusterSettings
from app.ws.settings.redis import RedisSettings
from app.ws.settings.study import StudySettings
from app.ws.settings.system import SystemSettings


@lru_cache(1)
def get_celery_settings() -> CelerySettings:
    return CelerySettings()


@lru_cache(1)
def get_redis_settings() -> RedisSettings:
    return RedisSettings()

@lru_cache(1)
def get_study_settings() -> StudySettings:
    settings = StudySettings()

    return settings

@lru_cache(1)
def get_system_settings(app=None) -> SystemSettings:
    settings = SystemSettings()

    return settings

@lru_cache(1)
def get_cluster_settings() -> HpcClusterSettings:
    settings = HpcClusterSettings()

    return settings

