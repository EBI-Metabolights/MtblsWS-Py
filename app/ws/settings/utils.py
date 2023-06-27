from functools import lru_cache
from app.config import get_settings
from app.config.model.hpc_cluster import HpcClusterConfiguration
from app.config.model.study import StudySettings




@lru_cache(1)
def get_study_settings() -> StudySettings:
    settings = get_settings().study
    return settings


@lru_cache(1)
def get_cluster_settings() -> HpcClusterConfiguration:
    settings = get_settings().hpc_cluster.configuration

    return settings

