from functools import lru_cache
from pydantic import BaseSettings


class StudySettings(BaseSettings):
    max_study_in_submitted_status: int = 2
    min_study_creation_interval_in_mins: int = 5


@lru_cache(1)
def get_study_settings(app) -> StudySettings:
    settings = StudySettings()

    return settings