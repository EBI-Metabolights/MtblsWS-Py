from functools import lru_cache

from app.ws.settings.base import MetabolightsBaseSettings


class StudySettings(MetabolightsBaseSettings):
    max_study_in_submitted_status: int = 2
    min_study_creation_interval_in_mins: int = 5