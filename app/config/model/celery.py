from pydantic import BaseModel

from app.config.model import redis_cache


class CeleryConfiguration(BaseModel):
    celery_task_acks_late: bool = True
    celery_task_acks_on_failure_or_timeout: bool = False
    celery_task_reject_on_worker_lost: bool = True
    celery_task_track_started: bool = True
    celery_result_expires: int = 5 * 60


class CeleryPeriodicTaskConfiguration(BaseModel):
    integration_test_period_in_seconds: int = 60
    es_compound_sync_task_period_in_secs: int = 600
    es_study_sync_task_period_in_secs: int = 600
    worker_heath_check_period_in_seconds: int = 600


class CelerySettings(BaseModel):
    broker: redis_cache.RedisConnection
    configuration: CeleryConfiguration
    periodic_task_configuration: CeleryPeriodicTaskConfiguration
