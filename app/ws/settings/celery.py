

from app.ws.settings.base import MetabolightsBaseSettings


class CelerySettings(MetabolightsBaseSettings):
    celery_task_acks_late: bool = True
    celery_task_acks_on_failure_or_timeout: bool = False
    celery_task_reject_on_worker_lost: bool = True
    celery_task_track_started: bool = True
    celery_result_expires: int = 5 * 60
