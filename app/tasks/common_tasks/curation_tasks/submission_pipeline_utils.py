import logging
from typing import Any

from app.tasks.worker import (
    MetabolightsTask,
    celery,
)
from app.utils import MetabolightsException
from app.ws.db.schemes import Study
from app.ws.db.types import StudyStatus
from app.ws.study.study_service import StudyService
from app.ws.study_status_utils import StudyStatusHelper

logger = logging.getLogger(__name__)


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=1,
    max_retries=1,
    name="app.tasks.common_tasks.curation_tasks.submission_pipeline_utils.revert_db_status_task",
)
def revert_db_status_task(self, params: dict[str, Any]):
    study_id = params.get("study_id")
    task_name = params.get("task_name", "")
    if task_name is None:
        raise MetabolightsException(
            "from_provisional_to_private task: task_name is not valid"
        )
    if not study_id:
        raise MetabolightsException(
            "from_provisional_to_private task: Study id is not valid"
        )
    current_status = params.get("current_status")
    if current_status == StudyStatus.PROVISIONAL:
        db_study: Study = StudyService.get_study_by_req_or_mtbls_id(study_id)
        StudyStatusHelper.update_status(
            study_id,
            current_status.name.lower(),
            first_public_date=db_study.first_public_date,
            first_private_date=db_study.first_private_date,
        )
