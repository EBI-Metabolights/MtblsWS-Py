
import datetime
import glob
import json
import logging
import os
import time
import celery
from app.config import get_settings
from app.tasks.worker import (MetabolightsTask, celery)
from app.tasks.worker import MetabolightsTask
from app.ws.db.schemes import Study

from app.ws.settings.utils import get_study_settings
from app.ws.study.study_service import StudyService
from app.ws.study.validation.commons import update_validation_schema_files

logger = logging.getLogger('wslog')


@celery.task(bind=True, base=MetabolightsTask, default_retry_delay=10, max_retries=3, soft_time_limit=60*15, name="app.tasks.common_tasks.curation_tasks.validation.update_validation_files")
def update_validation_files(self, study_id: str, user_token: str):
    settings = get_study_settings()
    file_path = os.path.join(settings.mounted_paths.study_internal_files_root_path, study_id)
    validation_report_file_name = settings.validation_report_file_name
    validation_file = os.path.join(file_path, validation_report_file_name)
    study: Study = StudyService.get_instance().get_study_by_acc(study_id)
    self.update_state(state='STARTED', meta={'study_id': study_id})
    return update_validation_schema_files(validation_file, study_id, user_token, study.obfuscationcode)