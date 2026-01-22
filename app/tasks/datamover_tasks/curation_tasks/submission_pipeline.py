import logging
import os
from typing import Any

from app.config import get_settings
from app.services.storage_service.acl import Acl
from app.tasks.common_tasks.curation_tasks.submission_model import (
    MakeStudyPrivateParameters,
    StudySubmissionError,
)
from app.tasks.datamover_tasks.basic_tasks import file_management
from app.tasks.datamover_tasks.basic_tasks.ftp_operations import index_study_data_files
from app.tasks.worker import MetabolightsTask, celery
from app.ws.db.schemes import Study
from app.ws.study.study_service import StudyService

logger = logging.getLogger(__name__)


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=1,
    max_retries=1,
    name="app.tasks.datamover_tasks.curation_tasks.submission_pipeline.make_ftp_folder_readonly_task",
)
def make_ftp_folder_readonly_task(self, params: dict[str, Any]):
    model = MakeStudyPrivateParameters.model_validate(params)
    logger.info(f"{model.study_id} make_ftp_folder_readonly_task is running...")
    if model.test:
        return params
    status = update_folder_status(model, Acl.AUTHORIZED_READ.value)
    if status:
        model.index_data_files_task_status = True
        return model.model_dump()
    raise StudySubmissionError("FTP folder permission update failed")


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=1,
    max_retries=1,
    name="app.tasks.datamover_tasks.curation_tasks.submission_pipeline.make_ftp_folder_writable_task",
)
def make_ftp_folder_writable_task(self, params: dict[str, Any]):
    model = MakeStudyPrivateParameters.model_validate(params)
    logger.info(f"{model.study_id} make_ftp_folder_writable_task is running...")
    if model.test:
        return params
    status = update_folder_status(model, Acl.AUTHORIZED_READ_WRITE.value)
    if status:
        model.index_data_files_task_status = True
        return model.model_dump()
    raise StudySubmissionError("FTP folder permission update failed")


def update_folder_status(model: MakeStudyPrivateParameters, permission: int):
    study: Study = StudyService.get_instance().get_study_by_acc(model.study_id)
    mounted_paths = get_settings().hpc_cluster.datamover.mounted_paths
    private_ftp_root_path = mounted_paths.cluster_private_ftp_root_path
    study_private_ftp_path = os.path.join(
        private_ftp_root_path, f"{study.acc.lower()}-{study.obfuscationcode}"
    )
    model.current_private_ftp_permission = (
        os.stat(study_private_ftp_path).st_mode & 0o777
    )

    result = file_management.update_permission(study_private_ftp_path, permission)

    if result and result.get("status"):
        return True
    return False


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=1,
    max_retries=1,
    name="app.tasks.datamover_tasks.curation_tasks.submission_pipeline.index_study_data_files_task",
)
def index_study_data_files_task(self, params: dict[str, Any]):
    try:
        model = MakeStudyPrivateParameters.model_validate(params)
        logger.info(f"{model.study_id} index_study_data_files_task is running...")
        if model.test:
            return params
        result = index_study_data_files(model.study_id, model.obfuscation_code)

        if result and result.get("target_path"):
            model.index_data_files_task_status = True
            return model.model_dump()
        model.index_data_files_task_status = False
        return model.model_dump()
    except Exception as ex:
        make_ftp_folder_writable_task.apply_async(kwargs={"params": params})
        raise ex
