import logging
import os
from typing import Any


from app.config import get_settings
from app.services.storage_service.acl import Acl
from app.tasks.common_tasks.curation_tasks.submission_model import (
    RevalidateStudyParameters,
    StudySubmissionError,
)
from app.tasks.datamover_tasks.basic_tasks.ftp_operations import index_study_data_files
from app.tasks.worker import MetabolightsTask, celery, report_internal_technical_issue
from app.ws.db.schemes import Study
from app.tasks.datamover_tasks.basic_tasks import file_management
from app.ws.study.study_service import StudyService

from app.tasks.bash_client import BashClient, CapturedBashExecutionResult
from app.tasks.hpc_rsync_worker import HpcRsyncWorker
from app.ws.study.folder_utils import write_audit_files

logger = logging.getLogger(__name__)


@celery.task(
    bind=True,
    base=MetabolightsTask,
    max_retries=1,
    soft_time_limit=60,
    name="app.tasks.datamover_tasks.curation_tasks.submission_pipeline.rsync_metadata_files",
)
def rsync_metadata_files(self, params: dict[str, Any]):
    model = RevalidateStudyParameters.model_validate(params)
    message = ""
    try:
        study_id = model.study_id
        rsync_arguments = "-auv"
        include_list = ["[asi]_*.txt", "m_*.tsv"]
        exclude_list = ["*"]
        settings = get_settings()
        mounted_paths = settings.hpc_cluster.datamover.mounted_paths
        source_path = os.path.join(
            mounted_paths.cluster_private_ftp_root_path,
            f"{study_id.lower()}-{model.obfuscation_code}",
        )
        # source_path = os.path.join(
        #     mounted_paths.cluster_rw_storage_recycle_bin_root_path, study_id
        # )
        target_path = os.path.join(
            mounted_paths.cluster_study_metadata_files_root_path, study_id
        )
        command = HpcRsyncWorker.build_rsync_command(
            source_path,
            target_path,
            include_list,
            exclude_list,
            rsync_arguments=rsync_arguments,
        )

        task_name = "rsync:from:private_ftp_metadata:to:rw_metadata"

        write_audit_files(study_id)

        result: CapturedBashExecutionResult = BashClient.execute_command(
            command=command,
            email=None,
            task_name=task_name,
        )

        if result.returncode == 0:
            model.sync_metadata_files_task_status = True
            return model.model_dump()
        message = ", ".join(result.stderr or [])
    except Exception as ex:
        message = str(ex)
        logger.error(message)
    make_ftp_folder_writable_task.apply_async(kwargs={"params": params})
    report_internal_technical_issue(
        f"Sync metadata files on {model.study_id} Private FTP folder task failed.",
        f"Sync metadata files on {model.study_id} Private FTP folder task result: {message}.",
    )
    raise StudySubmissionError("FTP folder metadtata file synchronization failed")


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=1,
    max_retries=1,
    name="app.tasks.datamover_tasks.curation_tasks.submission_pipeline.make_ftp_folder_readonly_task",
)
def make_ftp_folder_readonly_task(self, params: dict[str, Any]):
    model = RevalidateStudyParameters.model_validate(params)
    message = ""
    try:
        logger.info(f"{model.study_id} make_ftp_folder_readonly_task is running...")
        if model.test:
            return params
        status = update_folder_status(model, Acl.AUTHORIZED_READ.value)
        if status:
            model.make_ftp_folder_readonly_task_status = True
            return model.model_dump()
    except Exception as ex:
        message = str(ex)
        logger.error(message)

    report_internal_technical_issue(
        f"Make {model.study_id} Private FTP folder readonly task failed.",
        f"Check and maintain {model.study_id} Private FTP folder. {message}",
    )
    raise StudySubmissionError(
        f"Make {model.study_id} Private FTP folder readonly task failed."
    )


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=1,
    max_retries=1,
    name="app.tasks.datamover_tasks.curation_tasks.submission_pipeline.make_ftp_folder_writable_task",
)
def make_ftp_folder_writable_task(self, params: dict[str, Any]):
    model = RevalidateStudyParameters.model_validate(params)
    logger.info(f"{model.study_id} make_ftp_folder_writable_task is running...")
    message = ""
    try:
        if model.test:
            return params
        status = update_folder_status(model, Acl.AUTHORIZED_READ_WRITE.value)
        if status:
            model.index_data_files_task_status = True
            return model.model_dump()
    except Exception as ex:
        message = str(ex)
        logger.error(message)

    report_internal_technical_issue(
        f"Make {model.study_id} Private FTP folder writable task failed.",
        f"Check {model.study_id} Private FTP folder and maintain. {message}.",
    )
    raise StudySubmissionError(
        f"{model.study_id} Private FTP folder writable task failed"
    )


def update_folder_status(model: RevalidateStudyParameters, permission: int):
    study: Study = StudyService.get_instance().get_study_by_acc(model.study_id)
    mounted_paths = get_settings().hpc_cluster.datamover.mounted_paths
    private_ftp_root_path = mounted_paths.cluster_private_ftp_root_path
    study_private_ftp_path = os.path.join(
        private_ftp_root_path, f"{study.acc.lower()}-{study.obfuscationcode}"
    )
    model.current_private_ftp_permission =  os.stat(study_private_ftp_path).st_mode & 0o777
    
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
    model = RevalidateStudyParameters.model_validate(params)
    try:
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
        report_internal_technical_issue(
            f"Index {model.study_id} data files task failed.",
            f"Index {model.study_id} data files task failed. {str(ex)}.",
        )
        raise ex
