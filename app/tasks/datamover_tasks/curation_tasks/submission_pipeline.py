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
from app.utils import MetabolightsException
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
    study_id = params.get("study_id")

    if not study_id:
        raise MetabolightsException("validate_study task: Study id is not valid")

    obfuscation_code = params.get("obfuscation_code")
    if not obfuscation_code:
        raise MetabolightsException(
            "validate_study task: obfuscation_code id is not valid"
        )
    logger.info(f"{study_id} rsync_metadata_files is running...")
    if params.get("test"):
        return params
    message = ""
    try:
        rsync_arguments = "-auv"
        include_list = ["[asi]_*.txt", "m_*.tsv"]
        exclude_list = ["*"]
        settings = get_settings()
        mounted_paths = settings.hpc_cluster.datamover.mounted_paths
        source_path = os.path.join(
            mounted_paths.cluster_private_ftp_root_path,
            f"{study_id.lower()}-{obfuscation_code}",
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
            params["sync_metadata_files_task_status"] = True
            return params
        message = ", ".join(result.stderr or [])
    except Exception as ex:
        message = str(ex)
        logger.error(message)
    revert_ftp_folder_permission_task.apply_async(kwargs={"params": params})
    report_internal_technical_issue(
        f"Sync metadata files on {study_id} Private FTP folder task failed.",
        f"Sync metadata files on {study_id} Private FTP folder task result: {message}.",
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
    # model = RevalidateStudyParameters.model_validate(params)
    study_id = params.get("study_id")

    if not study_id:
        raise MetabolightsException("validate_study task: Study id is not valid")
    message = ""
    try:
        logger.info(f"{study_id} make_ftp_folder_readonly_task is running...")
        if params.get("test"):
            _, permissions = get_private_ftp_path(study_id)
            params["current_private_ftp_permission"] = permissions
            return params
        status, current_private_ftp_permission = update_folder_status(
            study_id, Acl.AUTHORIZED_READ.value
        )
        if status:
            params["make_ftp_folder_readonly_task_status"] = True
            params["current_private_ftp_permission"] = current_private_ftp_permission
            return params
    except Exception as ex:
        message = str(ex)
        logger.error(message)

    report_internal_technical_issue(
        f"Make {study_id} Private FTP folder readonly task failed.",
        f"Check and maintain {study_id} Private FTP folder. {message}",
    )
    raise StudySubmissionError(
        f"Make {study_id} Private FTP folder readonly task failed."
    )


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=1,
    max_retries=1,
    name="app.tasks.datamover_tasks.curation_tasks.submission_pipeline.revert_ftp_folder_permission_task",
)
def revert_ftp_folder_permission_task(self, params: dict[str, Any]):
    # model = RevalidateStudyParameters.model_validate(params)
    study_id = params.get("study_id")
    if not study_id:
        raise MetabolightsException("validate_study task: Study id is not valid")
    logger.info(f"{study_id} revert_ftp_folder_permission_task is running...")
    message = ""
    try:
        if params.get("test"):
            return params
        prev_permission = params.get("current_private_ftp_permission")
        _, permissions = get_private_ftp_path(study_id)
        if prev_permission and prev_permission == permissions:
            params["revert_ftp_folder_permission_task"] = True
            return params
        status = update_folder_status(study_id, Acl.AUTHORIZED_READ_WRITE.value)
        if status:
            params["revert_ftp_folder_permission_task"] = True
            return params
        
    except Exception as ex:
        message = str(ex)
        logger.error(message)

    report_internal_technical_issue(
        f"Make {study_id} Private FTP folder writable task failed.",
        f"Check {study_id} Private FTP folder and maintain. {message}.",
    )
    raise StudySubmissionError(f"{study_id} Private FTP folder writable task failed")


def get_private_ftp_path(study_id: str) -> int:
    study: Study = StudyService.get_instance().get_study_by_acc(study_id)
    mounted_paths = get_settings().hpc_cluster.datamover.mounted_paths
    private_ftp_root_path = mounted_paths.cluster_private_ftp_root_path
    study_private_ftp_path = os.path.join(
        private_ftp_root_path, f"{study.acc.lower()}-{study.obfuscationcode}"
    )
    current_private_ftp_permission = os.stat(study_private_ftp_path).st_mode & 0o777
    return study_private_ftp_path, current_private_ftp_permission


def update_folder_status(study_id: str, permission: int):
    study_private_ftp_path, current_private_ftp_permission = get_private_ftp_path(
        study_id
    )
    result = file_management.update_permission(study_private_ftp_path, permission)

    if result and result.get("status"):
        return True, current_private_ftp_permission
    return False, None


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=1,
    max_retries=1,
    name="app.tasks.datamover_tasks.curation_tasks.submission_pipeline.index_study_data_files_task",
)
def index_study_data_files_task(self, params: dict[str, Any]):
    study_id = params.get("study_id")

    if not study_id:
        raise MetabolightsException("validate_study task: Study id is not valid")

    obfuscation_code = params.get("obfuscation_code")
    if not obfuscation_code:
        raise MetabolightsException(
            "index_study_data_files_task task: obfuscation_code id is not valid"
        )

    try:
        logger.info(f"{study_id} index_study_data_files_task is running...")
        if params.get("test"):
            return params
        result = index_study_data_files(study_id, obfuscation_code)

        if result and result.get("target_path"):
            params["index_data_files_task_status"] = True
            return params
        raise MetabolightsException(
            "index_study_data_files_task task failed", str(result)
        )
    except Exception as ex:
        revert_ftp_folder_permission_task.apply_async(kwargs={"params": params})
        report_internal_technical_issue(
            f"Index {study_id} data files task failed.",
            f"Index {study_id} data files task failed. {str(ex)}.",
        )
        raise ex
