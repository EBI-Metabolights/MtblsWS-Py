import json
import time
import logging
import os
from pathlib import Path
import shutil
from typing import Any


from app.config import get_settings
from app.services.storage_service.acl import Acl
from app.tasks.common_tasks.curation_tasks.submission_model import StudySubmissionError

from app.tasks.common_tasks.curation_tasks.submission_pipeline_utils import revert_db_status_task
from app.tasks.datamover_tasks.basic_tasks.ftp_operations import index_study_data_files
from app.tasks.worker import MetabolightsTask, celery, report_internal_technical_issue
from app.utils import MetabolightsException
from app.ws.db.schemes import Study
from app.tasks.datamover_tasks.basic_tasks import file_management
from app.ws.db.types import StudyStatus
from app.ws.study.study_service import StudyService
from app.ws.study_status_utils import StudyStatusHelper


logger = logging.getLogger(__name__)


def is_hidden(p: Path) -> bool:
    return any(part.startswith(".") for part in p.parts)


def gather_matches(base: Path, patterns, recursive=False):
    matches = []
    for pat in patterns:
        glob_iter = base.rglob(pat) if recursive else base.glob(pat)
        for p in glob_iter:
            if not p.is_file():
                continue
            if is_hidden(p.relative_to(base).parent):
                continue
            matches.append(p)
    return matches


def copy_metadata_files(
    source_path: str | Path, dest_path: str | Path, recursive: bool = True
):
    if isinstance(source_path, str):
        source_path = Path(source_path)
    if isinstance(dest_path, str):
        dest_path = Path(dest_path)

    patterns = ("[asim]_*.txt", "[asim]_*.tsv")
    if not source_path.exists():
        logger.error("Source path does not exist: %s", str(source_path))
        return False
    files: list[Path] = gather_matches(source_path, patterns, recursive=recursive)

    if not files:
        logger.info("There is no metadata file on %s.", str(source_path))
        return True

    logger.info("%s metadata files are found.", len(files))
    try:
        dest_path.mkdir(parents=True, exist_ok=True)
        logger.info("Destination path is created: %s", str(dest_path))
        for f in files:
            target = dest_path / f.name
            # Resolve name clashes by appending a counter
            counter = 1
            while target.exists():
                target = dest_path / f"{f.stem}_{counter}{f.suffix}"
                counter += 1
            shutil.copy2(str(f), str(target))
            logger.info("Metadata file is copied: %s  ->  %s", f, target)
        logger.info("Metadata files are moved.")
        return True
    except Exception as ex:
        logger.error("Copy file task failed: %s", str(ex))
        if dest_path.exists():
            shutil.rmtree(str(dest_path))
        return False
    # try:
    #     for f in files:
    #         f.unlink()
    #         logger.info("Metadata file is deleted: %s", f)
    #     return True
    # except Exception as ex:
    #     logger.error("Delete file task failed: %s", str(ex))

    # return False


@celery.task(
    bind=True,
    base=MetabolightsTask,
    max_retries=1,
    soft_time_limit=60,
    name="app.tasks.datamover_tasks.curation_tasks.submission_pipeline.backup_metadata_files_on_private_ftp",
)
def backup_metadata_files_on_private_ftp(self, params: dict[str, Any]):
    study_id: None | str = params.get("study_id")
    task_name: None | str = params.get("task_name")
    if task_name is None:
        raise MetabolightsException(
            "backup_metadata_files_on_private_ftp task: task_name is not valid"
        )
    if not study_id:
        raise MetabolightsException("validate_study task: Study id is not valid")

    obfuscation_code = params.get("obfuscation_code")
    if not obfuscation_code:
        raise MetabolightsException(
            "validate_study task: obfuscation_code id is not valid"
        )
    logger.info(f"{study_id} rsync_metadata_files is running...")
    if params.get("test"):
        logger.info(f"{study_id} rsync_metadata_files is in test mode. Skipping...")
        return params
    message = ""

    try:
        settings = get_settings()
        mounted_paths = settings.hpc_cluster.datamover.mounted_paths
        source_path = os.path.join(
            mounted_paths.cluster_private_ftp_root_path,
            f"{study_id.lower()}-{obfuscation_code}",
        )
        date_format = "%Y-%m-%d_%H-%M-%S"
        folder_name = time.strftime(date_format) + "_PRIVATE_FTP_BACKUP"
        dest_path = os.path.join(
            mounted_paths.cluster_study_audit_files_root_path,
            study_id,
            settings.study.audit_folder_name,
            folder_name,
        )
        success = copy_metadata_files(
            source_path=Path(source_path), dest_path=dest_path
        )

        if success:
            params["backup_metadata_files_on_private_ftp"] = True
            return params
        message = "Metadata file copy task failed."
    except Exception as ex:
        message = str(ex)
        logger.error(message)

    revert_ftp_folder_permission_task.apply_async(kwargs={"params": params})
    revert_db_status_task.apply_async(kwargs={"params": params})
    params_str = json.dumps(params, indent=2)
    params_str = params_str.replace("\n", "<br/>")
    logger.error(f"Validate {study_id} task failed. <br/>{params_str}")
    report_internal_technical_issue(
        f"{task_name} task failed.",
        f"FTP folder metadata file synchronization {study_id} task failed. <br/>{params_str}",
    )
    raise StudySubmissionError("FTP folder metadata file synchronization failed")


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
    task_name = params.get("task_name", "")
    if task_name is None:
        raise MetabolightsException(
            "make_ftp_folder_readonly_task task: task_name is not valid"
        )
    if not study_id:
        raise MetabolightsException("validate_study task: Study id is not valid")
    message = ""
    try:
        logger.info(f"{study_id} make_ftp_folder_readonly_task is running...")
        if params.get("test"):
            logger.info(
                f"{study_id} make_ftp_folder_readonly_task is in test mode. Skipping..."
            )
            params["current_private_ftp_permission"] = 0o750
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

    params_str = json.dumps(params, indent=2)
    params_str = params_str.replace("\n", "<br/>")
    logger.error(f"Validate {study_id} task failed. <br/>{str(ex)}. <br/>{params_str}")
    report_internal_technical_issue(
        f"{task_name} task failed.",
        f"Make {study_id} Private FTP folder readonly task failed. <br>"
        f"Check and maintain {study_id} Private FTP folder. <br>"
        f"<br/> {message}. <br/>{params_str}",
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
    task_name = params.get("task_name", "")
    if task_name is None:
        raise MetabolightsException(
            "revert_ftp_folder_permission_task task: task_name is not valid"
        )
    if not study_id:
        raise MetabolightsException("validate_study task: Study id is not valid")
    logger.info(f"{study_id} revert_ftp_folder_permission_task is running...")
    message = ""
    try:
        if params.get("test"):
            logger.info(
                f"{study_id} revert_ftp_folder_permission_task is in test mode. Skipping..."
            )
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

    params_str = json.dumps(params, indent=2)
    params_str = params_str.replace("\n", "<br/>")
    logger.error(f"Validate {study_id} task failed. <br/>{str(ex)}. <br/>{params_str}")
    report_internal_technical_issue(
        f"{task_name} task failed.",
        f"Revert {study_id} Private FTP folder permission task failed. <br>"
        f"Check and maintain {study_id} Private FTP folder. <br>"
        f"<br/> {message}. <br/>{params_str}",
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
    task_name = params.get("task_name", "")
    if task_name is None:
        raise MetabolightsException(
            "index_study_data_files_task task: task_name is not valid"
        )
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
            logger.info(
                f"{study_id} index_study_data_files_task is in test mode. Skipping..."
            )
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
        revert_db_status_task.apply_async(kwargs={"params": params})
        params_str = json.dumps(params, indent=2)
        params_str = params_str.replace("\n", "<br/>")
        logger.error(
            f"Validate {study_id} task failed. <br/>{str(ex)}. <br/>{params_str}"
        )
        report_internal_technical_issue(
            f"{task_name} task failed.",
            f"Index {study_id} data files task failed. <br/>{str(ex)}. <br/>{params_str}",
        )
        raise ex


if __name__ == "__main__":
    copy_metadata_files(Path("x_backup_folder"), Path("target_move"))
