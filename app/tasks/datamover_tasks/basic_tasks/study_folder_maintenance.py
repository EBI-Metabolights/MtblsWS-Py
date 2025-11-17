import datetime
import json
import logging
import os
import shutil
from typing import Union

import pandas as pd

from app.config import get_settings
from app.tasks.worker import (
    MetabolightsTask,
    celery,
    report_internal_technical_issue,
    send_email,
)
from app.utils import MetabolightsDBException, current_time
from app.ws.auth.auth_manager import AuthenticationManager
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study, User
from app.ws.db.types import StudyStatus
from app.ws.folder_maintenance import StudyFolderMaintenanceTask
from app.ws.settings.utils import get_study_settings
from app.ws.study.user_service import UserService

logger = logging.getLogger(__name__)


def create_study_folders(
    all_results,
    study: Study,
    maintenance_task: StudyFolderMaintenanceTask,
    maintain_metadata_storage,
    maintain_private_ftp_storage,
    failing_gracefully,
):
    try:
        study_id = study.acc
        study_status = StudyStatus(study.status)

        if maintain_metadata_storage:
            maintenance_task.maintain_study_rw_storage_folders()
            
        if maintain_private_ftp_storage:
            maintenance_task.create_maintenace_actions_for_study_private_ftp_folder()

        rows = []
        for action_log in maintenance_task.actions:
            success = action_log.successful
            action_name = action_log.action.name
            item = action_log.item
            message = (
                action_log.message.replace(item, "")
                if action_log.message
                else action_log.message
            )
            parameters = action_log.parameters
            rows.append(
                [
                    f"{study_id}",
                    f"{study_status.name}",
                    f"{success}",
                    f"{action_name}",
                    f"{item}",
                    f"{message}",
                    f"{parameters}",
                ]
            )

        all_results.extend(rows)

    except Exception as exc:
        all_results.append(
            [
                f"{study_id}",
                f"{study_status.name}",
                "ERROR",
                "ERROR_MESSAGE",
                f"{study_id}",
                f"{str(exc)}",
                "",
            ]
        )
        if not failing_gracefully:
            raise exc


@celery.task(
    base=MetabolightsTask,
    name="app.tasks.datamover_tasks.basic_tasks.study_folder_maintenance.delete_study_folders",
)
def delete_study_folders(
    study_id: Union[None, str] = None,
    force_to_maintain=False,
    delete_metadata_storage_folders=True,
    delete_private_ftp_storage_folders=True,
    failing_gracefully=False,
    task_name=None,
):
    try:
        with DBManager.get_instance().session_maker() as db_session:
            study: Study = db_session.query(Study).filter(Study.acc == study_id).first()

            if not study:
                raise MetabolightsDBException("No study found on db.")

            study_status = StudyStatus(study.status)
            maintenance_task = StudyFolderMaintenanceTask(
                study.acc,
                study_status,
                study.releasedate,
                study.submissiondate,
                task_name=task_name,
                obfuscationcode=study.obfuscationcode,
                delete_unreferenced_metadata_files=False,
                settings=get_study_settings(),
                apply_future_actions=True,
                force_to_maintain=force_to_maintain,
                cluster_execution_mode=True,
                mhd_accession=study.mhd_accession,
                mhd_model_version=study.mhd_model_version,
                study_category=study.study_category,
                sample_template=study.sample_type,
                dataset_license=study.dataset_license,
                template_version=study.template_version,
            )
            all_results = []
            if delete_metadata_storage_folders:
                maintenance_task.delete_rw_storage_folders()
            if delete_private_ftp_storage_folders:
                maintenance_task.delete_private_ftp_storage_folders()

            create_study_folders(
                all_results,
                study,
                maintenance_task,
                maintain_metadata_storage=delete_metadata_storage_folders,
                maintain_private_ftp_storage=delete_private_ftp_storage_folders,
                failing_gracefully=failing_gracefully,
            )

        return True
    except Exception as ex:
        raise ex


@celery.task(
    base=MetabolightsTask,
    name="app.tasks.datamover_tasks.basic_tasks.study_folder_maintenance.create_links_on_data_storage",
)
def create_links_on_data_storage(study_id: str, updated_study_id: str):
    mounted_paths = get_settings().study.mounted_paths
    managed_paths = [
        mounted_paths.study_readonly_audit_files_actual_root_path,
        mounted_paths.study_readonly_files_actual_root_path,
        mounted_paths.study_readonly_integrity_check_files_root_path,
        mounted_paths.study_readonly_public_metadata_versions_root_path,
        mounted_paths.study_readonly_metadata_files_root_path,
    ]

    for root_path in managed_paths:
        new_path = os.path.join(root_path, updated_study_id)
        current_path = os.path.join(root_path, study_id)
        if not os.path.exists(new_path):
            if os.path.islink(new_path):
                current_target = os.readlink(new_path)
                if current_target == new_path:
                    continue
                else:
                    os.unlink(new_path)

            if os.path.exists(new_path):
                continue
            os.symlink(current_path, new_path, target_is_directory=True)


@celery.task(
    base=MetabolightsTask,
    name="app.tasks.datamover_tasks.basic_tasks.study_folder_maintenance.create_links_on_private_storage",
)
def create_links_on_private_storage(
    study_id: str, updated_study_id: str, obfuscation_code: str
):
    folder_name = f"{study_id.lower()}-{obfuscation_code}"
    new_folder_name = f"{updated_study_id.lower()}-{obfuscation_code}"
    mounted_paths = get_settings().hpc_cluster.datamover.mounted_paths
    new_path = os.path.join(
        mounted_paths.cluster_private_ftp_root_path, new_folder_name
    )
    current_path = os.path.join(
        mounted_paths.cluster_private_ftp_root_path, folder_name
    )
    if not os.path.exists(new_path):
        if os.path.islink(new_path):
            current_target = os.readlink(new_path)
            if current_target != new_path:
                os.unlink(new_path)
        if not os.path.exists(new_path):
            os.symlink(current_path, new_path, target_is_directory=True)


@celery.task(
    base=MetabolightsTask,
    name="app.tasks.datamover_tasks.basic_tasks.study_folder_maintenance.rename_folder_on_private_storage",
)
def rename_folder_on_private_storage(
    study_id: str, updated_study_id: str, obfuscation_code: str
):
    folder_name = f"{study_id.lower()}-{obfuscation_code}"
    new_folder_name = f"{updated_study_id.lower()}-{obfuscation_code}"
    mounted_paths = get_settings().hpc_cluster.datamover.mounted_paths
    new_path = os.path.join(
        mounted_paths.cluster_private_ftp_root_path, new_folder_name
    )
    current_path = os.path.join(
        mounted_paths.cluster_private_ftp_root_path, folder_name
    )
    if study_id == updated_study_id:
        if not os.path.exists(current_path):
            if os.path.islink(current_path):
                os.unlink(current_path)
            os.makedirs(current_path, exist_ok=True)
        return
    if os.path.islink(current_path):
        os.unlink(current_path)
    if os.path.islink(new_path):
        os.unlink(new_path)
    timestamp = str(int(datetime.datetime.now().timestamp()))
    recycle_path = os.path.join(
        mounted_paths.cluster_private_ftp_recycle_bin_root_path,
        new_folder_name,
        timestamp,
    )
    if os.path.exists(new_path):
        shutil.move(new_path, recycle_path)
    if not os.path.exists(current_path):
        os.makedirs(current_path, exist_ok=True)
    shutil.move(current_path, new_path)


@celery.task(
    base=MetabolightsTask,
    name="app.tasks.datamover_tasks.basic_tasks.study_folder_maintenance.maintain_storage_study_folders",
)
def maintain_storage_study_folders(
    user_token: str,
    send_email_to_submitter=False,
    study_id: Union[None, str] = None,
    force_to_maintain=False,
    maintain_metadata_storage=True,
    maintain_private_ftp_storage=True,
    cluster_execution_mode=True,
    failing_gracefully=False,
    task_name=None,
):
    all_results = []
    headers = [
        "STUDY_ID",
        "STUDY STATUS",
        "STATUS",
        "ACTION",
        "ITEM",
        "MESSAGE",
        "PARAMETERS",
    ]
    email = None
    studies = []
    try:
        with DBManager.get_instance().session_maker() as db_session:
            auth_manager = AuthenticationManager.get_instance()
            if study_id:
                user: User = UserService.get_instance(
                    auth_manager
                ).validate_user_has_write_access(user_token, study_id=study_id)
            else:
                user: User = UserService.get_instance(
                    auth_manager
                ).validate_user_has_curator_role(user_token)

            if not user:
                raise MetabolightsDBException("No user")

            email = user[1]

            if not email or not isinstance(email, str):
                raise MetabolightsDBException("User email is not valid")

            if study_id:
                studies = db_session.query(Study).filter(Study.acc == study_id).all()
            else:
                studies = db_session.query(Study).all()

            if not studies:
                raise MetabolightsDBException("No study found on db.")

        for item in studies:
            study: Study = item
            try:
                study_status = StudyStatus(study.status)
                maintenance_task = StudyFolderMaintenanceTask(
                    study.acc,
                    study_status,
                    study.releasedate,
                    study.submissiondate,
                    task_name=task_name,
                    obfuscationcode=study.obfuscationcode,
                    delete_unreferenced_metadata_files=False,
                    settings=get_study_settings(),
                    apply_future_actions=True,
                    force_to_maintain=force_to_maintain,
                    cluster_execution_mode=cluster_execution_mode,
                    mhd_accession=study.mhd_accession,
                    mhd_model_version=study.mhd_model_version,
                    study_category=study.study_category,
                    sample_template=study.sample_type,
                    dataset_license=study.dataset_license,
                    template_version=study.template_version,
                )

                if maintain_metadata_storage:
                    maintenance_task.maintain_study_rw_storage_folders()
                if maintain_private_ftp_storage:
                    maintenance_task.create_maintenace_actions_for_study_private_ftp_folder()
                rows = []
                for action_log in maintenance_task.actions:
                    success = action_log.successful
                    action_name = action_log.action.name
                    item = action_log.item
                    message = (
                        action_log.message.replace(item, "")
                        if action_log.message
                        else action_log.message
                    )
                    parameters = action_log.parameters
                    rows.append(
                        [
                            f"{study_id}",
                            f"{study_status.name}",
                            f"{success}",
                            f"{action_name}",
                            f"{item}",
                            f"{message}",
                            f"{parameters}",
                        ]
                    )

                all_results.extend(rows)

            except Exception as exc:
                all_results.append(
                    [
                        f"{study_id}",
                        f"{study_status.name}",
                        "ERROR",
                        "ERROR_MESSAGE",
                        f"{study_id}",
                        f"{str(exc)}",
                        "",
                    ]
                )
                if not failing_gracefully:
                    raise exc
        df = pd.DataFrame(all_results, columns=headers)

        result = {
            "time": current_time().strftime("%Y-%m-%d %H:%M:%S"),
            "executed_on": os.uname().nodename,
            "result": "Listed Below",
        }
        result_str = json.dumps(result, indent=4)
        result_str = (
            result_str + "<p>" + df.to_html().replace('border="1"', 'border="0"')
        )
        if send_email_to_submitter:
            send_email(
                "Result of the task: maintain MetaboLights study folders",
                result_str,
                None,
                email,
                None,
            )
        return result_str
    except Exception as ex:
        if send_email_to_submitter:
            result_str = str(ex).replace("\n", "<p>")
            if all_results:
                df = pd.DataFrame(all_results, columns=headers)
                result_str = (
                    result_str
                    + "<p>"
                    + df.to_html().replace('border="1"', 'border="0"')
                )
            if email:
                send_email(
                    "A task was failed: maintain MetaboLights study folders",
                    result_str,
                    None,
                    email,
                    None,
                )
            else:
                report_internal_technical_issue(
                    "A task was failed: maintain MetaboLights study folders", result_str
                )
        raise ex
