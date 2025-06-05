import logging
import os
from pathlib import Path
from app.config import get_settings
from app.services.cluster.hpc_client import HpcClient
from app.services.cluster.hpc_utils import get_new_hpc_datamover_client
from app.tasks.bash_client import BashClient
from app.tasks.worker import MetabolightsTask, celery
from app.utils import MetabolightsException, current_time
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study, StudyRevision

from app.ws.db.types import StudyRevisionStatus, StudyStatus
from app.ws.settings.utils import get_study_settings
from app.ws.study.study_revision_service import StudyRevisionService
from app.ws.study.study_service import StudyService

logger = logging.getLogger("wslog")


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=10,
    max_retries=3,
    soft_time_limit=60 * 15,
    name="app.tasks.common_tasks.curation_tasks.study_revision.prepare_study_revision",
)
def prepare_study_revision(self, study_id: str, user_token: str):
    settings = get_study_settings()
    with DBManager.get_instance().session_maker() as db_session:
        try:
            query = db_session.query(Study)
            study: Study = query.filter(Study.acc == study_id).first()
            
            query = db_session.query(StudyRevision)
            study_revision: StudyRevision = query.filter(
                StudyRevision.accession_number == study_id,
                StudyRevision.revision_number == study.revision_number,
            ).first()
            if not study_revision:
                raise Exception(
                    f"Study revision not found for study {study_id} revision {study.revision_number}"
                )
        except Exception as e:
            raise MetabolightsException(str(e))

    StudyRevisionService.update_investigation_file_for_revision(study_id)

    folder_status, source_path, created_path = (
        StudyRevisionService.create_revision_folder(study)
    )
    # data_files_root_path = settings.mounted_paths.study_readonly_files_actual_root_path
    # study_data_files_path = os.path.join(data_files_root_path, study_id)

    # study_internal_files_root_path = settings.mounted_paths.study_internal_files_root_path
    # revisions_root_hash_path = os.path.join(study_internal_files_root_path, study_id, "PUBLIC_METADATA", "HASHES")
    # StudyRevisionService.create_data_file_hashes(study, search_path=study_data_files_path, copy_paths=[revisions_root_hash_path])

    # StudyRevisionService.check_dataset_integrity(study_id, metadata_files_path=created_path, data_files_path=study_data_files_path)
    kwargs = {
        "study_id": study_id,
        "user_token": user_token,
        "latest_revision": study.revision_number,
    }
    sync_study_revision.apply_async(kwargs=kwargs)
    return {
        "study_id": study_id,
        "create_folder": folder_status,
        "revision_folder_path": created_path,
    }


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=10,
    max_retries=3,
    soft_time_limit=60 * 15,
    name="app.tasks.common_tasks.curation_tasks.study_revision.delete_study_revision",
)
def delete_study_revision(self, study_id: str, revision_number: int):
    revision = StudyRevisionService.delete_study_revision(
        study_id, int(revision_number)
    )

    return {"study_id": study_id, "deleted-revision": revision.model_dump()}


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=10,
    max_retries=3,
    soft_time_limit=60 * 15,
    name="app.tasks.common_tasks.curation_tasks.study_revision.sync_study_metadata_folder",
)
def sync_study_metadata_folder(self, study_id: str, user_token: str):
    study: Study = StudyService.get_instance().get_study_by_acc(study_id)
    if StudyStatus(study.status) != StudyStatus.PUBLIC:
        raise MetabolightsException(message="Only public studies can be sync.")

    try:
        # Copy all revisions to Public FTP folder. METADATA_FILES/<STUDY_ID> to <PUBLIC FTP FOLDER>/<STUDY ID> folder (delete folders/files if it is not on the source folder)
        mounted_paths = get_settings().hpc_cluster.datamover.mounted_paths
        current = current_time().strftime("%Y-%m-%d_%H-%M-%S")
        metadata_files_path = os.path.join(
            mounted_paths.cluster_study_metadata_files_root_path, study_id
        )
        public_study_path = os.path.join(
            mounted_paths.cluster_public_ftp_root_path, study_id
        )
        email = get_settings().email.email_service.configuration.hpc_cluster_job_track_email_address

        private_ftp_root_path = mounted_paths.cluster_private_ftp_root_path
        study_private_ftp_path = os.path.join(
            private_ftp_root_path, f"{study.acc.lower()}-{study.obfuscationcode}"
        )

        job_id, messages = sync_public_ftp_folder_with_metadata_folder(
            study_id=study_id,
            source_path=metadata_files_path,
            target_path=public_study_path,
            user_token=user_token,
            email=email,
            task_name=f"{study_id}_PUBLIC_FTP_SYNC_{current}",
            study_private_ftp_path=study_private_ftp_path,
        )
        return {"job_id": job_id, "messages": messages}
    except Exception as e:
        raise e


def sync_public_ftp_folder_with_metadata_folder(
    task_name: str,
    study_id: str,
    email: str,
    source_path: str,
    target_path: str,
    user_token: str,
    study_private_ftp_path,
):
    settings = get_settings()
    client: HpcClient = get_new_hpc_datamover_client()
    study_log_path = os.path.join(
        settings.study.mounted_paths.study_internal_files_root_path,
        study_id,
        settings.study.internal_logs_folder_name,
        "PUBLIC_FTP_SYNC",
    )
    task_log_path = os.path.join(study_log_path, task_name)
    os.makedirs(task_log_path, exist_ok=True)
    out_log_path = os.path.join(task_log_path, f"{task_name}_out.log")
    err_log_path = os.path.join(task_log_path, f"{task_name}_err.log")

    messages = []
    inputs = {
        "EMAIL_TO": email,
        "STUDY_METADATA_PATH": source_path,
        "STUDY_PUBLIC_FTP_PATH": target_path,
        "STUDY_PRIVATE_FTP_PATH": study_private_ftp_path,
    }
    hpc_queue_name = settings.hpc_cluster.datamover.default_queue

    script_template = "sync_public_ftp_folder_with_metadata_folder.sh.j2"
    script_path = BashClient.prepare_script_from_template(script_template, **inputs)
    logger.info("sync_public_ftp_folder script is ready.")
    logger.info(Path(script_path).read_text())

    try:
        submission_result = client.submit_hpc_job(
            script_path,
            task_name,
            output_file=out_log_path,
            error_file=err_log_path,
            queue=hpc_queue_name,
            account=email,
            mail_type="END",
            mem="5G",
            cpu=2,
            runtime_limit="24:00:00",
        )
        job_id = submission_result.job_ids[0] if submission_result else None

        messages.append(f"New job was submitted with job id {job_id} for {task_name}")
        return job_id, messages
    except Exception as exc:
        message = f"Exception after job submission. {str(exc)}"
        logger.warning(message)
        messages.append(message)
        return None, messages
    finally:
        if script_path and os.path.exists(script_path):
            os.remove(script_path)


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=10,
    max_retries=3,
    soft_time_limit=60 * 15,
    name="app.tasks.common_tasks.curation_tasks.study_revision.sync_study_revision",
)
def sync_study_revision(self, study_id: str, user_token: str, latest_revision: int):
    study: Study = StudyService.get_instance().get_study_by_acc(study_id)

    try:
        # Copy all revisions to Public FTP folder. AUDIT_FILES/PUBLIC_METADATA to <PUBLIC FTP FOLDER>/<STUDY ID> folder (delete folders/files if it is not on the source folder)
        mounted_paths = get_settings().hpc_cluster.datamover.mounted_paths

        revisions_root_path = os.path.join(
            mounted_paths.cluster_study_internal_files_root_path,
            study_id,
            "PUBLIC_METADATA",
        )
        public_study_path = os.path.join(
            mounted_paths.cluster_public_ftp_root_path, study_id
        )
        email = get_settings().email.email_service.configuration.hpc_cluster_job_track_email_address
        current = current_time().strftime("%Y-%m-%d_%H-%M-%S")

        private_ftp_root_path = mounted_paths.cluster_private_ftp_root_path
        study_private_ftp_path = os.path.join(
            private_ftp_root_path, f"{study.acc.lower()}-{study.obfuscationcode}"
        )
        root_data_files_hash_path = os.path.join(
            revisions_root_path, "HASHES", "data_sha256.json"
        )
        data_files_hash_path = os.path.join(
            revisions_root_path,
            "METADATA_REVISIONS",
            f"{study_id}_{latest_revision:02}",
            "HASHES",
            "data_sha256.json",
        )
        Path(root_data_files_hash_path).parent.mkdir(parents=True, exist_ok=True)
        Path(data_files_hash_path).parent.mkdir(parents=True, exist_ok=True)
        job_id, messages = sync_public_ftp_folder_with_revisions(
            study_id=study_id,
            revision_number=study.revision_number,
            source_path=revisions_root_path,
            target_path=public_study_path,
            user_token=user_token,
            email=email,
            task_name=f"{study_id}_{study.revision_number:02}_PUBLIC_FTP_SYNC_{current}",
            study_private_ftp_path=study_private_ftp_path,
            data_files_hash_path=data_files_hash_path,
            root_data_files_hash_path=root_data_files_hash_path,
        )
        return {"job_id": job_id, "messages": messages}
    except Exception as e:
        raise e


def sync_public_ftp_folder_with_revisions(
    task_name: str,
    study_id: str,
    revision_number: int,
    email: str,
    source_path: str,
    target_path: str,
    user_token: str,
    study_private_ftp_path: str,
    data_files_hash_path: str,
    root_data_files_hash_path: str,
):
    settings = get_settings()
    client: HpcClient = get_new_hpc_datamover_client()
    resource = f"{settings.server.service.resources_path}/studies/{study_id}/revisions/{revision_number}"
    update_url = settings.server.service.app_host_url + resource
    messages = []
    study_log_path = os.path.join(
        settings.study.mounted_paths.study_internal_files_root_path,
        study_id,
        settings.study.internal_logs_folder_name,
        "PUBLIC_FTP_SYNC",
    )
    task_log_path = os.path.join(study_log_path, task_name)
    os.makedirs(task_log_path, exist_ok=True)
    out_log_path = os.path.join(task_log_path, f"{task_name}_out.log")
    err_log_path = os.path.join(task_log_path, f"{task_name}_err.log")

    inputs = {
        "EMAIL_TO": email,
        "STUDY_METADATA_PATH": source_path,
        "STUDY_PUBLIC_FTP_PATH": target_path,
        "STUDY_PRIVATE_FTP_PATH": study_private_ftp_path,
        "DATA_FILES_HASH_PATH": data_files_hash_path,
        "ROOT_DATA_FILES_HASH_PATH": root_data_files_hash_path,
        "UPDATE_URL": update_url,
        "USER_TOKEN": user_token,
    }

    hpc_queue_name = settings.hpc_cluster.datamover.default_queue

    script_template = "sync_public_ftp_folder_with_revisions.sh.j2"
    script_path = BashClient.prepare_script_from_template(script_template, **inputs)
    logger.info("sync_public_ftp_folder script is ready.")
    logger.info(Path(script_path).read_text())

    try:
        submission_result = client.submit_hpc_job(
            script_path,
            task_name,
            output_file=out_log_path,
            error_file=err_log_path,
            queue=hpc_queue_name,
            account=email,
            mail_type="END",
            mem="5G",
            cpu=2,
            runtime_limit="24:00:00",
        )
        job_id = submission_result.job_ids[0] if submission_result else None

        messages.append(f"New job was submitted with job id {job_id} for {task_name}")
        return job_id, messages
    except Exception as exc:
        message = f"Exception after job submission. {str(exc)}"
        logger.warning(message)
        messages.append(message)
        return None, messages
    finally:
        if script_path and os.path.exists(script_path):
            os.remove(script_path)


if __name__ == "__main__":
    user_token = get_settings().auth.service_account.api_token
    # user = UserService.get_instance().get_db_user_by_user_token(user_token)

    studies = []
    with DBManager.get_instance().session_maker() as db_session:
        try:
            # db_session.query(StudyRevision).delete()
            # db_session.commit()
            result = db_session.query(
                Study.acc, Study.revision_number, Study.revision_datetime, Study.status, Study.studysize
            ). all()
            if result:
                studies = list(result)
                studies.sort(
                    key=lambda x: int(x["acc"].replace("MTBLS", "").replace("REQ", ""))
                )

        except Exception as e:
            db_session.rollback()
            raise e
    selected_studies = [
        (x["acc"], x["studysize"])
        for x in studies
        if x["revision_number"] == 1
        # if int(x["acc"].replace("MTBLS", "").replace("REQ", "")) >= 10000 
    ]
    selected_studies.sort(key=lambda x: x[1])
    studies = [ x[0] for x in selected_studies ]
    
    # studies = ["MTBLS8"]
    for study_id in studies:
        study: Study = StudyService.get_instance().get_study_by_acc(study_id)
        study_status = StudyStatus(study.status)
        if study_status not in {StudyStatus.PUBLIC}:
            continue

        if study.revision_number > 0:
            revision = StudyRevisionService.get_study_revision(
                study.acc, study.revision_number
            )
            if revision.status in {
                StudyRevisionStatus.FAILED,
                StudyRevisionStatus.INITIATED,
            }:
                result = sync_study_revision(
                    study_id=study_id,
                    user_token=user_token,
                    latest_revision=study.revision_number,
                )
                print(f"{result}")
            else:
                print(
                    f"{study_id} will be skipped. Its revision status: {revision.status.name}"
                )
