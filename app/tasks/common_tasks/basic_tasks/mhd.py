import logging
import os

from mhd_model.mhd_client import MhdClient, MhdClientError, SubmittedRevision

from app.config import get_settings
from app.tasks.common_tasks.basic_tasks.send_email import send_technical_issue_email
from app.tasks.worker import MetabolightsTask, celery
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import StudyRevision
from app.ws.db.types import MhdSubmissionStatus

logger = logging.getLogger("wslog")


@celery.task(
    bind=True,
    base=MetabolightsTask,
    default_retry_delay=10,
    max_retries=3,
    soft_time_limit=60 * 15,
    name="app.tasks.common_tasks.basic_tasks.mhd.submit_announcement_file",
)
def submit_announcement_file_task(
    self, study_id: str, revision_number: int, mhd_id: str, announcement_reason: str
):
    mounted_paths = get_settings().hpc_cluster.datamover.mounted_paths

    mhd_files_root_path = os.path.join(
        mounted_paths.cluster_study_internal_files_root_path,
        study_id,
        "DATA_FILES",
    )
    announcement_file_path = os.path.join(
        mhd_files_root_path, f"{mhd_id}.announcement.json"
    )
    if not os.path.exists(announcement_file_path):
        inputs = {
            "subject": "MHD file submission error. Announcement file not found",
            "body": f"{study_id} Announcement file not found: {announcement_file_path}",
        }
        send_technical_issue_email.apply_async(kwargs=inputs)
        return

    with DBManager.get_instance().session_maker() as db_session:
        try:
            query = db_session.query(StudyRevision)
            revision: StudyRevision = query.filter(
                StudyRevision.accession_number == study_id,
                StudyRevision.revision_number == revision_number,
            ).one_or_none()
            if not revision:
                raise Exception(
                    f"Study revision not found for study {study_id} revision {revision_number}"
                )
            if revision.mhd_share_status == MhdSubmissionStatus.COMPLETED.value:
                logger.warning(
                    f"Study {study_id} revision {revision_number} is already submitted to MetabolomicsHub."
                )
                return
            if revision.mhd_share_status == MhdSubmissionStatus.IN_PROGRESS.value:
                logger.warning(
                    f"Study {study_id} revision {revision_number} MetabolomicsHub submission task is in progress."
                )
                return
            revision.mhd_share_status = MhdSubmissionStatus.IN_PROGRESS.value
            db_session.commit()
        except Exception as e:
            logger.error(e)

    api_key = get_settings().mhd.api_key
    mhd_submission_url = get_settings().mhd.mhd_webservice_base_url
    client = MhdClient(mhd_submission_url, api_key)
    success = False
    try:
        revision: SubmittedRevision = client.submit_announcement_file(
            study_id, mhd_id, announcement_file_path, announcement_reason
        )
        success = True if revision else False
    except MhdClientError as e:
        inputs = {
            "subject": "MHD file submission error",
            "body": f"{study_id} submissin error : {e}",
        }
        send_technical_issue_email.apply_async(kwargs=inputs)
        logger.error(e)

    with DBManager.get_instance().session_maker() as db_session:
        try:
            query = db_session.query(StudyRevision)
            revision: StudyRevision = query.filter(
                StudyRevision.accession_number == study_id,
                StudyRevision.revision_number == revision_number,
            ).one_or_none()
            if not revision:
                raise Exception(
                    f"Study revision not found for study {study_id} revision {revision_number}"
                )
            if success:
                revision.mhd_share_status = MhdSubmissionStatus.COMPLETED.value
            else:
                revision.mhd_share_status = MhdSubmissionStatus.FAILED.value
            db_session.commit()
        except Exception as e:
            logger.error(e)
