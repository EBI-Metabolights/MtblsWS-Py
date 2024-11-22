from app.tasks.common_tasks.basic_tasks.email import send_email_for_new_submission
from app.tasks.worker import MetabolightsTask, celery, get_flask_app
from app.ws.db.schemes import Study
from app.ws.study.commons import create_ftp_folder
from app.ws.study.study_service import StudyService


@celery.task(
    base=MetabolightsTask,
    bind=True,
    name="app.tasks.common_tasks.basic_tasks.ftp_operations.create_ftp_folder",
    soft_time_limit=110,
    time_limit=120,
    autoretry_for={Exception},
    default_retry_delay=10,
    max_retries=2,
)
def create_private_ftp_folder(self, user_token=None, study_id=None, send_email=False):
    study: Study = StudyService.get_instance().get_study_by_acc(study_id)
    result = create_ftp_folder(
        study.acc,
        study.obfuscationcode,
        user_token,
        email_service=None,
        send_email=False,
    )

    if send_email:
        if result and "upload_location" in result and result["upload_location"]:
            upload_location = result["upload_location"]

            send_email_for_new_submission(user_token, study_id, upload_location)
    return result
