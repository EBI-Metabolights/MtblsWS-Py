from app.tasks.common.email import send_email_for_private_ftp_folder
from app.tasks.worker import MetabolightsTask, celery, get_flask_app
from app.ws.study.commons import create_ftp_folder
from app.ws.study.study_service import StudyService


@celery.task(
    base=MetabolightsTask,
    bind=True,
    name="app.tasks.common.ftp_operations.create_ftp_folder",
    soft_time_limit=110,
    time_limit=120,
    autoretry_for={Exception},
    default_retry_delay=10,
    max_retries=2,
)
def create_private_ftp_folder(self, user_token=None, study_id=None, send_email=False):

    flask_app = get_flask_app()
    with flask_app.app_context():
        study = StudyService.get_instance(flask_app).get_study_by_acc(study_id)
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

                send_email_for_private_ftp_folder(user_token, study_id, upload_location)
        return result
