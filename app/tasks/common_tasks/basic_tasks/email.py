import datetime
import os
from app.config import get_settings
from app.config.utils import get_private_ftp_relative_root_path

from app.ws.db_connection import (
    get_email,
    get_release_date_of_study,
    query_study_submitters,
)
from app.ws.settings.utils import get_study_settings
from app.ws.study.user_service import UserService
from app.tasks.worker import (
    MetabolightsTask,
    celery,
    get_email_service,
    get_flask_app,
    report_internal_technical_issue,
    send_email,
)


@celery.task(
    base=MetabolightsTask, name="app.tasks.common_tasks.basic_tasks.email.send_email_for_study_submitted"
)
def send_email_for_study_submitted(user_token, study_id):
    flask_app = get_flask_app()
    with flask_app.app_context():
        user = UserService.get_instance().validate_user_has_submitter_or_super_user_role(user_token)
        email_service = get_email_service(flask_app)
        user_email = user.username
        submitters_email_list = [user_email]
        release_date = get_release_date_of_study(study_id)
        email_service.send_email_for_queued_study_submitted(
            study_id, release_date, user_email, submitters_email_list
        )
        return {
            "study_id": study_id,
            "user_email": user_email,
            "submitters_email_list": submitters_email_list,
        }

@celery.task(
    base=MetabolightsTask, name="app.tasks.common_tasks.basic_tasks.email.send_test_email"
)
def send_test_email(user_token):
    flask_app = get_flask_app()
    with flask_app.app_context():
        user = UserService.get_instance().validate_user_has_curator_role(user_token)
        email_service = get_email_service(flask_app)
        user_email = user.username
        time = datetime.datetime.now().isoformat()
        email_service.send_generic_email("Test Email", f"This email was sent at {time} from metabolights ws", "no-reply@ebi.ac.uk", user_email)
        return {
            "user_email": user_email,
            "time": time
        }

@celery.task(
    base=MetabolightsTask,
    name="app.tasks.common_tasks.basic_tasks.email.send_email_for_private_ftp_folder",
)
def send_email_for_private_ftp_folder(user_token, study_id, folder_name):

    flask_app = get_flask_app()
    with flask_app.app_context():
        
        relative_studies_root_path = get_private_ftp_relative_root_path()
        relative_study_path = os.path.join(
            os.sep, relative_studies_root_path.lstrip(os.sep), folder_name
        )
        user_email = get_email(user_token)
        submitter_emails = query_study_submitters(study_id)
        submitters_email_list = []
        if submitter_emails:
            submitters_email_list = [
                submitter[0] for submitter in submitter_emails if submitter
            ]
        email_service = get_email_service(flask_app)
        email_service.send_email_for_requested_ftp_folder_created(
            study_id, relative_study_path, user_email, submitters_email_list
        )

        return {
            "study_id": study_id,
            "relative_study_path": relative_study_path,
            "user_email": user_email,
            "submitters_email_list": submitters_email_list,
        }


@celery.task(name="app.tasks.common_tasks.basic_tasks.email.send_generic_email")
def send_generic_email(subject, body, from_address, to_addresses, cc_addresses):

    send_email(subject, body, from_address, to_addresses, cc_addresses)

@celery.task(name="app.tasks.common_tasks.basic_tasks.email.send_technical_issue_email")
def send_technical_issue_email(subject, body):
    report_internal_technical_issue(subject, body)