import datetime
import os
from app.config import get_settings
from app.config.utils import get_private_ftp_relative_root_path
from app.utils import current_time

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
    base=MetabolightsTask, name="app.tasks.common_tasks.basic_tasks.email.send_test_email"
)
def send_test_email(user_token):
    flask_app = get_flask_app()
    with flask_app.app_context():
        user = UserService.get_instance().validate_user_has_curator_role(user_token)
        email_service = get_email_service(flask_app)
        user_email = user.username
        time = current_time().isoformat()
        email_service.send_generic_email("Test Email", f"This email was sent at {time} from metabolights ws", "no-reply@ebi.ac.uk", user_email)
        return {
            "user_email": user_email,
            "time": time
        }

@celery.task(
    base=MetabolightsTask,
    name="app.tasks.common_tasks.basic_tasks.email.send_email_for_new_submission",
)
def send_email_for_new_submission(user_token, study_id, folder_name):

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
        email_service.send_email_for_new_submission(
            study_id, relative_study_path, user_email, submitters_email_list
        )

        return {
            "study_id": study_id,
            "relative_study_path": relative_study_path,
            "user_email": user_email,
            "submitters_email_list": submitters_email_list,
        }


@celery.task(
    base=MetabolightsTask,
    name="app.tasks.common_tasks.basic_tasks.email.send_email_for_new_accession_number",
)
def send_email_for_new_accession_number(user_token, study_id, submission_id, obfuscation_code):

    flask_app = get_flask_app()
    with flask_app.app_context():   
        user_email = get_email(user_token)
        submitter_emails = query_study_submitters(study_id)
        submitters_email_list = []
        if submitter_emails:
            submitters_email_list = [
                submitter[0] for submitter in submitter_emails if submitter
            ]
        email_service = get_email_service(flask_app)
        email_service.send_email_for_new_accession_number(
            study_id, submission_id, obfuscation_code,  user_email, submitters_email_list
        )

        return {
            "submission_id": submission_id,
            "study_id": study_id,
            "obfuscation_code": obfuscation_code,
            "user_email": user_email,
            "submitters_email_list": submitters_email_list,
        }

@celery.task(
    base=MetabolightsTask,
    name="app.tasks.common_tasks.basic_tasks.email.send_email_on_public",
)
def send_email_on_public(user_token, study_id, release_date):
    flask_app = get_flask_app()
    with flask_app.app_context():   
        user_email = get_email(user_token)
        submitter_emails = query_study_submitters(study_id)
        submitters_email_list = []
        if submitter_emails:
            submitters_email_list = [
                submitter[0] for submitter in submitter_emails if submitter
            ]
        email_service = get_email_service(flask_app)
        email_service.send_email_on_public(
            study_id, release_date,  user_email, submitters_email_list
        )

        return {
            "study_id": study_id,
            "release_date": release_date,
            "user_email": user_email,
            "submitters_email_list": submitters_email_list,
        }
        

@celery.task(name="app.tasks.common_tasks.basic_tasks.email.send_generic_email")
def send_generic_email(subject, body, from_address, to_addresses, cc_addresses):

    send_email(subject, body, from_address, to_addresses, cc_addresses)

@celery.task(name="app.tasks.common_tasks.basic_tasks.email.send_technical_issue_email")
def send_technical_issue_email(subject, body):
    report_internal_technical_issue(subject, body)