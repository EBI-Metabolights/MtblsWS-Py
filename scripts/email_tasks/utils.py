import datetime
import logging

from app.tasks.worker import get_email_service, get_flask_app
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import StudyTask
from app.ws.db.types import StudyTaskStatus
from app.ws.study.study_service import StudyService
from email_validator import validate_email

from app.ws.tasks.db_track import complete_task, create_task

logger = logging.getLogger(__name__)

flask_app = get_flask_app()
email_service = get_email_service(flask_app=flask_app)


def send_task_email(
    study_id: str,
    task_name: str,
    subject_name: str,
    body: str,
    from_mail_address,
    to_mail_addresses,
    cc_mail_addresses=None,
    bcc_mail_addresses=None,
    reply_to=None,
) -> bool:
    task = None
    try:
        task = create_task(study_id=study_id, task_name=task_name)
    except Exception as ex:
        logger.error(str(ex))
    if not task:
        return False
    with flask_app.app_context():
        try:
            if not from_mail_address:
                raise Exception("From email address is empty")
            if not to_mail_addresses:
                raise Exception("To email address is empty")
            if not cc_mail_addresses:
                cc_mail_addresses = []
            if not bcc_mail_addresses:
                bcc_mail_addresses = []
            if not isinstance(cc_mail_addresses, list):
                cc_mail_addresses = cc_mail_addresses.split(",")
            if not isinstance(to_mail_addresses, list):
                to_mail_addresses = to_mail_addresses.split(",")
            if not isinstance(bcc_mail_addresses, list):
                bcc_mail_addresses = bcc_mail_addresses.split(",")
            for emails in (to_mail_addresses, cc_mail_addresses):
                for email in emails:
                    validate_email(email)

            email_service.send_generic_email(
                subject_name=subject_name,
                body=body,
                from_mail_address=from_mail_address,
                to_mail_addresses=to_mail_addresses,
                cc_mail_addresses=cc_mail_addresses if cc_mail_addresses else None,
                bcc_mail_addresses=bcc_mail_addresses if bcc_mail_addresses else None,
                reply_to=reply_to,
                fail_silently=False,
            )
            complete_task(
                study_id,
                task_name=task_name,
                status=StudyTaskStatus.EXECUTION_SUCCESSFUL,
                message="Email is sent.",
            )
            return True
        except Exception as ex:
            complete_task(
                study_id,
                task_name=task_name,
                status=StudyTaskStatus.EXECUTION_FAILED,
                message=f"Email is not sent. {str(ex)}",
            )
            return False

