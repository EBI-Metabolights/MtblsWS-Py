import datetime
import logging

from email_validator import validate_email

from app.tasks.worker import get_email_service, get_flask_app
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import StudyTask
from app.ws.db.types import StudyTaskStatus
from app.ws.study.study_service import StudyService

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


def complete_task(study_id, task_name: str, status: str, message: str) -> StudyTask:
    tasks = StudyService.get_instance().get_study_tasks(
        study_id=study_id, task_name=task_name
    )

    with DBManager.get_instance().session_maker() as db_session:
        if tasks:
            task: StudyTask = tasks[0]
            task.last_execution_message = message
            task.last_execution_status = status
            task.last_request_executed = task.last_request_time
            db_session.add(task)
            db_session.commit()
        else:
            logger.error("%s %s There is no task", study_id, task_name)
            # raise Exception(f"{study_id} {task_name}. There is no task")


def create_task(study_id, task_name: str) -> StudyTask:
    tasks = StudyService.get_instance().get_study_tasks(
        study_id=study_id, task_name=task_name
    )
    with DBManager.get_instance().session_maker() as db_session:
        if tasks:
            task: StudyTask = tasks[0]
        else:
            now = datetime.datetime.now(datetime.UTC)
            task = StudyTask()
            task.study_acc = study_id
            task.task_name = task_name
            task.last_request_time = now
            task.last_execution_time = now
            task.last_request_executed = now
            task.last_execution_status = StudyTaskStatus.NOT_EXECUTED
            task.last_execution_message = "Task is initiated."

        if task.last_execution_status in {
            StudyTaskStatus.NOT_EXECUTED,
            StudyTaskStatus.EXECUTION_FAILED,
        }:
            task.last_execution_status = StudyTaskStatus.EXECUTING
            task.last_execution_time = task.last_request_time
            task.last_request_executed = task.last_execution_time
            task.last_execution_message = "Task is initiated."
            db_session.add(task)
            db_session.commit()
            return task
        else:
            raise Exception(
                f"{study_id} task {task_name} failed. "
                f"Task status is {task.last_execution_status}."
            )
