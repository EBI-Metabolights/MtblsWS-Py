import os
from functools import lru_cache

from celery import Celery
from celery.signals import after_task_publish
from flask import Flask
from flask_mail import Mail

from app.tasks.utils import ValueMaskUtility
from app.ws.email.email_service import EmailService
from app.ws.settings.utils import (get_celery_settings, get_redis_settings,
                                   get_system_settings)
from app.ws.study.user_service import UserService

celery_settings = get_celery_settings()

rs = get_redis_settings()
broker_url = (
    f"redis://:{rs.redis_password}@{rs.redis_host}:{rs.redis_port}/{rs.redis_db}"
)
result_backend = broker_url
celery = Celery(
    __name__,
    include=[
        "app.tasks.common_tasks.admin_tasks.es_and_db_compound_syncronization",
        "app.tasks.common_tasks.admin_tasks.es_and_db_study_syncronization",
        "app.tasks.common_tasks.curation_tasks.metabolon",
        "app.tasks.common_tasks.basic_tasks.email",
        "app.tasks.common_tasks.basic_tasks.elasticsearch",
        "app.tasks.common_tasks.basic_tasks.ftp_operations",
        "app.tasks.datamover_tasks.basic_tasks.study_folder_maintenance",
        "app.tasks.datamover_tasks.basic_tasks.file_management",
        "app.tasks.system_monitor_tasks.worker_maintenance",
        "app.tasks.system_monitor_tasks.integration_check",
    ],
)


@lru_cache(1)
def get_flask_app():
    flask_app = Flask(__name__)
    flask_app.config.from_object("config")

    return flask_app


@lru_cache(1)
def get_flask_mail(flask_app=None):
    if not flask_app:
        flask_app = get_flask_app()
    with flask_app.app_context():
        return Mail(flask_app)


@lru_cache(1)
def get_email_service(flask_app=None):
    if not flask_app:
        flask_app = get_flask_app()
    with flask_app.app_context():
        mail = get_flask_mail(flask_app)
        email_service = EmailService.get_instance(flask_app, mail=mail)
        return email_service


celery.conf.update(
    task_routes={
        "app.tasks.common_tasks.*": {"queue": "common-tasks"},
        "app.tasks.compute_tasks.*": {"queue": "compute-tasks"},
        "app.tasks.datamover_tasks.*": {"queue": "datamover-tasks"},
        "app.tasks.system_monitor_tasks.*": {"queue": "monitor-tasks"},
    },
    task_default_queue="common-tasks",
    broker_url=broker_url,
    result_backend=result_backend,
    task_acks_late=celery_settings.celery_task_acks_late,
    task_acks_on_failure_or_timeout=celery_settings.celery_task_acks_on_failure_or_timeout,
    task_reject_on_worker_lost=celery_settings.celery_task_reject_on_worker_lost,
    task_track_started=celery_settings.celery_task_track_started,
    result_expires=celery_settings.celery_result_expires,
)


@after_task_publish.connect
def update_task_was_sent_state(sender=None, headers=None, **kwargs):
    task = celery.tasks.get(sender)
    backend = task.backend if task else celery.backend
    backend.store_result(headers["id"], None, "INITIATED")

system_settings = get_system_settings(None)
# celery.conf.beat_schedule = {
#     "check_integration": {
#         "task": "app.tasks.common_tasks.admin_tasks.integration_check.check_integrations",
#         "schedule": system_settings.integration_test_period_in_seconds,
#         "options": {"expires": 55},
#     },
#     "sync_compound_on_es_and_db": {
#         "task": "app.tasks.common_tasks.admin_tasks.es_and_db_compound_syncronization.sync_compounds_on_es_and_db",
#         "schedule": system_settings.es_compound_sync_task_period_in_secs ,
#         "args": (system_settings.metabolights_apitoken,),
#         "options": {"expires": 60 },
#     },
#         "sync_study_on_es_and_db": {
#         "task": "app.tasks.common_tasks.admin_tasks.es_and_db_study_syncronization.sync_studies_on_es_and_db",
#         "schedule": system_settings.es_study_sync_task_period_in_secs ,
#         "args": (system_settings.metabolights_apitoken,),
#         "options": {"expires": 60 },
#     }
# }
celery.conf.beat_schedule = {

    "check_workers": {
        "task": "app.tasks.system_monitor_tasks.worker_maintenance.check_all_workers",
        "schedule": system_settings.worker_heath_check_period_in_seconds,
        "options": {"expires": system_settings.worker_heath_check_period_in_seconds - 3}
    }
}
celery.conf.timezone = "UTC"


def send_email(subject_name, body, from_address, to_addresses, cc_addresses):

    flask_app = get_flask_app()
    with flask_app.app_context():
        email_service = get_email_service(flask_app)
        email_service.send_generic_email(
            subject_name,
            body,
            from_address,
            to_addresses,
            cc_mail_addresses=cc_addresses,
        )

def report_internal_technical_issue(subject, body):
    settings = get_system_settings(None)
    send_email(subject, body, None, settings.technical_issue_recipient_email, None)
    
class MetabolightsTask(celery.Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        flask_app = get_flask_app()
        with flask_app.app_context():
            subject_name = f"Task {self.name} with {task_id} failed"
            username = ""
            if "email" in kwargs:
                username = kwargs["email"]
            if not username and "user_token" in kwargs and kwargs["user_token"]:
                user = UserService.get_instance(flask_app).get_simplified_user_by_token(kwargs["user_token"])
                if user:
                    username = user.userName
                
            new_kwargs = {}
            if kwargs:

                for key in kwargs:
                    new_kwargs[key] = ValueMaskUtility.mask_value(key, kwargs[key])

            kwargs_str = str(new_kwargs) if new_kwargs else ""
            traceback = str(einfo.traceback).replace("\n", "<p>")
            args_str = str(args) if args else ""
            body = f"Task <b>{self.name}</b> with <b>{str(task_id)}</b> failed. <p>Submitter: {username} <p> Executed on: {os.uname().nodename} <p>  {str(exc)}<p>Args: {args_str}<p>kwargs: {kwargs_str}<p>{traceback}"
            report_internal_technical_issue(subject_name, body)
