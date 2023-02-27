from functools import lru_cache

from celery import Celery
from celery.signals import after_task_publish
from flask import Flask
from flask_mail import Mail

from app.ws.email.email_service import EmailService
from app.ws.settings.utils import get_celery_settings, get_redis_settings, get_system_settings
from app.tasks.utils import ValueMaskUtility

celery_settings = get_celery_settings()

rs = get_redis_settings()
broker_url = (
    f"redis://:{rs.redis_password}@{rs.redis_host}:{rs.redis_port}/{rs.redis_db}"
)
result_backend = broker_url
celery = Celery(
    __name__,
    include=[
        "app.tasks.periodic_tasks.integration_check",
        "app.tasks.common.email",
        "app.tasks.common.elasticsearch",
        "app.tasks.common.ftp_operations",
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
        "app.tasks.*": {"queue": "mtbls-tasks"},
    },
    task_default_queue="mtbls-tasks",
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
celery.conf.beat_schedule = {
    "check_integration": {
        "task": "app.tasks.periodic_tasks.integration_check.check_integrations",
        "schedule": system_settings.integration_test_period_in_seconds,
        "options": {"expires": 55},
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

        subject_name = f"Task {self.name} with {task_id} failed"
        new_kwargs = {}
        if kwargs:

            for key in kwargs:
                new_kwargs[key] = ValueMaskUtility.mask_value(key, kwargs[key])

        kwargs_str = str(new_kwargs) if new_kwargs else ""
        traceback = str(einfo.traceback).replace("\n", "<p>")
        args_str = str(args) if args else ""
        body = f"Task <b>{self.name}</b> with <b>{str(task_id)}</b> failed. <p> {str(exc)}<p>Args: {args_str}<p>kwargs: {kwargs_str}<p>{traceback}"
        report_internal_technical_issue(subject_name, body)
