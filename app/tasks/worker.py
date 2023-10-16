import os
from functools import lru_cache

from celery import Celery
from celery.signals import after_task_publish
from flask import Flask
from flask_mail import Mail
from app.config import get_settings
from app.config.model.celery import CelerySettings
from app.config.model import redis_cache

from app.utils import MetabolightsException, ValueMaskUtility
from app.ws.email.email_service import EmailService

from app.ws.study.user_service import UserService

settings: CelerySettings = get_settings().celery

rs: redis_cache.RedisConnection = settings.broker
# broker_url = f'redis+sentinel://:{rs.redis_password}@{rs.redis_host}:{rs.redis_port}/{rs.redis_db}'
broker_url = None
broker_transport_options = None
result_backend_transport_options = None
if rs.connection_type == "redis":
    rc = rs.redis_connection
    broker_url = (
        f"redis://:{rs.redis_password}@{rc.redis_host}:{rc.redis_port}/{rs.redis_db}"
    )
    result_backend = broker_url
else:
    sc = rs.sentinel_connection
    broker_url = ";".join(
        [
            f"sentinel://:{rs.redis_password}@{host.name}:{host.port}/{rs.redis_db}"
            for host in sc.hosts
        ]
    )
    broker_transport_options = {
        "master_name": sc.master_name,
        "sentinel_kwargs": {"password": rs.redis_password},
    }
    result_backend_transport_options = broker_transport_options
    result_backend = broker_url


common_tasks = [
    "app.tasks.common_tasks.admin_tasks.es_and_db_compound_synchronization",
    "app.tasks.common_tasks.admin_tasks.es_and_db_study_synchronization",
    "app.tasks.common_tasks.curation_tasks.metabolon",
    "app.tasks.common_tasks.curation_tasks.validation",
    "app.tasks.common_tasks.basic_tasks.email",
    "app.tasks.common_tasks.basic_tasks.elasticsearch",
]
datamover_tasks = [
    "app.tasks.datamover_tasks.basic_tasks.study_folder_maintenance",
    "app.tasks.datamover_tasks.basic_tasks.file_management",
    "app.tasks.datamover_tasks.curation_tasks.data_file_operations",
    "app.tasks.datamover_tasks.basic_tasks.execute_commands",
]


admin_tasks = [
    "app.tasks.common_tasks.admin_tasks.es_and_db_compound_synchronization",
    "app.tasks.common_tasks.admin_tasks.es_and_db_study_synchronization",
    "app.tasks.system_monitor_tasks.heartbeat",
    "app.tasks.system_monitor_tasks.worker_maintenance",
    "app.tasks.system_monitor_tasks.integration_check",
]


compute_tasks = []

celery = Celery(
    __name__,
    include=[
        "app.tasks.common_tasks.admin_tasks.es_and_db_compound_synchronization",
        "app.tasks.common_tasks.admin_tasks.es_and_db_study_synchronization",
        "app.tasks.common_tasks.curation_tasks.metabolon",
        "app.tasks.common_tasks.curation_tasks.validation",
        "app.tasks.common_tasks.basic_tasks.email",
        "app.tasks.common_tasks.basic_tasks.elasticsearch",
        "app.tasks.datamover_tasks.basic_tasks.study_folder_maintenance",
        "app.tasks.datamover_tasks.basic_tasks.file_management",
        "app.tasks.datamover_tasks.curation_tasks.data_file_operations",
        "app.tasks.datamover_tasks.basic_tasks.execute_commands",
        "app.tasks.system_monitor_tasks.heartbeat",
        "app.tasks.system_monitor_tasks.worker_maintenance",
        "app.tasks.system_monitor_tasks.integration_check",
    ],
)


@lru_cache(1)
def get_flask_app():
    flask_app = Flask(__name__)
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
        "app.tasks.system_monitor_tasks.heartbeat.*": {
            "queue": "datamover-tasks",
            "router_key": "heartbeat",
        },
        "app.tasks.datamover_tasks.*": {"queue": "datamover-tasks"},
        "app.tasks.system_monitor_tasks.*": {"queue": "monitor-tasks"},
    },
    task_default_queue="common-tasks",
    broker_connection_retry_on_startup=True,
    broker_url=broker_url,
    result_backend=result_backend,
    task_acks_late=True,
    task_acks_on_failure_or_timeout=False,
    task_reject_on_worker_lost=True,
    task_track_started=True,
    result_expires=settings.configuration.celery_result_expires,
)

if broker_transport_options:
    celery.conf.update(broker_transport_options=broker_transport_options)
if result_backend_transport_options:
    celery.conf.update(
        result_backend_transport_options=result_backend_transport_options
    )


@after_task_publish.connect
def update_task_was_sent_state(sender=None, headers=None, **kwargs):
    task = celery.tasks.get(sender)
    backend = task.backend if task else celery.backend
    backend.store_result(headers["id"], None, "INITIATED")


service_account_apitoken = get_settings().auth.service_account.api_token
periodic_task_configuration = get_settings().celery.periodic_task_configuration
# celery.conf.beat_schedule = {
#     "check_integration": {
#         "task": "app.tasks.common_tasks.admin_tasks.integration_check.check_integrations",
#         "schedule": periodic_task_configuration.integration_test_period_in_seconds,
#         "options": {"expires": 55},
#     },
#     "sync_compound_on_es_and_db": {
#         "task": "app.tasks.common_tasks.admin_tasks.es_and_db_compound_synchronization.sync_compounds_on_es_and_db",
#         "schedule": periodic_task_configuration.es_compound_sync_task_period_in_secs ,
#         "args": (service_account_apitoken,),
#         "options": {"expires": 60 },
#     },
#         "sync_study_on_es_and_db": {
#         "task": "app.tasks.common_tasks.admin_tasks.es_and_db_study_synchronization.sync_studies_on_es_and_db",
#         "schedule": periodic_task_configuration.es_study_sync_task_period_in_secs ,
#         "args": (service_account_apitoken,),
#         "options": {"expires": 60 },
#     }
# }

celery.conf.beat_schedule = {
    "check_integration": {
        "task": "app.tasks.common_tasks.admin_tasks.integration_check.check_integrations",
        "schedule": periodic_task_configuration.integration_test_period_in_seconds*3,
        "options": {"expires": 55},
    },
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
    email = (
        get_settings().email.email_service.configuration.technical_issue_recipient_email_address
    )
    send_email(subject, body, None, email, None)


class MetabolightsTask(celery.Task):
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        flask_app = get_flask_app()
        with flask_app.app_context():
            subject_name = f"Task {self.name} with {task_id} failed"
            username = ""
            if "email" in kwargs:
                username = kwargs["email"]
            if not username and "user_token" in kwargs and kwargs["user_token"]:
                user = UserService.get_instance().get_simplified_user_by_token(
                    kwargs["user_token"]
                )
                if user:
                    username = user.userName

            new_kwargs = {}
            if kwargs:
                for key in kwargs:
                    new_kwargs[key] = ValueMaskUtility.mask_value(key, kwargs[key])

            kwargs_str = str(new_kwargs) if new_kwargs else ""
            send: bool = False
            if isinstance(einfo.exception, MetabolightsException):
                exc: MetabolightsException = einfo.exception
                if exc.http_code >= 500:
                    send = True
            if send:
                traceback = str(einfo.traceback).replace("\n", "<p>")
                args_str = str(args) if args else ""
                body = f"Task <b>{self.name}</b> with <b>{str(task_id)}</b> failed. <p>Submitter: {username} <p> Executed on: {os.uname().nodename} <p>  {str(exc)}<p>Args: {args_str}<p>kwargs: {kwargs_str}<p>{traceback}"
                report_internal_technical_issue(subject_name, body)
