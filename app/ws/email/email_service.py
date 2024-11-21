import logging
import os.path
from typing import Union

from flask_mail import Mail, Message
from jinja2 import Environment, PackageLoader, select_autoescape
from app.config import get_settings

from app.config.model.email import EmailSettings


logger = logging.getLogger("wslog")

env = Environment(
    loader=PackageLoader("app.ws.email", "email_templates"), autoescape=select_autoescape(["html", "xml"])
)


class EmailService(object):
    def __init__(self, settings: Union[None, EmailSettings] = None, mail: Union[None, Mail] = None):
        self.email_settings = settings
        self.mail = mail

    email_service = None

    @classmethod
    def get_instance(cls, app=None, mail=None):
        if app and not cls.email_service:
            settings = get_settings().email
            configs =  {
                "MAIL_SERVER": settings.email_service.connection.host,
                "MAIL_PORT": settings.email_service.connection.port,
                "MAIL_USERNAME": settings.email_service.connection.username,
                "MAIL_PASSWORD": settings.email_service.connection.password,
                "MAIL_USE_TLS": settings.email_service.connection.use_tls,
                "MAIL_USE_SSL": settings.email_service.connection.use_ssl,
            }
            for item in configs:
                if configs[item]:
                    app.config[item] = configs[item]

            mail = Mail(app)
            cls.email_service = EmailService(settings, mail)
        return cls.email_service

    def send_generic_email(self, subject_name, body, from_mail_address, to_mail_addresses, cc_mail_addresses=None):
        if not from_mail_address:
            from_mail_address = self.email_settings.email_service.configuration.no_reply_email_address
        if not cc_mail_addresses:
            cc_mail_addresses = []
        if not isinstance(cc_mail_addresses, list):
            cc_mail_addresses = cc_mail_addresses.split(",")
        if not isinstance(to_mail_addresses, list):
            recipients = to_mail_addresses.split(",")
        else:
            recipients = to_mail_addresses
        msg = Message(
            subject=subject_name, sender=from_mail_address, recipients=recipients, cc=cc_mail_addresses, html=body
        )
        try:
            self.mail.send(msg)
        except Exception as exc:
            message = f"Sending email failed: subject= {subject_name} receipents:{str(recipients)} body={str(body)}\nError {str(exc)}"
            logger.error(message)

    def send_email(
        self,
        subject_name,
        body,
        submitters_mail_addresses,
        user_email,
        from_mail_address=None,
        curation_mail_address=None,
    ):
        if not from_mail_address:
            from_mail_address = self.email_settings.email_service.configuration.no_reply_email_address
        if not curation_mail_address:
            curation_mail_address = self.email_settings.email_service.configuration.curation_email_address
        dev_email = get_settings().email.email_service.configuration.technical_issue_recipient_email_address
        recipients = set()
        recipients.add(user_email)
        recipients = list(recipients.union(submitters_mail_addresses))
        msg = Message(
            subject=subject_name, sender=from_mail_address, recipients=recipients, cc=[dev_email], html=body
        )
        try:
            self.mail.send(msg)
        except Exception as e:
            message = f"Sending email failed: subject= {subject_name} receipents:{str(recipients)} body={str(body)}\nError {str(e)}"
            logger.error(message)

    def get_rendered_body(self, template_name: str, content):
        template = env.get_template(template_name)
        body = template.render(content)
        return body

    def send_email_for_queued_study_submitted(self, study_id, release_date, user_email, submitters_mail_addresses):
        file_name = " * Online submission * "
        host = get_settings().server.service.ws_app_base_link
        public_study_url = os.path.join(host, study_id)
        private_study_url = os.path.join(host, "editor", "study", study_id)
        subject_name = f"{study_id}: Your MetaboLights study request has been successfully processed!"

        content = {
            "study_id": study_id,
            "release_date": release_date,
            "file_name": file_name,
            "public_study_url": public_study_url,
            "private_study_url": private_study_url,
        }
        body = self.get_rendered_body("send_queued_study_submitted.html", content)
        self.send_email(subject_name, body, submitters_mail_addresses, user_email)

    def send_email_for_requested_ftp_folder_created(self, study_id, ftp_folder, user_email, submitters_mail_addresses):
        settings = get_settings()
        user_name = settings.ftp_server.private.connection.username
        user_password = settings.ftp_server.private.connection.password
        server = settings.ftp_server.private.connection.host
        ftp_upload_doc_link = settings.email.template_email_configuration.ftp_upload_help_doc_url

        content = {
            "user_name": user_name,
            "user_password": user_password,
            "server": server,
            "ftp_folder": ftp_folder,
            "ftp_upload_doc_link": ftp_upload_doc_link,
        }

        body = self.get_rendered_body("requested_ftp_folder_created.html", content)
        subject_name = f"{study_id}:  MetaboLights private FTP upload folder for your submission"

        self.send_email(subject_name, body, submitters_mail_addresses, user_email)

    def send_email_for_task_completed(self, subject_name, task_id, task_result, to):
        content = {"task_id": task_id, "task_result": task_result}
        body = self.get_rendered_body("curation_tasks/worker_task_completed.html", content)

        self.send_generic_email(subject_name, body, from_mail_address=None, to_mail_addresses=to)
