import logging
import os.path
from typing import Union

from flask_mail import Mail, Message
from jinja2 import Environment, PackageLoader, select_autoescape
from app.config import get_settings
import urllib.parse
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
            message = f"Sending email failed: subject= {subject_name} recipients:{str(recipients)} body={str(body)}\nError {str(exc)}"
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
            message = f"Sending email failed: subject= {subject_name} recipients:{str(recipients)} body={str(body)}\nError {str(e)}"
            logger.error(message)

    def get_rendered_body(self, template_name: str, content):
        template = env.get_template(template_name)
        body = template.render(content)
        return body


    def send_email_for_new_submission(self, submission_id, ftp_folder, user_email, submitters_mail_addresses, submitter_fullname):
        settings = get_settings()
        user_name = settings.ftp_server.private.connection.username
        user_password = settings.ftp_server.private.connection.password
        ftp_server = settings.ftp_server.private.connection.host
        ftp_upload_doc_link = settings.email.template_email_configuration.ftp_upload_help_doc_url
        host = get_settings().server.service.ws_app_base_link
        submision_url = os.path.join(host, "editor", "study", submission_id)
        metabolights_help_email = "metabolights-help@ebi.ac.uk"
        metabolights_website_url = get_settings().server.service.ws_app_base_link
        content = {
            "submission_id": submission_id,
            "submitter_fullname": submitter_fullname,
            "submision_url": submision_url,
            "user_name": user_name,
            "user_password": user_password,
            "ftp_server": ftp_server,
            "ftp_folder": ftp_folder,
            "metabolights_website_url": metabolights_website_url,
            "metabolights_help_email": metabolights_help_email
        }

        body = self.get_rendered_body("new_submission.html", content)
        subject_name = f"MetaboLights Temporary Submission initiated ({submission_id})"

        self.send_email(subject_name, body, submitters_mail_addresses, user_email)
        
    def send_email_for_new_accession_number(self, study_id, submission_id, obfuscation_code, user_email, submitters_mail_addresses, submitter_fullname, study_title, release_date, previous_ftp_folder, new_ftp_folder):
        host = get_settings().server.service.ws_app_base_link
        subject_name = f"Submission Complete and Accessioned"
        if study_id != submission_id:
            subject_name += f" - from {submission_id} to {study_id}"
        reviewer_url = os.path.join(host, f"reviewer{obfuscation_code}")
        metabolights_help_email = "metabolights-help@ebi.ac.uk"
        metabolights_website_url = get_settings().server.service.ws_app_base_link
        content = {
            "submission_id": submission_id,
            "mtbls_accession": study_id,
            "submitter_fullname": submitter_fullname,
            "study_title": study_title,
            "release_date": release_date,
            "reviewer_url": reviewer_url,
            "previous_ftp_folder": previous_ftp_folder,
            "new_ftp_folder": new_ftp_folder,
            "metabolights_website_url": metabolights_website_url,
            "metabolights_help_email": metabolights_help_email
        }
        body = self.get_rendered_body("new_accession_number.html", content)
        self.send_email(subject_name, body, submitters_mail_addresses, user_email)

    def send_email_on_public(self, study_id, release_date, user_email, submitters_mail_addresses, 
                             submitter_fullname, study_title, study_contacts, publication_doi, publication_pubmed_id):
        subject_name = f"MetaboLights Study ({study_id}) Made Public"
        metabolights_help_email = "metabolights-help@ebi.ac.uk"
        metabolights_website_url = get_settings().server.service.ws_app_base_link
        mtbls_accession_url = os.path.join(metabolights_website_url, study_id)
        public_ftp_server = "ftp.ebi.ac.uk"
        public_ftp_remote_folder = "/pub/databases/metabolights/studies/public"
        public_ftp_base_url = f"http://{public_ftp_server}{public_ftp_remote_folder}"
        study_ftp_download_url = os.path.join(public_ftp_base_url, study_id)
        globus_collection_name = "EMBL-EBI Public Data"
        study_path = os.path.join(public_ftp_remote_folder, study_id)
        origin_path = urllib.parse.quote(study_path)
        origin_id = "47772002-3e5b-4fd3-b97c-18cee38d6df2"
        study_globus_url = f"https://app.globus.org/file-manager?origin_id={origin_id}&origin_path={origin_path}%2F"
        content = {
            "mtbls_accession": study_id,
            "submitter_fullname": submitter_fullname,
            "study_title": study_title,
            "release_date": release_date,
            "study_contacts": study_contacts,
            "mtbls_accession_url": mtbls_accession_url,
            "study_ftp_download_url": study_ftp_download_url,
            "public_ftp_server": public_ftp_server,
            "public_ftp_remote_folder": study_path,
            "metabolights_website_url": metabolights_website_url,
            "metabolights_help_email": metabolights_help_email,
            "publication_doi": publication_doi,
            "publication_pubmed_id": publication_pubmed_id,
            "globus_collection_name": globus_collection_name,
            "study_globus_url": study_globus_url
        }
        body = self.get_rendered_body("status_is_public.html", content)
        self.send_email(subject_name, body, submitters_mail_addresses, user_email)


    def send_email_for_task_completed(self, subject_name, task_id, task_result, to):
        content = {"task_id": task_id, "task_result": task_result}
        body = self.get_rendered_body("curation_tasks/worker_task_completed.html", content)

        self.send_generic_email(subject_name, body, from_mail_address=None, to_mail_addresses=to)
