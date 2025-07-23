import logging
import os.path
from typing import List, Set, Union
from typing import List, Set, Union

from flask_mail import Mail, Message
from jinja2 import Environment, PackageLoader, select_autoescape
from app.config import get_settings
import urllib.parse
from app.config.model.email import EmailSettings


logger = logging.getLogger("wslog")

env = Environment(
    loader=PackageLoader("app.ws.email", "email_templates"),
    autoescape=select_autoescape(["html", "xml"]),
)


class EmailService(object):
    def __init__(
        self,
        settings: Union[None, EmailSettings] = None,
        mail: Union[None, Mail] = None,
    ):
        self.email_settings = settings
        self.mail = mail

    email_service = None

    @classmethod
    def get_instance(cls, app=None, mail=None):
        if app and not cls.email_service:
            settings = get_settings().email
            configs = {
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

    def send_generic_email(
        self,
        subject,
        body,
        from_mail_address,
        to_mail_addresses,
        cc_mail_addresses=None,
        bcc_mail_addresses=None,
        reply_to=None,
        fail_silently: bool = True,
        bcc_mail_addresses=None,
        reply_to=None,
        fail_silently: bool = True,
    ):
        if not from_mail_address:
            from_mail_address = (
                self.email_settings.email_service.configuration.no_reply_email_address
            )
        if not cc_mail_addresses:
            cc_mail_addresses = []
        if not bcc_mail_addresses:
            bcc_mail_addresses = []
        if not bcc_mail_addresses:
            bcc_mail_addresses = []
        if not isinstance(cc_mail_addresses, list):
            cc_mail_addresses = cc_mail_addresses.split(",")
        if not isinstance(bcc_mail_addresses, list):
            bcc_mail_addresses = bcc_mail_addresses.split(",")
        if not isinstance(bcc_mail_addresses, list):
            bcc_mail_addresses = bcc_mail_addresses.split(",")
        if not isinstance(to_mail_addresses, list):
            recipients = to_mail_addresses.split(",")
        else:
            recipients = to_mail_addresses
        if not reply_to:
            reply_to = from_mail_address
        if not reply_to:
            reply_to = from_mail_address
        msg = Message(
            subject=subject,
            sender=from_mail_address,
            recipients=recipients,
            cc=cc_mail_addresses,
            bcc=bcc_mail_addresses,
            bcc=bcc_mail_addresses,
            html=body,
            reply_to=reply_to,
            reply_to=reply_to,
        )
        try:
            self.mail.send(msg)
        except Exception as exc:
            message = f"Sending email failed: subject= {subject} recipients:{str(recipients)} body={str(body)}\nError {str(exc)}"
            logger.error(message)
            if not fail_silently:
                raise exc
            if not fail_silently:
                raise exc

    def send_email(
        self,
        subject,
        body,
        submitters_mail_addresses,
        user_email,
        from_mail_address=None,
        curation_mail_address: Union[str, List[str]] = None,
        curation_mail_address: Union[str, List[str]] = None,
        additional_cc_emails=None,
    ):
        if not from_mail_address:
            from_mail_address = (
                self.email_settings.email_service.configuration.no_reply_email_address
            )


        dev_email = get_settings().email.email_service.configuration.technical_issue_recipient_email_address
        recipients: Set[str] = set()
        if submitters_mail_addresses:
            recipients = recipients.union(submitters_mail_addresses)
            recipients.discard(None)
            recipients.discard("")
        if not recipients:
            logger.error(f"There is no recipient email address for email: '{subject}'")
            return
        default_curation_mail_address = (
            self.email_settings.email_service.configuration.curation_email_address
        )
        system_emails = {dev_email, default_curation_mail_address}
        default_curation_mail_address = (
            self.email_settings.email_service.configuration.curation_email_address
        )
        system_emails = {dev_email, default_curation_mail_address}
        if additional_cc_emails:
            if isinstance(additional_cc_emails, list):
                additional_cc_emails = [
                    x for x in additional_cc_emails if x and x not in recipients
                ]
            else:
                additional_cc_emails = [
                    x
                    for x in str(additional_cc_emails).split(",")
                    if x and x not in recipients and x not in system_emails
                ]

            if isinstance(additional_cc_emails, list):
                additional_cc_emails = [
                    x for x in additional_cc_emails if x and x not in recipients
                ]
            else:
                additional_cc_emails = [
                    x
                    for x in str(additional_cc_emails).split(",")
                    if x and x not in recipients and x not in system_emails
                ]

        additional_cc_emails = additional_cc_emails if additional_cc_emails else None
        bcc = {dev_email}
        if curation_mail_address:
            if isinstance(curation_mail_address, list):
                bcc.add(curation_mail_address)
            else:
                bcc.update(
                    [x.strip() for x in str(curation_mail_address).split(",") if x]
                )

        all_emails = {default_curation_mail_address, dev_email}
        all_emails.update(recipients)
        if additional_cc_emails:
            all_emails.update(additional_cc_emails)
        if bcc:
            all_emails.update(bcc)

        if user_email and user_email not in all_emails:
            bcc.add(user_email)

        msg = Message(
            subject=subject,
            sender=from_mail_address,
            recipients=list(recipients),
            cc=additional_cc_emails,
            bcc=list(bcc) if bcc else None,
            bcc=list(bcc) if bcc else None,
            html=body,
        )
        try:
            self.mail.send(msg)
        except Exception as e:
            message = f"Sending email failed: subject= {subject} recipients:{str(recipients)} body={str(body)}\nError {str(e)}"
            logger.error(message)

    def get_rendered_body(self, template_name: str, content):
        template = env.get_template(template_name)
        body = template.render(content)
        return body

    def send_email_for_new_provisional_study(
        self,
        provisional_id,
        ftp_folder,
        user_email,
        submitters_mail_addresses,
        submitter_fullname,
    ):
        settings = get_settings()
        user_name = settings.ftp_server.private.connection.username
        user_password = settings.ftp_server.private.connection.password
        ftp_server = settings.ftp_server.private.connection.host
        host = get_settings().server.service.ws_app_base_link
        submission_url = os.path.join(host, "editor", "study", provisional_id)
        metabolights_help_email = "metabolights-help@ebi.ac.uk"
        metabolights_website_url = get_settings().server.service.ws_app_base_link
        content = {
            "provisional_id": provisional_id,
            "submitter_fullname": submitter_fullname,
            "submission_url": submission_url,
            "user_name": user_name,
            "user_password": user_password,
            "ftp_server": ftp_server,
            "ftp_folder": ftp_folder,
            "metabolights_website_url": metabolights_website_url,
            "metabolights_help_email": metabolights_help_email,
        }

        body = self.get_rendered_body("new_submission.html", content)
        subject_name = f"MetaboLights Temporary Submission initiated ({provisional_id})"
        self.send_email(subject_name, body, submitters_mail_addresses, user_email)

    def send_email_for_new_accession_number(
        self,
        study_id,
        provisional_id,
        obfuscation_code,
        user_email,
        submitters_mail_addresses,
        submitter_fullname,
        study_title,
        release_date,
        previous_ftp_folder,
        new_ftp_folder,
        additional_cc_emails,
        study_contacts,
    ):
        host = get_settings().server.service.ws_app_base_link
        subject = "Submission Complete and Accessioned"
        if study_id != provisional_id:
            subject += f" - from {provisional_id} to {study_id}"
        reviewer_url = os.path.join(host, f"reviewer{obfuscation_code}")
        metabolights_help_email = "metabolights-help@ebi.ac.uk"
        metabolights_website_url = get_settings().server.service.ws_app_base_link
        settings = get_settings()
        user_name = settings.ftp_server.private.connection.username
        user_password = settings.ftp_server.private.connection.password
        ftp_server = settings.ftp_server.private.connection.host
        content = {
            "provisional_id": provisional_id,
            "mtbls_accession": study_id,
            "submitter_fullname": submitter_fullname,
            "study_title": study_title,
            "release_date": release_date,
            "reviewer_url": reviewer_url,
            "previous_ftp_folder": previous_ftp_folder,
            "new_ftp_folder": new_ftp_folder,
            "metabolights_website_url": metabolights_website_url,
            "metabolights_help_email": metabolights_help_email,
            "user_name": user_name,
            "user_password": user_password,
            "ftp_server": ftp_server,
            "study_contacts": study_contacts,
        }
        body = self.get_rendered_body("new_accession_number.html", content)

        self.send_email(
            subject,
            body,
            submitters_mail_addresses,
            user_email,
            additional_cc_emails=additional_cc_emails,
        )

    def send_email_on_public(
        self,
        study_id,
        release_date,
        user_email,
        submitters_mail_addresses,
        submitter_fullname,
        study_title,
        study_contacts,
        publication_doi,
        publication_pubmed_id,
        additional_cc_emails,
    ):
        subject = f"MetaboLights Study Made Public ({study_id})"
        metabolights_help_email = "metabolights-help@ebi.ac.uk"
        metabolights_website_url = get_settings().server.service.ws_app_base_link
        # mtbls_accession_url = os.path.join(metabolights_website_url, study_id)
        mtbls_accession_url = os.path.join(
            "https://www.ebi.ac.uk/metabolights", study_id
        )
        public_ftp_server = "ftp.ebi.ac.uk"
        download_settings = get_settings().ftp_server.public.configuration
        public_ftp_base_url = download_settings.public_studies_ftp_base_url

        public_ftp_remote_folder = ""
        if public_ftp_server in public_ftp_base_url:
            public_ftp_remote_folder = public_ftp_base_url.split(public_ftp_server)[-1]

        study_ftp_download_url = os.path.join(
            download_settings.public_studies_http_base_url, study_id
        )
        globus_collection_name = "EMBL-EBI Public Data"
        study_path = os.path.join(public_ftp_remote_folder, study_id)
        study_globus_url = (
            f"{download_settings.public_studies_globus_base_url}/{study_id}%2F"
        )
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
            "study_globus_url": study_globus_url,
        }
        body = self.get_rendered_body("status_is_public.html", content)
        curation_mail_address = (
            self.email_settings.email_service.configuration.curation_email_address
        )
        self.send_email(
            subject,
            body,
            submitters_mail_addresses,
            user_email,
            additional_cc_emails=additional_cc_emails,
            curation_mail_address=curation_mail_address,
        )

    def send_email_study_validation_failed(
        self,
        task_name,
        study_id,
        current_status,
        next_status,
        user_email,
        submitters_mail_addresses,
        submitter_fullname,
        study_title,
    ):
        subject = f"{task_name} failed."
        metabolights_help_email = "metabolights-help@ebi.ac.uk"
        # metabolights_website_url = get_settings().server.service.ws_app_base_link
        metabolights_website_url = "https://www.ebi.ac.uk/metabolights"
        mtbls_accession_url = os.path.join(metabolights_website_url, study_id)

        content = {
            "metabolights_website_url": metabolights_website_url,
            "mtbls_accession_url": mtbls_accession_url,
            "mtbls_accession": study_id,
            "submitter_fullname": submitter_fullname,
            "study_title": study_title,
            "current_status": current_status,
            "next_status": next_status,
            "metabolights_help_email": metabolights_help_email,
        }
        body = self.get_rendered_body("validation_failure.html", content)

        self.send_email(
            subject,
            body,
            submitters_mail_addresses,
            user_email,
        )

    def send_email_for_task_completed(self, subject, task_id, task_result, to):
        content = {"task_id": task_id, "task_result": task_result}
        body = self.get_rendered_body(
            "curation_tasks/worker_task_completed.html", content
        )

        self.send_generic_email(
            subject, body, from_mail_address=None, to_mail_addresses=to
        )
