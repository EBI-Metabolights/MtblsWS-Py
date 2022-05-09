import os.path

from flask import current_app as app
from flask_mail import Mail, Message
from jinja2 import Environment, PackageLoader, select_autoescape

from app.ws.email.settings import EmailServiceSettings

env = Environment(
    loader=PackageLoader("app.ws.email", 'email_templates'),
    autoescape=select_autoescape(['html', 'xml'])
)


class EmailService(object):

    def __init__(self, application=None, settings: EmailServiceSettings = None):
        self.app = application
        self.settings = settings
        self.mail = Mail(self.app)

    def send_email(self, subject_name, body, submitters_mail_addresses, user_email,
                   from_mail_address=None, curation_mail_address=None):
        if not from_mail_address:
            from_mail_address = self.settings.email_no_reply_address
        if not curation_mail_address:
            curation_mail_address = self.settings.curation_mail_address
        recipients = set()
        recipients.add(user_email)
        recipients = recipients.union(submitters_mail_addresses)
        msg = Message(subject=subject_name,
                      sender=from_mail_address,
                      recipients=recipients,
                      cc=[curation_mail_address],
                      body=body)
        self.mail.send(msg)

    def send_email_for_queued_study_submitted(self, study_id, release_date, user_email, submitters_mail_addresses):

        template = env.get_template('send_queued_study_submitted.html')

        file_name = " * Online submission * "
        study_url = os.path.join(self.settings.metabolights_host_url, study_id)
        subject_name = f"Congratulations! Your study {study_id} has been successfully processed!"

        content = {"study_id": study_id, "release_date": release_date,
                   "file_name": file_name, "study_url": study_url}
        body = template.render(content)
        self.send_email(subject_name, body, submitters_mail_addresses, user_email)

    def send_email_for_requested_ftp_folder_created(self, study_id, ftp_folder, user_email, submitters_mail_addresses):

        template = env.get_template('requested_ftp_folder_created.html')

        user_name = self.settings.private_ftp_server_user
        user_password = self.settings.private_ftp_server_password
        server = self.settings.private_ftp_server
        ftp_upload_doc_link = self.settings.ftp_upload_help_doc

        content = {"user_name": user_name, "user_password": user_password, "server": server,
                   "ftp_folder": ftp_folder, "ftp_upload_doc_link": ftp_upload_doc_link}
        body = template.render(content)

        subject_name = f"Requested Study upload folder for {study_id}"

        self.send_email(subject_name, body, submitters_mail_addresses, user_email)


def get_email_service(application=None):
    if not application:
        application = app
    return EmailService(application)
