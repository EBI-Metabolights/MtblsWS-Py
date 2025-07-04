import datetime
import logging
import pathlib
from jinja2 import Environment, PackageLoader, select_autoescape

from scripts.email_tasks.models import MetaboLightsStudyReport, load_study_report
from scripts.email_tasks.utils import send_task_email


logger = logging.getLogger(__name__)


if __name__ == "__main__":
    task_name = "SEND_METABOLIGHTS_UPDATES_EMAIL"
    env = Environment(
        loader=PackageLoader("scripts.email_tasks", "templates"),
        autoescape=select_autoescape(["html", "xml"]),
    )

    report: MetaboLightsStudyReport = load_study_report(
        study_report_path="study_report.json"
    )
    exclue_submitter_emails = set()
    exclue_submitter_emails.update(
        [
            "placeholder@ebi.ac.uk",
            "metabolights-help@ebi.ac.uk",
        ]
    )
    #max_created_at = datetime.datetime.fromisoformat("2025-06-13 09:00:00.00000")
    #min_created_at = datetime.datetime.fromisoformat("2025-01-01 00:00:00.00000")
    # max_created_at = datetime.datetime.fromisoformat("2025-01-01 00:00:00.00000")
    # min_created_at = datetime.datetime.fromisoformat("2024-01-01 00:00:00.00000")
    # max_created_at = datetime.datetime.fromisoformat("2024-01-01 00:00:00.00000")
    # min_created_at = datetime.datetime.fromisoformat("2022-01-01 00:00:00.00000")
    max_created_at = datetime.datetime.fromisoformat("2022-01-01 00:00:00.00000")
    min_created_at = None
    studies = report.filter_study_report(
        status="provisional",
        exclude_submitter_emails=exclue_submitter_emails,
        min_created_at=min_created_at,
        max_created_at=max_created_at,
    )

    script_template_name = "submitted_studies.html"
    template = env.get_template(script_template_name)

    for idx, study in enumerate(studies, start=1):
        try:
            subject_name = f"MetaboLights {study.study_id} New Status Labels & Submitter Led Data Publication"
            if len(study.submitters) == 1:
                submitter_fullname = ", ".join([x.full_name for x in study.submitters])
            else:
                submitter_fullname = ", ".join(
                    [
                        x.full_name
                        for idx, x in enumerate(study.submitters)
                        if idx < len(study.submitters)
                    ]
                )
                submitter_fullname += " and " + study.submitters[-1].full_name

            inputs = {
                "submitter_fullname": submitter_fullname,
                "metabolights_website_url": "https://www.ebi.ac.uk/metabolights",
                "study_id": study.study_id,
                "title": study.title,
                "previous_status": "SUBMITTED",
                "current_status": study.status.upper(),
                "metabolights_help_email": "metabolights-help@ebi.ac.uk",
                ## more keys in template
            }
            body = template.render(inputs)
            from_mail_address = "metabolights-no-reply@ebi.ac.uk"
            to_mail_addresses = []
            for x in study.submitters:
                to_mail_addresses.extend(x.emails)

            cc_mail_addresses = []
            for x in study.contacts:
                cc_mail_addresses.extend(x.emails)

            send_task_email(
                study_id=study.study_id,
                task_name=task_name,
                subject_name=subject_name,
                body=body,
                from_mail_address=from_mail_address,
                # to_mail_addresses=to_mail_addresses,
                # cc_mail_addresses=cc_mail_addresses,
                to_mail_addresses="jhunter@ebi.ac.uk",
                cc_mail_addresses="ozgury@ebi.ac.uk",
                bcc_mail_addresses="metabolights-dev@ebi.ac.uk",
                reply_to="metabolights-help@ebi.ac.uk",
            )
            print(idx, len(studies), study.study_id, study.created_at, "Email sent successfully")
            if idx > 1:
                break
        except Exception as ex:
            raise ex
