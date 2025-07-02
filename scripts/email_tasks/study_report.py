import datetime
import logging
import os
import pathlib
from app.config import get_settings
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study, User
from app.ws.db.types import StudyStatus
from scripts.email_tasks.models import (
    MetaboLightsStudyReport,
    StudyContact,
    StudyOverview,
)
from isatools import isatab, model

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    
    now = datetime.datetime.now(datetime.UTC)
    report = MetaboLightsStudyReport(report_datetime=now)
    study_root_path = get_settings().study.mounted_paths.study_metadata_files_root_path
    with DBManager.get_instance().session_maker() as db_session:
        studies: list[Study] = db_session.query(Study).all()
        sorted_studies = [x for x in studies]
        sorted_studies.sort(
            key=lambda x: int(x.acc.replace("MTBLS", "").replace("REQ", ""))
        )
        for idx, study in enumerate(sorted_studies):
            if idx > 10:
                break
            study_overview = StudyOverview(
                study_id=study.acc,
                status=StudyStatus(study.status).name.lower().capitalize(),
                created_at=study.submissiondate,
                first_private_date=study.first_private_date,
                first_public_date=study.first_public_date,
                revision=study.revision_number,
                revision_datetime=study.revision_datetime,
                reserved_accession=study.reserved_accession,
                reserved_submission_id=study.reserved_submission_id,
            )
            report.studies.append(study_overview)
            study = db_session.query(Study).filter(Study.acc == study.acc).one_or_none()
            all_emails: dict[str, StudyContact] = {}
            if study.users:
                for submitter in study.users:
                    submitter = StudyContact(
                        emails=[submitter.email],
                        full_name=f"{submitter.firstname} {submitter.lastname}",
                    )
                    all_emails[submitter.emails[0]] = submitter
                    study_overview.submitters.append(submitter)

            # Load investigation file
            study_path = os.path.join(study_root_path, study.acc)
            investigation: model.Investigation = isatab.load(
                study_path, skip_load_tables=True
            )
            if not investigation.studies:
                logger.error("%s has no study", study.acc)
            study_description: model.Study = investigation.studies[0]
            study_overview.title = study_description.title

            if study_description.contacts:
                contacts = []
                for item in study_description.contacts:
                    contact: model.Person = item
                    if not contact.email:
                        continue
                    new_contact = None
                    try:
                        names = [
                            x
                            for x in (
                                contact.first_name,
                                contact.mid_initials,
                                contact.last_name,
                            )
                            if x and x.strip()
                        ]
                        full_name = " ".join(names)
                        new_contact = StudyContact(
                            emails=[contact.email], full_name=full_name
                        )
                    except Exception as ex:
                        logger.error("Contact information error: %s", ex)

                    if new_contact and new_contact.emails[0] not in all_emails:
                        all_emails[new_contact.emails[0]] = new_contact
                        contacts.append(new_contact)
                study_overview.contacts = contacts
            logger.info("%s summary is completed.")

    pathlib.Path("study_report.json").open("w").write(report.model_dump_json(indent=2))
