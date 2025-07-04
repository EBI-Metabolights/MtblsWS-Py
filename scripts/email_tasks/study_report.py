import datetime
import logging
import os
import pathlib
import sys
from app.config import get_settings
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study
from app.ws.db.types import StudyStatus
from scripts.email_tasks.models import (
    MetaboLightsStudyReport,
    StudyContact,
    StudyOverview,
    StudyPublication,
    load_study_report,
    save_study_report,
)
from metabolights_utils.isatab import Reader
from metabolights_utils.isatab.reader import (
    InvestigationFileReader,
    InvestigationFileReaderResult,
)
from metabolights_utils.models.isa import investigation_file as inv_model

logger = logging.getLogger(__name__)


def update_status():
    study_report: MetaboLightsStudyReport = load_study_report()
    
    study_report_items = {x.study_id:x for x in study_report.studies}
    with DBManager.get_instance().session_maker() as db_session:
        studies: list[str, int] = db_session.query(Study.acc, Study.status).all()
    for study in studies:
        study_id = study[0]
        if study_id in study_report_items:
            current_status = study_report_items[study_id].status.capitalize()
            new_status_item = StudyStatus(study[1])
            new_status = new_status_item.name.capitalize()
            if current_status != new_status:
                study_report_items[study_id].status = new_status
            
    
    save_study_report(report=study_report)
    

def create_study_report(study_report_path: str):
    loaded_studies = {}
    failed_studies = {}
    target_path = pathlib.Path(study_report_path)
    if target_path.exists():
        report = MetaboLightsStudyReport.model_validate_json(target_path.open().read())
        for item in report.studies:
            loaded_studies[item.study_id] = item
    else:
        now = datetime.datetime.now(datetime.UTC)
        report = MetaboLightsStudyReport(report_datetime=now)

    study_root_path = get_settings().study.mounted_paths.study_metadata_files_root_path
    with DBManager.get_instance().session_maker() as db_session:
        studies: list[Study] = db_session.query(Study).all()
        sorted_studies: list[Study] = [x for x in studies]
        sorted_studies.sort(
            key=lambda x: int(x.acc.replace("MTBLS", "").replace("REQ", ""))
        )
        for idx, study in enumerate(sorted_studies):
            if study.acc in loaded_studies:
                logger.info("%s skipping.", study.acc)
                continue
            try:
                if idx == 0:
                    logger.info("Task started.")
                elif idx % 100 == 0:
                    target_path.open("w").write(report.model_dump_json(indent=2))
                    pathlib.Path("failed_studies.json").open("w").write(str(failed_studies))
                    logger.info("Current: %s", study.acc)
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

                study = (
                    db_session.query(Study).filter(Study.acc == study.acc).one_or_none()
                )
                all_emails: dict[str, StudyContact] = {}
                if study.users:
                    for submitter in study.users:
                        try:
                            submitter = StudyContact(
                                emails=[submitter.email],
                                full_name=f"{submitter.firstname} {submitter.lastname}",
                            )
                            all_emails[submitter.emails[0]] = submitter
                            study_overview.submitters.append(submitter)
                        except Exception as ex:
                            logger.error("Skipping study... %s %s", study.acc, str(ex))

                # Load investigation file
                study_path = os.path.join(
                    study_root_path, study.acc, "i_Investigation.txt"
                )
                if not os.path.exists(study_path):
                    print(f"{study.acc} investigation file is not found.")
                    continue
                reader: InvestigationFileReader = Reader.get_investigation_file_reader()
                result: InvestigationFileReaderResult = reader.read(
                    file_buffer_or_path=study_path
                )

                if not result.investigation.studies:
                    logger.error("%s has no study", study.acc)
                study_description: inv_model.Study = result.investigation.studies[0]
                study_overview.title = study_description.title

                if study_description.study_publications.publications:
                    publications = []
                    study_overview.publications = publications
                    for item in study_description.study_publications.publications:
                        publication: inv_model.Publication = item

                        new_publication = StudyPublication(
                            title=publication.title,
                            authors_list=publication.author_list,
                            doi=publication.doi,
                            pub_med_id=publication.doi,
                        )
                        publications.append(new_publication)
                if study_description.study_contacts.people:
                    contacts = []
                    for item in study_description.study_contacts.people:
                        contact: inv_model.Person = item
                        if not contact.email:
                            continue
                        new_contact = None
                        full_name = ""
                        try:
                            names = [
                                x.capitalize()
                                for x in (
                                    contact.first_name,
                                    contact.mid_initials,
                                    contact.last_name,
                                )
                                if x and x.strip()
                            ]
                            full_name = " ".join(names)
                            new_contact = StudyContact(
                                emails=split_email_address_text(contact.email),
                                full_name=full_name,
                            )
                        except Exception as ex:
                            logger.error(
                                "Contact information error for %s '%s': %s",
                                study.acc,
                                full_name,
                                ex,
                            )

                        if new_contact and new_contact.emails and new_contact.emails[0] not in all_emails:
                            all_emails[new_contact.emails[0]] = new_contact
                            contacts.append(new_contact)
                    study_overview.contacts = contacts
                # logger.info("%s summary is completed.", study.acc)
                loaded_studies[study.acc] = study_overview
                if study_overview.submitters:
                    report.studies.append(study_overview)
                else:
                    failed_studies[study.acc] = "No submitter email address"
            except Exception as ex:
                failed_studies[study.acc] = str(ex)
                logger.error("%s %s", study.acc, str(ex))
                logger.exception(ex)

    target_path.open("w").write(report.model_dump_json(indent=2))
    pathlib.Path("failed_studies.json").open("w").write(str(failed_studies))
    logger.info("Current: %s", study.acc)
    return study_report_path

def split_email_address_text(email_str=str) -> list[str]:
    new_str = email_str.replace("and", " ").replace(",", " ").replace(";", " ")
    emails = [x.strip() for x in new_str.split() if x and x.strip() and "@" in x]
    return emails


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s [%(moduleName)s] %(message)s",
        datefmt="%d/%b/%Y %H:%M:%S",
        stream=sys.stdout,
    )
    logger.setLevel(logging.DEBUG)
    study_report_path =  "study_report.json"
    # study_report = create_study_report(study_report_path=study_report_path)
    
    update_status()