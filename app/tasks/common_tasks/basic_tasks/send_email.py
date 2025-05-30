import datetime
import os
import re
from typing import List, Union
from app.config import get_settings
from app.config.utils import get_private_ftp_relative_root_path
from app.utils import current_time

from app.ws.db_connection import (
    get_email,
    query_study_submitters,
)
from app.ws.isaApiClient import IsaApiClient
from app.ws.study.user_service import UserService
from app.tasks.worker import (
    MetabolightsTask,
    celery,
    get_email_service,
    get_flask_app,
    report_internal_technical_issue,
    send_email,
)
from isatools.model import Study, Person



@celery.task(
    base=MetabolightsTask, name="app.tasks.common_tasks.basic_tasks.send_email.send_test_email"
)
def send_test_email(user_token):
    flask_app = get_flask_app()
    with flask_app.app_context():
        user = UserService.get_instance().validate_user_has_curator_role(user_token)
        email_service = get_email_service(flask_app)
        user_email = user.username
        time = current_time().isoformat()
        email_service.send_generic_email("Test Email", f"This email was sent at {time} from metabolights ws", "no-reply@ebi.ac.uk", user_email)
        return {
            "user_email": user_email,
            "time": time
        }

@celery.task(
    base=MetabolightsTask,
    name="app.tasks.common_tasks.basic_tasks.send_email.send_email_for_new_provisional_study",
)
def send_email_for_new_provisional_study(user_token, study_id, folder_name):

    flask_app = get_flask_app()
    with flask_app.app_context():
        
        relative_studies_root_path = get_private_ftp_relative_root_path()
        relative_study_path = os.path.join(
            os.sep, relative_studies_root_path.lstrip(os.sep), folder_name
        )
        user_email = get_email(user_token)
        submitter_emails = query_study_submitters(study_id)
        submitters_email_list = []
        if submitter_emails:
            submitters_email_list = [
                submitter[0] for submitter in submitter_emails if submitter
            ]
        if not submitters_email_list:
            return
        submitter_email = submitters_email_list[0]
        user = UserService.get_instance().get_db_user_by_user_name(submitter_email)
        submitter_fullname = user.fullName
        email_service = get_email_service(flask_app)
        email_service.send_email_for_new_provisional_study(
            study_id, relative_study_path, user_email, submitters_email_list, submitter_fullname
        )

        return {
            "study_id": study_id,
            "relative_study_path": relative_study_path,
            "user_email": user_email,
            "submitters_email_list": submitters_email_list,
        }


@celery.task(
    base=MetabolightsTask,
    name="app.tasks.common_tasks.basic_tasks.send_email.send_email_for_new_accession_number",
)
def send_email_for_new_accession_number(user_token: str, study_id: str, provisional_id: str, 
                                        obfuscation_code: str, study_title: str, release_date: str, additional_cc_emails: Union[None, List[str]] = None):

    flask_app = get_flask_app()
    with flask_app.app_context():   
        user_email = get_email(user_token)
        submitter_emails = query_study_submitters(study_id)
        submitters_email_list = []
        if submitter_emails:
            submitters_email_list = [
                submitter[0] for submitter in submitter_emails if submitter
            ]
        if not submitters_email_list:
            return
        submitter_email = submitters_email_list[0]
        user = UserService.get_instance().get_db_user_by_user_name(submitter_email)
        submitter_fullname = user.fullName
  
        ftp_user_home = get_settings().hpc_cluster.datamover.cluster_private_ftp_user_home_path
        private_ftp_root_path = get_settings().hpc_cluster.datamover.mounted_paths.cluster_private_ftp_root_path
        
        ftp_base_folder = private_ftp_root_path.replace(ftp_user_home, "")
        previous_ftp_folder = os.path.join(ftp_base_folder.rstrip('"'), f"{provisional_id.lower()}-{obfuscation_code}")
        new_ftp_folder = os.path.join(ftp_base_folder.rstrip('"'), f"{study_id.lower()}-{obfuscation_code}")
        email_service = get_email_service(flask_app)
        email_service.send_email_for_new_accession_number(
            study_id, provisional_id, obfuscation_code,  user_email, submitters_email_list,
            submitter_fullname=submitter_fullname, 
            study_title=study_title, release_date=release_date, 
            previous_ftp_folder=previous_ftp_folder, new_ftp_folder=new_ftp_folder, 
            additional_cc_emails=additional_cc_emails
        )

        return {
            "provisional_id": provisional_id,
            "study_id": study_id,
            "obfuscation_code": obfuscation_code,
            "user_email": user_email,
            "submitters_email_list": submitters_email_list,
        }

@celery.task(
    base=MetabolightsTask,
    name="app.tasks.common_tasks.basic_tasks.send_email.send_email_on_public",
)
def send_email_on_public(user_token, study_id, release_date):
    flask_app = get_flask_app()
    with flask_app.app_context():   
        user_email = get_email(user_token)
        submitter_emails = query_study_submitters(study_id)
        submitters_email_list = []
        if submitter_emails:
            submitters_email_list = [
                submitter[0] for submitter in submitter_emails if submitter
            ]
        if not submitters_email_list:
            return
        
        submitter_email = submitters_email_list[0]
        user = UserService.get_instance().get_db_user_by_user_name(submitter_email)
        submitter_fullname = user.fullName
        email_service = get_email_service(flask_app)
        iac = IsaApiClient()
        metadata_root_path = get_settings().study.mounted_paths.study_metadata_files_root_path
        study_location = os.path.join(metadata_root_path, study_id)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)
        study: Study = isa_study
        study_title = study.title
        study_contacts = ", ".join([f"{x.first_name} {x.last_name}" for x in study.contacts if x])
        publication_doi_list = [x.doi for x in study.publications]
        publication_pubmed_id_list = [x.pubmed_id for x in study.publications]
        publication_doi = ", ".join([x if x else "-" for x in publication_doi_list])
        publication_pubmed_id = ", ".join([x if x else "-" for x in publication_pubmed_id_list])
        if not publication_doi:
            publication_doi = "-"
        if not publication_pubmed_id:
            publication_pubmed_id = "-"
        if not study_contacts:
            study_contacts = user.fullName
        additional_cc_emails = get_principal_investigator_emails(study)
        email_service.send_email_on_public(
            study_id, release_date,  user_email, submitters_email_list, 
            submitter_fullname=submitter_fullname, study_title=study_title, 
            study_contacts=study_contacts, 
            publication_doi=publication_doi, 
            publication_pubmed_id=publication_pubmed_id,
            additional_cc_emails=additional_cc_emails
        )

        return {
            "study_id": study_id,
            "release_date": release_date,
            "user_email": user_email,
            "submitters_email_list": submitters_email_list,
        }

def get_principal_investigator_emails(study: Study):
    additional_cc_emails = []
    for contact in study.contacts:
        x: Person = contact
        if x.email:
            for role in x.roles:
                if role and role.term and role.term.lower().startswith("principal investigator"):
                    additional_cc_emails.append(x.email)
    additional_cc_emails = additional_cc_emails if additional_cc_emails else None
    return additional_cc_emails
        

@celery.task(name="app.tasks.common_tasks.basic_tasks.send_email.send_generic_email")
def send_generic_email(subject, body, from_address, to_addresses, cc_addresses):

    send_email(subject, body, from_address, to_addresses, cc_addresses)

@celery.task(name="app.tasks.common_tasks.basic_tasks.send_email.send_technical_issue_email")
def send_technical_issue_email(subject, body):
    report_internal_technical_issue(subject, body)


