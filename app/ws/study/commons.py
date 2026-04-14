import json
import logging
import os
from typing import Union

from app.config.utils import get_private_ftp_relative_root_path
from app.services.storage_service.acl import Acl
from app.services.storage_service.storage_service import StorageService
from app.ws.auth.auth_manager import AuthenticationManager
from app.ws.db_connection import (
    get_all_non_public_studies,
    get_private_studies,
    get_provisional_study_ids_for_user,
    get_public_studies,
    get_study_by_type,
    query_study_submitters,
)
from app.ws.email.email_service import EmailService
from app.ws.study.user_service import UserService

logger = logging.getLogger("wslog")


def get_study_by_status(stype, publicS=True):
    logger.info("Getting all public studies")
    studyID, studytype = get_study_by_type(stype, publicS)
    res = json.dumps(dict(zip(studyID, studytype)))
    return res


def get_private_studies_list():
    logger.info("Getting all private studies")
    study_list = get_private_studies()
    studies = [acc[0] for acc in study_list]
    studies.sort(key=sort_by_study_id)

    logger.info("... found %d private studies", len(studies))
    return {"studies": len(studies), "content": studies}


def get_non_public_studies_list():
    logger.info("Getting all non public studies")
    study_list = get_all_non_public_studies()
    studies = [acc[0] for acc in study_list]
    studies.sort(key=sort_by_study_id)

    logger.info(f"...found {len(studies)} non public studies")
    return {"studies": len(studies), "content": studies}


def sort_by_study_id(key: str):
    if key:
        val = key.replace("MTBLS", "").replace("REQ", "")
        if val.isnumeric():
            return int(val)
    return -1


def get_public_studies_list():
    logger.info("Getting all public studies")

    study_list = get_public_studies()
    studies = [acc for acc in study_list]
    studies.sort(key=sort_by_study_id)
    logger.info("... found %d public studies", len(studies))
    return {"studies": len(studies), "content": studies}


def get_all_studies_for_user(user_token):
    logger.info("Getting provisional studies using user_token")
    study_id_list = get_provisional_study_ids_for_user(user_token)
    study_id_list.sort(key=sort_by_study_id)
    text_resp = json.dumps(study_id_list)
    logger.info("Found the following studies %s", text_resp)
    return text_resp


def create_ftp_folder(
    study_id,
    obfuscation_code,
    username,
    email_service: Union[None, EmailService] = None,
    send_email=True,
):
    private_ftp_sm = StorageService.get_ftp_private_storage()
    new_folder_name = study_id.lower() + "-" + obfuscation_code

    new_folder = False
    if not private_ftp_sm.remote.does_folder_exist(new_folder_name):
        logger.info("Creating a new study upload folder for Study %s", study_id)
        raw_files_path = os.path.join(new_folder_name, "RAW_FILES")
        derived_files_path = os.path.join(new_folder_name, "DERIVED_FILES")
        supplementary_files_path = os.path.join(new_folder_name, "SUPPLEMENTARY_FILES")

        logger.info(f"Creating folder {new_folder_name}")
        folders = [
            new_folder_name,
            raw_files_path,
            derived_files_path,
            supplementary_files_path,
        ]
        private_ftp_sm.remote.create_folder(
            folders, acl=Acl.AUTHORIZED_READ_WRITE, exist_ok=True
        )
        new_folder = True

    relative_studies_root_path = get_private_ftp_relative_root_path()
    relative_study_path = os.path.join(
        os.sep, relative_studies_root_path.lstrip(os.sep), new_folder_name
    )

    if new_folder and send_email:
        submitter_emails = query_study_submitters(study_id)
        submitters_email_list = []
        if submitter_emails:
            submitters_email_list = [
                submitter[0] for submitter in submitter_emails if submitter
            ]
        submitter_email = submitters_email_list[0]
        auth_manager = AuthenticationManager.get_instance()
        user = UserService.get_instance(auth_manager).get_db_user_by_user_name(
            submitter_email
        )
        submitter_fullname = user.fullName

        email_service.send_email_for_new_provisional_study(
            study_id,
            relative_study_path,
            username,
            submitters_email_list,
            submitter_fullname,
        )
    status_message = "FTP folder created" if new_folder else "Folder is already created"

    return {
        "os_upload_path": new_folder_name,
        "upload_location": relative_study_path,
        "status": status_message,
    }
