import json
import logging
import os

from flask import abort, current_app as app
from app.config import get_settings

from app.services.storage_service.acl import Acl
from app.services.storage_service.storage_service import StorageService
from app.ws.db_connection import check_access_rights, get_submitted_study_ids_for_user, get_email, \
    query_study_submitters, get_public_studies, get_private_studies, get_study_by_type, get_all_non_public_studies
from app.ws.settings.utils import get_study_settings

logger = logging.getLogger('wslog')


def get_study_by_status(stype, publicS=True):
    logger.info('Getting all public studies')
    studyID, studytype = get_study_by_type(stype, publicS)
    res = json.dumps(dict(zip(studyID, studytype)))
    return res


def get_user_email(user_token):
    user_email = get_email(user_token)
    logger.info(" User Email: " + str(user_email))
    return user_email


def get_private_studies_list():
    logger.info('Getting all private studies')
    studies = []
    study_list = get_private_studies()
    for acc in study_list:
        studies.append(acc[0])

    logger.info('... found %d private studies', len(studies))
    return {"studies": len(studies), "content": studies}

def get_non_public_studies_list():
    logger.info('Getting all non public studies')
    studies = []
    study_list = get_all_non_public_studies()
    for acc in study_list:
        studies.append(acc[0])

    logger.info(f'...found {len(studies)} non public studies')
    return {"studies": len(studies), "content": studies}

def get_public_studies_list():
    logger.info('Getting all public studies')
    studies = []
    study_list = get_public_studies()
    for acc in study_list:
        studies.append(acc[0])

    logger.info('... found %d public studies', len(studies))
    return {"studies": len(studies), "content": studies}


def get_all_studies_for_user(user_token):
    logger.info('Getting submitted studies using user_token')
    study_id_list = get_submitted_study_ids_for_user(user_token)
    text_resp = json.dumps(study_id_list)
    logger.info('Found the following studies %s', text_resp)
    return text_resp


def get_permissions(study_id, user_token, obfuscation_code=None):
    """
    Check MTBLS-WS for permissions on this Study for this user

    Study       User    Submitter   Curator     Reviewer/Read-only
    SUBMITTED   ----    Read+Write  Read+Write  Read
    INCURATION  ----    Read        Read+Write  Read
    INREVIEW    ----    Read        Read+Write  Read
    PUBLIC      Read    Read        Read+Write  Read

    :param obfuscation_code:
    :param study_id:
    :param user_token:
    :return: study details and permission levels

    """
    if not user_token:
        user_token = "public_access_only"

    # Reviewer access will pass the study obfuscation code instead of api_key
    if study_id and not obfuscation_code and user_token.startswith("ocode:"):
        logger.info("Study obfuscation code passed instead of user API_CODE")
        obfuscation_code = user_token.replace("ocode:", "")

    is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
    updated_date, study_status = check_access_rights(user_token, study_id.upper(),
                                                     study_obfuscation_code=obfuscation_code)

    logger.info("Read access: " + str(read_access) + ". Write access: " + str(write_access))

    return is_curator, read_access, write_access, obfuscation_code, study_location, release_date, \
           submission_date, study_status


def get_study_location(study_id, user_token):
    """
    Get the actual location of the study files in the File System

    :param study_id: Identifier of the study in MetaboLights
    :param user_token: User API token. Used to check for permissions
    """
    logger.info('Getting actual location for Study %s on the filesystem', study_id)

    is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
        get_permissions(study_id, user_token)
    if not read_access:
        abort(403)
    settings = get_study_settings()
    location = os.path.join(settings.study_metadata_files_root_path, study_id.upper())
    if not os.path.isdir(location):
        abort(404, 'There is no path for %s' % (study_id,))
    logger.info('... found study folder %s', location)
    return location


def create_ftp_folder(study_id, obfuscation_code, user_token, email_service=None, send_email=True):
    private_ftp_sm = StorageService.get_ftp_private_storage()
    new_folder_name = study_id.lower() + '-' + obfuscation_code

    new_folder = False
    if not private_ftp_sm.remote.does_folder_exist(new_folder_name):
        logger.info('Creating a new study upload folder for Study %s', study_id)
        raw_files_path = os.path.join(new_folder_name, "RAW_FILES")
        derived_files_path = os.path.join(new_folder_name, "DERIVED_FILES")

        logger.info(f"Creating folder {new_folder_name}")
        folders = [new_folder_name, raw_files_path, derived_files_path]
        private_ftp_sm.remote.create_folder(folders, acl=Acl.AUTHORIZED_READ_WRITE, exist_ok=True)
        new_folder = True

    relative_studies_root_path = get_settings().ftp_server.private.configuration.private_ftp_folders_relative_path
    relative_study_path = os.path.join(os.sep, relative_studies_root_path.lstrip(os.sep), new_folder_name)

    if new_folder and send_email:
        user_email = get_email(user_token)
        submitter_emails = query_study_submitters(study_id)
        submitters_email_list = []
        if submitter_emails:
            submitters_email_list = [submitter[0] for submitter in submitter_emails if submitter]

        email_service.send_email_for_requested_ftp_folder_created(study_id, relative_study_path, user_email,
                                                                  submitters_email_list)
    status_message = "FTP folder created" if new_folder else "Folder is already created"

    return {'os_upload_path': new_folder_name, 'upload_location': relative_study_path, 'status': status_message}
