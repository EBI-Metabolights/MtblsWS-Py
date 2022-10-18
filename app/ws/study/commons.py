import json
import logging
import os

from flask import abort, current_app as app

from app.ws.db_connection import check_access_rights, get_submitted_study_ids_for_user, get_email, \
    query_study_submitters, get_public_studies, get_private_studies, get_study_by_type, get_all_non_public_studies

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


def get_queue_folder():
    # Updated to remove Java WS /study/getQueueFolder dependency
    queue_folder = app.config.get('STUDY_QUEUE_FOLDER')
    logger.info('Found queue upload folder for this server as:' + queue_folder)
    return queue_folder


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

    location = os.path.join(app.config.get('STUDY_PATH'), study_id.upper())
    location = os.path.join(app.config.get('DEBUG_STUDIES_PATH'), location.strip('/'))
    if not os.path.isdir(location):
        abort(404, 'There is no path for %s' % (study_id,))
    logger.info('... found study folder %s', location)
    return location


def create_ftp_folder(study_id, obfuscation_code, user_token, email_service):
    new_folder_name = study_id.lower() + '-' + obfuscation_code
    ftp_folder = os.path.join(app.config.get('MTBLS_FTP_ROOT'), new_folder_name)
    os_upload = ftp_folder
    new_folder = False
    if not os.path.exists(ftp_folder):
        logger.info('Creating a new study upload folder for Study %s', study_id)
        ftp_private_folder_root = app.config.get('MTBLS_PRIVATE_FTP_ROOT')
        ftp_path = os.path.join(ftp_private_folder_root, new_folder_name)
        raw_files_path = os.path.join(ftp_path, "RAW_FILES")
        derived_files_path = os.path.join(ftp_path, "DERIVED_FILES")
        logger.info(f"Creating folder {ftp_path}")
        previous_mask = os.umask(0)
        try:
            os.makedirs(ftp_path, mode=0o770, exist_ok=True)
            os.makedirs(raw_files_path, mode=0o770, exist_ok=True)
            os.makedirs(derived_files_path, mode=0o770, exist_ok=True)
        finally:
            os.umask(previous_mask)
        os_upload = ftp_path
        new_folder = True

    upload_loc = None
    private_ftp_user = app.config.get("PRIVATE_FTP_SERVER_USER")
    if private_ftp_user in ftp_folder:
        upload_location = ftp_folder.split('/' + private_ftp_user + '/')  # FTP/Aspera root starts here
        upload_location = [x for x in upload_location if x]
        if len(upload_location) > 0:
            upload_loc = upload_location[1]
        else:
            upload_loc = upload_location[0]

    if new_folder:
        user_email = get_email(user_token)
        submitter_emails = query_study_submitters(study_id)
        submitters_email_list = []
        if submitter_emails:
            submitters_email_list = [submitter[0] for submitter in submitter_emails if submitter]

        email_service.send_email_for_requested_ftp_folder_created(study_id,
                                                                  upload_loc, user_email,
                                                                  submitters_email_list)
    status_message = "FTP folder created" if new_folder else "Folder is already created"

    return {'os_upload_path': os_upload, 'upload_location': upload_loc, 'status': status_message}
