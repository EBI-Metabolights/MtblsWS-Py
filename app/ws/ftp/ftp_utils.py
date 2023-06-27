import logging

from app.services.storage_service.acl import Acl
from app.services.storage_service.storage_service import StorageService
from app.ws.study.study_service import StudyService
from app.ws.study.user_service import UserService

logger = logging.getLogger('wslog')


def get_ftp_folder_access_status(app, study_id):

    study = StudyService.get_instance().get_study_by_acc(study_id)
    ftp_private_study_folder = study_id.lower() + '-' + study.obfuscationcode
    ftp_private_storage = StorageService.get_ftp_private_storage()
    logger.info("Getting ftp folder permission")
    access = ""
    try:
        if ftp_private_storage.remote.does_folder_exist(ftp_private_study_folder):
            permission = ftp_private_storage.remote.get_folder_permission(ftp_private_study_folder)
            if permission == Acl.AUTHORIZED_READ_WRITE:
                access = "Write"
            else:
                if permission == Acl.AUTHORIZED_READ or permission == Acl.READ_ONLY:
                    access = "Read"
                else:
                    access = "Unkown"
                    return {'Access': access, 'status': 'error', 'message': "Permission status is unknown"}
        else:
            return {'Access': access, 'status': 'error', 'message': "There is no folder"}
        return {'Access': access, 'status': 'success'}
    except OSError as e:
        logger.error('Error in getting the permission for %s ', ftp_private_study_folder, str(e))
        return {'Access': access, 'status': 'error', 'message': "Internal error"}


def toogle_ftp_folder_permission(app, study_id):
    study = StudyService.get_instance().get_study_by_acc(study_id)
    ftp_study_folder = study_id.lower() + '-' + study.obfuscationcode
    ftp_private_storage = StorageService.get_ftp_private_storage()
    logger.info("changing ftp folder permission")
    try:
        access = "Unknown"
        if ftp_private_storage.remote.does_folder_exist(ftp_study_folder):
            permission = ftp_private_storage.remote.get_folder_permission(ftp_study_folder)
            if permission == Acl.AUTHORIZED_READ_WRITE:
                ftp_private_storage.remote.update_folder_permission(ftp_study_folder, Acl.AUTHORIZED_READ)
                access = "Read"
            elif permission == Acl.AUTHORIZED_READ or permission == Acl.READ_ONLY:
                ftp_private_storage.remote.update_folder_permission(ftp_study_folder, Acl.AUTHORIZED_READ_WRITE)
                access = "Write"

        return {'Access': access}
    except OSError as e:
        logger.error(f'Error in updating the permission for {ftp_study_folder} Error {str(e)}')