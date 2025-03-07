import logging
import os.path

from flask import current_app as app, request
from flask_restful import Resource, reqparse, abort
from flask_restful_swagger import swagger
from app.config import get_settings
from app.config.utils import get_private_ftp_relative_root_path

from app.services.storage_service.acl import Acl
from app.services.storage_service.storage_service import StorageService
from app.tasks.hpc_study_rsync_client import StudyFolder, StudyFolderLocation, StudyFolderType, StudyRsyncClient
from app.utils import MetabolightsException, metabolights_exception_handler
from app.ws.db.types import StudyStatus
from app.ws.ftp.ftp_utils import get_ftp_folder_access_status, toogle_ftp_folder_permission
from app.ws.mtblsWSclient import WsClient
from app.ws.study.study_service import StudyService
from app.ws.study.user_service import UserService
from app.ws.utils import log_request

logger = logging.getLogger('wslog')


class SyncCalculation(Resource):
    @swagger.operation(
        summary="Calculate differences between FTP upload folder and study folder",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "force",
                "description": "Force to recalculate updates on upload folder.",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": False,
                "default": False
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. Calculated."
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    @metabolights_exception_handler
    def post(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404, message='Please provide valid parameter for study identifier')
        study_id = study_id.upper()
        
        
        force_recalculate = False
        
        if request.args:
            
            force_recalculate = True if request.args.get('force').lower() == 'true' else False

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        status_check_only = False if force_recalculate else True
        study = StudyService.get_instance().get_study_by_acc(study_id)
        client = StudyRsyncClient(study_id=study_id, obfuscation_code=study.obfuscationcode)
        source = StudyFolder(location=StudyFolderLocation.PRIVATE_FTP_STORAGE, folder_type=StudyFolderType.METADATA)
        target = StudyFolder(location=StudyFolderLocation.RW_STUDY_STORAGE, folder_type=StudyFolderType.METADATA)
        status = client.rsync_dry_run(source, target, status_check_only=status_check_only)
        return status.model_dump()        
        
        # study_path = os.path.join(get_settings().study.mounted_paths.study_metadata_files_root_path, study_id)
        # storage = StorageService.get_ftp_private_storage()
        
        # meta_calc_result = storage.calculate_sync_status(study_id, study.obfuscationcode, study_path, force=force_recalculate)
        # # return jsonify({'result':{'meta_calc_result':meta_calc_result.model_dump(),'rdfiles_calc_result': rdfiles_calc_result.model_dump()}})
        # return jsonify(meta_calc_result.model_dump())


class SyncFromFtpFolder(Resource):
    @swagger.operation(
        summary="FTP folder sync process is triggered, and new/updated files are copied to study folder",
        nickname="Start sync process new and updated files from upload folder",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "sync-type",
                "description": "Sync category: sync metadada or data or internal files",
                "required": False,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
                "enum": ["metadata", "data", "internal"],
                "allowEmptyValue": False,
                "defaultValue": "metadata",
                "default": "metadata"
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    @metabolights_exception_handler
    def post(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            raise MetabolightsException(message='Please provide valid parameter for study identifier')
        study_id = study_id.upper()
        sync_type = "metadata"
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        
        if "sync_type" in request.headers:
            sync_type = request.headers["sync_type"]
        if sync_type not in ("data", "internal", "metadata"):
            raise MetabolightsException(message="sync_type is not valid")
        study = None
        if sync_type not in ("data", "metadata"):
            UserService.get_instance().validate_user_has_write_access(user_token, study_id)
            study = StudyService.get_instance().get_study_by_acc(study_id)
            if StudyStatus(study.status) != StudyStatus.SUBMITTED:
                UserService.get_instance().validate_user_has_curator_role(user_token)
        else:
            UserService.get_instance().validate_user_has_curator_role(user_token)
            
        if sync_type == "metadata":
            source = StudyFolder(location=StudyFolderLocation.PRIVATE_FTP_STORAGE, folder_type=StudyFolderType.METADATA)
            target = StudyFolder(location=StudyFolderLocation.RW_STUDY_STORAGE, folder_type=StudyFolderType.METADATA)
        elif sync_type == "data":
            source = StudyFolder(location=StudyFolderLocation.PRIVATE_FTP_STORAGE, folder_type=StudyFolderType.DATA)
            target = StudyFolder(location=StudyFolderLocation.READONLY_STUDY_STORAGE, folder_type=StudyFolderType.DATA)
        elif sync_type == "internal":
            source = StudyFolder(location=StudyFolderLocation.PRIVATE_FTP_STORAGE, folder_type=StudyFolderType.INTERNAL)
            target = StudyFolder(location=StudyFolderLocation.RW_STUDY_STORAGE, folder_type=StudyFolderType.INTERNAL)
        if not study:
            study = StudyService.get_instance().get_study_by_acc(study_id)
        client = StudyRsyncClient(study_id=study_id, obfuscation_code=study.obfuscationcode)
        
        
        status = client.rsync(source, target, status_check_only=False)
        return status.model_dump()

        # study_path = os.path.join(get_settings().study.mounted_paths.study_metadata_files_root_path, study_id)
        # storage = StorageService.get_ftp_private_storage()

        # ftp_folder_name = f"{study_id.lower()}-{study.obfuscationcode}"
        # ignore_list = get_settings().file_filters.internal_mapping_list
        # meta_sync_status = storage.sync_from_storage(ftp_folder_name, study_path, ignore_list=ignore_list, logger=logger)
        # return jsonify({'meta_sync_status': meta_sync_status.model_dump()})
        # meta_sync_status,rdfiles_sync_status = storage.sync_from_storage(ftp_folder_name, study_path, ignore_list=ignore_list, logger=logger)

        # return jsonify({'meta_sync_status': meta_sync_status, 'files_sync_status': rdfiles_sync_status})


class FtpFolderSyncStatus(Resource):
    @swagger.operation(
        summary="Returns  status of FTP folder synchronization task",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    @metabolights_exception_handler
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404, message='Please provide valid parameter for study identifier')
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_write_access(user_token, study_id)

        study = StudyService.get_instance().get_study_by_acc(study_id)
        client = StudyRsyncClient(study_id=study_id, obfuscation_code=study.obfuscationcode)
        source = StudyFolder(location=StudyFolderLocation.PRIVATE_FTP_STORAGE, folder_type=StudyFolderType.METADATA)
        target = StudyFolder(location=StudyFolderLocation.RW_STUDY_STORAGE, folder_type=StudyFolderType.METADATA)
        status = client.rsync(source, target, status_check_only=True)
        return status.model_dump()

class FtpFolderPermission(Resource):
    @swagger.operation(
        summary="Get Study FTP folder permission",
        nickname="Get FTP study permission",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. FTP folder permission returned"
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    @metabolights_exception_handler
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404, message='Please provide valid parameter for study identifier')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # Security check
        UserService.get_instance().validate_user_has_write_access(user_token, study_id)

        return get_ftp_folder_access_status(app, study_id)


class FtpFolderPermissionModification(Resource):
    @swagger.operation(
        summary="Change FTP study upload folder permission",
        nickname="Change FTP study permission",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. FTP folder permission toggled "
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    @metabolights_exception_handler
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404, message='Please provide valid parameter for study identifier')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        return toogle_ftp_folder_permission(app, study_id)


class PrivateFtpFolder(Resource):
    @swagger.operation(
        summary="Create a new study upload FTP folder",
        parameters=[
            {
                "name": "study_id",
                "description": "Existing Study Identifier to add an upload folder to",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "send_email",
                "description": "Send email if folder is created",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication. "
                           "Please provide a study id and a valid user token"
            },
            {
                "code": 403,
                "message": "Study does not exist or your do not have access to this study."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    @metabolights_exception_handler
    def post(self, study_id):
        log_request(request)
        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None or study_id is None:
            abort(401)
        send_email = None
        if request.args:
            
            send_email = True if request.args.get('send_email') and request.args.get('send_email').lower() == "true" else False

        study_id = study_id.upper()

        logger.info('Creating a new study upload folder for study %s', study_id)
        study = StudyService.get_instance().get_study_by_acc(study_id)
        return WsClient.create_upload_folder(study_id, study.obfuscationcode, user_token, send_email=send_email)


class PrivateFtpFolderPath(Resource):
    @swagger.operation(
        summary="Get FTP study folder path used to upload",
        nickname="Get FTP study folder path used to upload",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. FTP folder permission toggled "
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    @metabolights_exception_handler
    def get(self, study_id: str):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404, message='Please provide valid parameter for study identifier')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        study = StudyService.get_instance().get_study_by_acc(study_id)
        relative_studies_root_path = get_private_ftp_relative_root_path()
        folder_name = f'{study_id.lower()}-{study.obfuscationcode}'
        relative_ftp_study_path = os.path.join(os.sep, relative_studies_root_path.lstrip(os.sep), folder_name)
        return relative_ftp_study_path


class PrivateFtpUploadInfo(Resource):
    @swagger.operation(
        summary="Get FTP study folder path used to upload and its credentials",
        nickname="Get FTP study folder path used to upload and its credentials",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. FTP folder permission toggled "
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    @metabolights_exception_handler
    def get(self, study_id: str):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404, message='Please provide valid parameter for study identifier')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        study = StudyService.get_instance().get_study_by_acc(study_id)
        relative_studies_root_path = get_private_ftp_relative_root_path()
        folder_name = f'{study_id.lower()}-{study.obfuscationcode}'
        relative_ftp_study_path = os.path.join(os.sep, relative_studies_root_path.lstrip(os.sep), folder_name)
        ftp_connection = get_settings().ftp_server.private.connection
        return {"study_id": study_id, 
                "ftp_folder": relative_ftp_study_path, 
                "ftp_host": ftp_connection.host, 
                "ftp_user": ftp_connection.username, 
                "ftp_password": ftp_connection.password }
    
class SyncFromStudyFolder(Resource):
    @swagger.operation(
        summary="Copy files from study folder to private FTP  folder",
        nickname="Copy from study folder",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "sync-type",
                "description": "Sync category: sync metadada or data or internal files",
                "required": False,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
                "enum": ["metadata", "data", "internal"],
                "allowEmptyValue": False,
                "defaultValue": "metadata",
                "default": "metadata"
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. Files/Folders were copied across."
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    @metabolights_exception_handler
    def post(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            raise MetabolightsException(message='Please provide valid parameter for study identifier')
        study_id = study_id.upper()
        sync_type = "metadata"
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        
        if "sync_type" in request.headers:
            sync_type = request.headers["sync_type"]
            
        if sync_type not in ("data", "internal", "metadata"):
            raise MetabolightsException(message="sync_type is not valid")
        
        
        study = None
        if sync_type not in ("data", "metadata"):
            UserService.get_instance().validate_user_has_write_access(user_token, study_id)
            study = StudyService.get_instance().get_study_by_acc(study_id)
            if StudyStatus(study.status) != StudyStatus.SUBMITTED:
                UserService.get_instance().validate_user_has_curator_role(user_token)
        else:
            UserService.get_instance().validate_user_has_curator_role(user_token)
            
        if sync_type == "metadata":
            target = StudyFolder(location=StudyFolderLocation.PRIVATE_FTP_STORAGE, folder_type=StudyFolderType.METADATA)
            source = StudyFolder(location=StudyFolderLocation.RW_STUDY_STORAGE, folder_type=StudyFolderType.METADATA)
        elif sync_type == "data":
            target = StudyFolder(location=StudyFolderLocation.PRIVATE_FTP_STORAGE, folder_type=StudyFolderType.DATA)
            source = StudyFolder(location=StudyFolderLocation.READONLY_STUDY_STORAGE, folder_type=StudyFolderType.DATA)
        elif sync_type == "internal":
            target = StudyFolder(location=StudyFolderLocation.PRIVATE_FTP_STORAGE, folder_type=StudyFolderType.INTERNAL)
            source = StudyFolder(location=StudyFolderLocation.RW_STUDY_STORAGE, folder_type=StudyFolderType.INTERNAL)
        if not study:
            study = StudyService.get_instance().get_study_by_acc(study_id)
        client = StudyRsyncClient(study_id=study_id, obfuscation_code=study.obfuscationcode)
        
        
        status = client.rsync(source, target, status_check_only=False)
        return status.model_dump()
    
        # log_request(request)
        # # param validation
        # if study_id is None:
        #     abort(404, message='Please provide valid parameter for study identifier')
        # study_id = study_id.upper()

        # # User authentication
        # user_token = None
        # if "user_token" in request.headers:
        #     user_token = request.headers["user_token"]

        # 
        # 
        # sync_only_chebi_results = True
        # if request.args:
        #     
        #     sync_only_chebi_results = False if request.args.get('sync_only_chebi_pipeline_results').lower() == 'false' else True

        # UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        # study = StudyService.get_instance().get_study_by_acc(study_id)
        # destination = study_id.lower() + '-' + study.obfuscationcode

        # ftp_private_storage = StorageService.get_ftp_private_storage()
        # logger.info(f"syncing files from study folder to FTP folder for {study_id}")

        # ftp_private_storage.remote.create_folder(destination, acl=Acl.AUTHORIZED_READ_WRITE, exist_ok=True)

        # meta_sync_status,files_sync_status,chebi_sync_status = ftp_private_storage.sync_from_local(source_local_folder=None, target_folder=destination, ignore_list=None, sync_chebi_annotation=sync_only_chebi_results)
        # logger.info('Copying file %s to FTP %s', study_id, destination)
        # return {'meta_sync_status': meta_sync_status, 'files_sync_status':files_sync_status, 'chebi_sync_status':chebi_sync_status}

class SyncPublicStudyToFTP(Resource):
    @swagger.operation(
        summary="Sync study files from public study folder to public FTP",
        nickname="Sync from public study to FTP",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. Files/Folders were copied across."
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    @metabolights_exception_handler
    def post(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404, message='Please provide valid parameter for study identifier')
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_curator_role(user_token)
        study = StudyService.get_instance().get_study_by_acc(study_id)
        if study.status != 0:
           return {'Error': 'Given study is not public yet!'} 
        study_path = os.path.join(get_settings().study.mounted_paths.study_metadata_files_root_path, study_id)

        ftp_public_storage = StorageService.get_ftp_public_storage()
        logger.info(f"Syncing files from public study folder to FTP folder for {study_id}")
        meta_public_sync_status,files_public_sync_status = ftp_public_storage.sync_to_public_ftp(source_local_folder=study_path, target_folder=study_id, ignore_list=None)
        return {'meta_sync_status': meta_public_sync_status, 'files_sync_status':files_public_sync_status}