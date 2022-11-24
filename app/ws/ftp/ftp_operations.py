import logging
import os.path

from flask import current_app as app, jsonify
from flask import request, abort
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger

from app.services.storage_service.acl import Acl
from app.services.storage_service.storage_service import StorageService
from app.utils import metabolights_exception_handler
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
                "name": "user_token",
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
            abort(404, 'Please provide valid parameter for study identifier')
        study_id = study_id.upper()
        parser = reqparse.RequestParser()
        parser.add_argument('force', help='Force to recalculate')
        force_recalculate = False

        if request.args:
            args = parser.parse_args(req=request)
            force_recalculate = True if args['force'].lower() == 'true' else False

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance(app).validate_user_has_write_access(user_token, study_id)

        study = StudyService.get_instance(app).get_study_by_acc(study_id)
        study_path = os.path.join(app.config.get('STUDY_PATH'), study_id)
        storage = StorageService.get_ftp_private_storage(app)

        result = storage.calculate_sync_status(study_id, study.obfuscationcode, study_path, force=force_recalculate)
        return jsonify(result.dict())


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
                "name": "user_token",
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
            abort(404, 'Please provide valid parameter for study identifier')
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance(app).validate_user_has_write_access(user_token, study_id)

        study = StudyService.get_instance(app).get_study_by_acc(study_id)
        study_path = os.path.join(app.config.get('STUDY_PATH'), study_id)
        storage = StorageService.get_ftp_private_storage(app)

        ftp_folder_name = f"{study_id.lower()}-{study.obfuscationcode}"
        ignore_list = app.config.get('INTERNAL_MAPPING_LIST')
        storage.sync_from_storage(ftp_folder_name, study_path, ignore_list=ignore_list, logger=logger)

        return jsonify({"status": "sync task is started."})


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
                "name": "user_token",
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
            abort(404, 'Please provide valid parameter for study identifier')
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance(app).validate_user_has_write_access(user_token, study_id)

        study = StudyService.get_instance(app).get_study_by_acc(study_id)
        study_path = os.path.join(app.config.get('STUDY_PATH'), study_id)
        storage = StorageService.get_ftp_private_storage(app)

        result = storage.check_folder_sync_status(study_id, study.obfuscationcode, study_path)
        return jsonify(result.dict())

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
                "name": "user_token",
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
            abort(404, 'Please provide valid parameter for study identifier')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # Security check
        UserService.get_instance(app).validate_user_has_write_access(user_token, study_id)

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
                "name": "user_token",
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
            abort(404, 'Please provide valid parameter for study identifier')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance(app).validate_user_has_write_access(user_token, study_id)
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
                "name": "user_token",
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

        parser = reqparse.RequestParser()
        parser.add_argument('send_email', help='send_email')

        send_email = None
        if request.args:
            args = parser.parse_args(req=request)
            send_email = True if args['send_email'] and args['send_email'].lower() == "true" else False

        study_id = study_id.upper()

        logger.info('Creating a new study upload folder for study %s', study_id)
        study = StudyService.get_instance(app).get_study_by_acc(study_id)
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
                "name": "user_token",
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
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404, 'Please provide valid parameter for study identifier')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance(app).validate_user_has_write_access(user_token, study_id)
        study = StudyService.get_instance(app).get_study_by_acc(study_id)
        relative_studies_root_path = app.config.get("PRIVATE_FTP_RELATIVE_STUDIES_ROOT_PATH")
        folder_name = f'{study_id.lower()}-{study.obfuscationcode}'
        relative_ftp_study_path = os.path.join(os.sep, relative_studies_root_path.lstrip(os.sep), folder_name)
        return relative_ftp_study_path


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
                "name": "user_token",
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
            abort(404, 'Please provide valid parameter for study identifier')
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance(app).validate_user_has_write_access(user_token, study_id)
        study = StudyService.get_instance(app).get_study_by_acc(study_id)
        destination = study_id.lower() + '-' + study.obfuscationcode

        ftp_private_storage = StorageService.get_ftp_private_storage(app)
        logger.info(f"syncing files from study folder to FTP folder for {study_id}")

        ftp_private_storage.remote.create_folder(destination, acl=Acl.AUTHORIZED_READ_WRITE, exist_ok=True)

        ftp_private_storage.sync_from_local(study_id, destination, logger=logger, purge=False)

        logger.info('Copying file %s to FTP %s', study_id, destination)
        return {'Success': 'Copying files from study folder to ftp folder is started'}

