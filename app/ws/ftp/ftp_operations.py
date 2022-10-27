import logging
import os.path

from flask import current_app as app, jsonify
from flask import request, abort
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger

from app.services.storage_service.storage_service import StorageService
from app.utils import metabolights_exception_handler
from app.ws.study.study_service import StudyService
from app.ws.study.user_service import UserService
from app.ws.utils import log_request

logger = logging.getLogger('wslog')


class SyncCalculation(Resource):
    @swagger.operation(
        summary="Calculate differences between upload folder and study folder, returns new and updated uploaded files",
        nickname="Calculate new and updated files on upload folder",
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
        summary="Sync process is triggered, and new/updated files in upload folder are copied to study folder",
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
        summary="Returns sync status",
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