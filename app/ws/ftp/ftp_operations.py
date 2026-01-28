import logging
import os.path

from flask import request
from flask_restful import Resource
from flask_restful_swagger import swagger

from app.config import get_settings
from app.config.utils import get_private_ftp_relative_root_path
from app.services.storage_service.storage_service import StorageService
from app.tasks.hpc_study_rsync_client import (
    StudyFolder,
    StudyFolderLocation,
    StudyFolderType,
    StudyRsyncClient,
)
from app.utils import (
    MetabolightsDBException,
    MetabolightsException,
    metabolights_exception_handler,
)
from app.ws.auth.permissions import (
    raise_deprecation_error,
    validate_data_files_upload,
    validate_submission_update,
    validate_submission_view,
    validate_user_has_curator_role,
    validate_user_has_role,
)
from app.ws.db.types import StudyStatus
from app.ws.ftp.ftp_utils import (
    get_ftp_folder_access_status,
    toogle_ftp_folder_permission,
)
from app.ws.mtblsWSclient import WsClient
from app.ws.study.study_service import StudyService
from app.ws.utils import log_request

logger = logging.getLogger("wslog")


class SyncCalculation(Resource):
    @swagger.operation(
        summary="[Deprecated] Calculate differences between FTP upload folder and study folder",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
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
                "default": False,
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK. Calculated."},
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def post(self, study_id):
        log_request(request)
        raise_deprecation_error(request)
        result = validate_submission_update(request)
        study_id = result.context.study_id

        force_recalculate = (
            True if request.args.get("force", "").lower() == "true" else False
        )

        status_check_only = False if force_recalculate else True
        study = StudyService.get_instance().get_study_by_acc(study_id)
        client = StudyRsyncClient(
            study_id=study_id, obfuscation_code=study.obfuscationcode
        )
        source = StudyFolder(
            location=StudyFolderLocation.PRIVATE_FTP_STORAGE,
            folder_type=StudyFolderType.METADATA,
        )
        target = StudyFolder(
            location=StudyFolderLocation.RW_STUDY_STORAGE,
            folder_type=StudyFolderType.METADATA,
        )
        status = client.rsync_dry_run(
            source, target, status_check_only=status_check_only
        )
        return status.model_dump()


class SyncFromFtpFolder(Resource):
    @swagger.operation(
        summary="[Deprecated] FTP folder sync process is triggered, and new/updated files are copied to study folder",
        nickname="Start sync process new and updated files from upload folder",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
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
                "default": "metadata",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def post(self, study_id):
        log_request(request)
        raise_deprecation_error(request)

        sync_type = request.headers.get("sync_type", "metadata")
        if sync_type not in ("data", "internal", "metadata"):
            raise MetabolightsException(message="sync_type is not valid")

        study = None
        if sync_type not in ("data", "metadata"):
            result = validate_user_has_curator_role(request)
        else:
            result = validate_submission_update(request)
        study_id = result.context.study_id

        if sync_type == "metadata":
            source = StudyFolder(
                location=StudyFolderLocation.PRIVATE_FTP_STORAGE,
                folder_type=StudyFolderType.METADATA,
            )
            target = StudyFolder(
                location=StudyFolderLocation.RW_STUDY_STORAGE,
                folder_type=StudyFolderType.METADATA,
            )
        elif sync_type == "data":
            source = StudyFolder(
                location=StudyFolderLocation.PRIVATE_FTP_STORAGE,
                folder_type=StudyFolderType.DATA,
            )
            target = StudyFolder(
                location=StudyFolderLocation.READONLY_STUDY_STORAGE,
                folder_type=StudyFolderType.DATA,
            )
        elif sync_type == "internal":
            source = StudyFolder(
                location=StudyFolderLocation.PRIVATE_FTP_STORAGE,
                folder_type=StudyFolderType.INTERNAL,
            )
            target = StudyFolder(
                location=StudyFolderLocation.RW_STUDY_STORAGE,
                folder_type=StudyFolderType.INTERNAL,
            )
        if not study:
            study = StudyService.get_instance().get_study_by_acc(study_id)
        client = StudyRsyncClient(
            study_id=study_id, obfuscation_code=study.obfuscationcode
        )

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
        summary="[Deprecated] Returns  status of FTP folder synchronization task",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def get(self, study_id):
        log_request(request)
        raise_deprecation_error(request)
        result = validate_submission_update(request)
        study_id = result.context.study_id
        study = StudyService.get_instance().get_study_by_acc(study_id)
        client = StudyRsyncClient(
            study_id=study_id, obfuscation_code=study.obfuscationcode
        )
        source = StudyFolder(
            location=StudyFolderLocation.PRIVATE_FTP_STORAGE,
            folder_type=StudyFolderType.METADATA,
        )
        target = StudyFolder(
            location=StudyFolderLocation.RW_STUDY_STORAGE,
            folder_type=StudyFolderType.METADATA,
        )
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
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK. FTP folder permission returned"},
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def get(self, study_id):
        log_request(request)
        result = validate_submission_view(request)
        study_id = result.context.study_id

        return get_ftp_folder_access_status(study_id)


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
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK. FTP folder permission toggled "},
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def put(self, study_id):
        log_request(request)
        result = validate_user_has_role(request, study_required=True)
        study_id = result.context.study_id

        return toogle_ftp_folder_permission(study_id)


class PrivateFtpFolder(Resource):
    @swagger.operation(
        summary="[Deprecated] Create a new study upload FTP folder",
        parameters=[
            {
                "name": "study_id",
                "description": "Existing Study Identifier to add an upload folder to",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
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
                "default": True,
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication. "
                "Please provide a study id and a valid user token",
            },
            {
                "code": 403,
                "message": "Study does not exist or your do not have access to this study.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def post(self, study_id):
        log_request(request)
        raise_deprecation_error(request)
        result = validate_submission_update(request)
        study_id = result.context.study_id
        user_token = result.context.user_api_token

        send_email = True and request.args.get("send_email", "").lower() == "true"

        study_id = study_id.upper()

        logger.info("Creating a new study upload folder for study %s", study_id)
        study = StudyService.get_instance().get_study_by_acc(study_id)
        return WsClient.create_upload_folder(
            study_id, study.obfuscationcode, user_token, send_email=send_email
        )


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
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK. FTP folder permission toggled "},
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def get(self, study_id: str):
        log_request(request)
        result = validate_data_files_upload(request)
        study_id = result.context.study_id
        obfuscation_code = result.context.obfuscation_code

        relative_studies_root_path = get_private_ftp_relative_root_path()
        folder_name = f"{study_id.lower()}-{obfuscation_code}"
        relative_ftp_study_path = os.path.join(
            os.sep, relative_studies_root_path.lstrip(os.sep), folder_name
        )
        return relative_ftp_study_path


class PrivateFtpUploadInfo(Resource):
    @swagger.operation(
        summary="Get FTP study folder path used to upload and its credentials",
        nickname="Get FTP study folder path used to upload and its credentials",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS or REQ Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK. FTP folder permission toggled "},
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def get(self, study_id: str):
        log_request(request)
        study = None
        try:
            study = StudyService.get_instance().get_study_by_req_or_mtbls_id(study_id)
        except MetabolightsDBException:
            pass
        if not study:
            raise MetabolightsDBException("identifier is not valid", http_code=404)

        result = validate_data_files_upload(request)
        study_id = result.context.study_id

        relative_studies_root_path = get_private_ftp_relative_root_path()
        folder_name = f"{study_id.lower()}-{study.obfuscationcode}"
        relative_ftp_study_path = os.path.join(
            os.sep, relative_studies_root_path.lstrip(os.sep), folder_name
        )
        ftp_connection = get_settings().ftp_server.private.connection
        return {
            "study_id": study.acc,
            "ftp_folder": relative_ftp_study_path,
            "ftp_host": ftp_connection.host,
            "ftp_user": ftp_connection.username,
            "ftp_password": ftp_connection.password,
            "obfuscation_code": study.obfuscationcode,
            "reserved_accession": study.reserved_accession,
            "reserved_submission_id": study.reserved_submission_id,
            "mhd_accession": study.mhd_accession,
        }


class SyncFromStudyFolder(Resource):
    @swagger.operation(
        summary="[Deprecated] Copy files from study folder to private FTP  folder",
        nickname="Copy from study folder",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
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
                "default": "metadata",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK. Files/Folders were copied across."},
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def post(self, study_id):
        log_request(request)
        raise_deprecation_error(request)
        sync_type = request.headers.get("sync_type", "metadata")
        if sync_type not in ("data", "internal", "metadata"):
            raise MetabolightsException(message="sync_type is not valid")

        study = None
        if sync_type not in ("data", "metadata"):
            validate_user_has_curator_role(request)
        else:
            validate_submission_update(request)

        if sync_type == "metadata":
            target = StudyFolder(
                location=StudyFolderLocation.PRIVATE_FTP_STORAGE,
                folder_type=StudyFolderType.METADATA,
            )
            source = StudyFolder(
                location=StudyFolderLocation.RW_STUDY_STORAGE,
                folder_type=StudyFolderType.METADATA,
            )
        elif sync_type == "data":
            target = StudyFolder(
                location=StudyFolderLocation.PRIVATE_FTP_STORAGE,
                folder_type=StudyFolderType.DATA,
            )
            source = StudyFolder(
                location=StudyFolderLocation.READONLY_STUDY_STORAGE,
                folder_type=StudyFolderType.DATA,
            )
        elif sync_type == "internal":
            target = StudyFolder(
                location=StudyFolderLocation.PRIVATE_FTP_STORAGE,
                folder_type=StudyFolderType.INTERNAL,
            )
            source = StudyFolder(
                location=StudyFolderLocation.RW_STUDY_STORAGE,
                folder_type=StudyFolderType.INTERNAL,
            )
        if not study:
            study = StudyService.get_instance().get_study_by_acc(study_id)
        client = StudyRsyncClient(
            study_id=study_id, obfuscation_code=study.obfuscationcode
        )

        status = client.rsync(source, target, status_check_only=False)
        return status.model_dump()


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
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK. Files/Folders were copied across."},
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def post(self, study_id):
        log_request(request)
        result = validate_submission_update(request)
        study_id = result.context.study_id

        if result.context.study_status != StudyStatus.PUBLIC:
            return {"Error": "Given study is not public yet!"}
        study_path = os.path.join(
            get_settings().study.mounted_paths.study_metadata_files_root_path, study_id
        )

        ftp_public_storage = StorageService.get_ftp_public_storage()
        logger.info(
            "Syncing files from public study folder to FTP folder for %s", study_id
        )
        meta_public_sync_status, files_public_sync_status = (
            ftp_public_storage.sync_to_public_ftp(
                source_local_folder=study_path, target_folder=study_id, ignore_list=None
            )
        )
        return {
            "meta_sync_status": meta_public_sync_status,
            "files_sync_status": files_public_sync_status,
        }
