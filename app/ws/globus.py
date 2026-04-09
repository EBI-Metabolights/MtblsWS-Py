import logging
import os

from flask import request
from flask_restful import Resource, abort
from flask_restful_swagger import swagger
from pydantic import BaseModel

from app.config import get_settings
from app.services.storage_service import get_globus_client
from app.services.storage_service.globus import GlobusClient, GlobusPermission, Identity
from app.utils import (
    metabolights_exception_handler,
)
from app.ws.auth.permissions import (
    validate_submission_update,
    validate_user_has_curator_role,
    validate_user_has_submitter_or_super_user_role,
)
from app.ws.study.study_service import StudyService

logger = logging.getLogger(__name__)


class GlobusPermissionsResponse(BaseModel):
    content: None | list[GlobusPermission] = None
    message: None | str = None


class GlobusPermission(Resource):
    def get_private_ftp_path(self, study_id: str, obfuscation_code: str):
        ftp_path = get_settings().hpc_cluster.datamover.mounted_paths.cluster_private_ftp_root_path
        user = get_settings().ftp_server.private.connection.username
        subfolder = ftp_path.split(user)[1]
        study_folder_name = f"{study_id.lower()}-{obfuscation_code}"
        return os.path.join(subfolder, study_folder_name)

    @swagger.operation(
        summary="Get Globus folder permission for a study",
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
    )
    @metabolights_exception_handler
    def get(self, study_id):
        result = validate_submission_update(request)
        globus_client: GlobusClient = get_globus_client()
        study = StudyService.get_instance().get_study_by_req_or_mtbls_id(
            identifier=study_id
        )
        folder_path = self.get_private_ftp_path(
            study_id=study.acc, obfuscation_code=study.obfuscationcode
        )
        globus_username = result.context.globus_username
        permissions = globus_client.get_folder_permissions(
            folder_path=folder_path, user_emails=globus_username
        )
        return GlobusPermissionsResponse(content=permissions).model_dump(by_alias=True)

    @swagger.operation(
        summary="Add or update Globus folder permission for a study",
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
            {
                "name": "access-type",
                "description": "User API token",
                "paramType": "query",
                "type": "string",
                "enum": ["read-write", "read"],
                "default": "read-write",
                "required": False,
                "allowMultiple": False,
            },
        ],
    )
    @metabolights_exception_handler
    def put(self, study_id):
        result = validate_submission_update(request)
        globus_username = result.context.globus_username
        if not globus_username:
            abort(404, message="There is not Globus username.")
        access_type = request.args.get("access-type", "read-write").lower()
        permission = "r" if access_type == "read" else "rw"
        globus_client: GlobusClient = get_globus_client()
        study = StudyService.get_instance().get_study_by_req_or_mtbls_id(
            identifier=result.context.study_id
        )
        folder_path = self.get_private_ftp_path(
            study_id=study.acc, obfuscation_code=study.obfuscationcode
        )
        permissions = globus_client.update_folder_permission(
            folder_path=folder_path,
            user_email=globus_username,
            permission=permission,
            notify_email=False,
        )
        return GlobusPermissionsResponse(content=permissions).model_dump(by_alias=True)

    @swagger.operation(
        summary="Delete a Globus folder permission for a study",
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
    )
    @metabolights_exception_handler
    def delete(self, study_id):
        result = validate_submission_update(request)
        globus_client: GlobusClient = get_globus_client()
        study = StudyService.get_instance().get_study_by_req_or_mtbls_id(
            identifier=result.context.study_id
        )
        globus_username = result.context.globus_username
        folder_path = self.get_private_ftp_path(
            study_id=study.acc, obfuscation_code=study.obfuscationcode
        )
        permissions = globus_client.remove_folder_permission(
            folder_path=folder_path, user_emails=globus_username
        )
        return GlobusPermissionsResponse(content=permissions).model_dump(by_alias=True)


class GlobusPermissions(Resource):
    def get_private_ftp_path(self, study_id: str, obfuscation_code: str):
        ftp_path = get_settings().hpc_cluster.datamover.mounted_paths.cluster_private_ftp_root_path
        user = get_settings().ftp_server.private.connection.username
        subfolder = ftp_path.split(user)[1]
        study_folder_name = f"{study_id.lower()}-{obfuscation_code}"
        return os.path.join(subfolder, study_folder_name)

    @swagger.operation(
        summary="Get Globus folder permissions for a study (curator only)",
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
    )
    @metabolights_exception_handler
    def get(self, study_id):
        validate_user_has_curator_role(request, study_required=True)
        globus_client: GlobusClient = get_globus_client()
        study = StudyService.get_instance().get_study_by_req_or_mtbls_id(
            identifier=study_id
        )
        folder_path = self.get_private_ftp_path(
            study_id=study.acc, obfuscation_code=study.obfuscationcode
        )
        permissions = globus_client.get_folder_permissions(
            folder_path=folder_path, user_emails=None
        )
        return GlobusPermissionsResponse(content=permissions).model_dump(by_alias=True)

    @swagger.operation(
        summary="Add or update Globus folder permission for a study (Curator only)",
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
            {
                "name": "globus-username",
                "description": "Globus username",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "access-type",
                "description": "User API token",
                "paramType": "query",
                "type": "string",
                "enum": ["read", "read/write"],
                "default": "read/write",
                "required": True,
                "allowMultiple": False,
            },
        ],
    )
    @metabolights_exception_handler
    def put(self, study_id):
        result = validate_user_has_curator_role(request)
        globus_username = request.headers.get("globus-username")
        if not globus_username:
            abort(404, message="There is not Globus username.")
        access_type = request.args.get("access-type", "rw").lower()
        permission = "r" if access_type == "read" else "rw"
        globus_client: GlobusClient = get_globus_client()

        study = StudyService.get_instance().get_study_by_req_or_mtbls_id(
            identifier=result.context.study_id
        )
        folder_path = self.get_private_ftp_path(
            study_id=study.acc, obfuscation_code=study.obfuscationcode
        )
        permissions = globus_client.update_folder_permission(
            folder_path=folder_path,
            user_email=globus_username,
            permission=permission,
            notify_email=False,
        )
        return GlobusPermissionsResponse(content=permissions).model_dump(by_alias=True)

    @swagger.operation(
        summary="Delete a Globus folder permission for a study (Curator only)",
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
            {
                "name": "globus-username",
                "description": "Globus username",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
    )
    @metabolights_exception_handler
    def delete(self, study_id):
        result = validate_user_has_curator_role(request)
        globus_client: GlobusClient = get_globus_client()
        study = StudyService.get_instance().get_study_by_req_or_mtbls_id(
            identifier=result.context.study_id
        )
        globus_username = request.headers.get("globus-username")
        folder_path = self.get_private_ftp_path(
            study_id=study.acc, obfuscation_code=study.obfuscationcode
        )
        permissions = globus_client.remove_folder_permission(
            folder_path=folder_path, user_emails=globus_username
        )
        return GlobusPermissionsResponse(content=permissions).model_dump(by_alias=True)


class GlobusIdentities(Resource):
    @swagger.operation(
        summary="Globus identities",
        parameters=[
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "globus-username",
                "description": "Globus username",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
            {
                "name": "globus-identity-id",
                "description": "Globus identity id",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
        ],
    )
    @metabolights_exception_handler
    def get(self):
        result = validate_user_has_submitter_or_super_user_role(request)
        globus_client: GlobusClient = get_globus_client()
        globus_username = request.headers.get("globus-username", "")
        globus_identity_id = request.headers.get("globus-identity-id", "")
        if not globus_identity_id and not globus_username:
            raise Exception("globus username or identity id must be defined.")
        if globus_username:
            result = globus_client.get_globus_identities_by_email(
                user_email=globus_username
            )
        else:
            result = globus_client.get_globus_identities_by_id(id_=globus_identity_id)
        return [Identity.model_dump(x, by_alias=True) for x in result]
