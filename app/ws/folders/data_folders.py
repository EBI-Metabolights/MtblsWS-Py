#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Feb-26
#  Modified by:   kenneth
#
#  Copyright 2019 EMBL - European Bioinformatics Institute
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

import logging

from flask import request
from flask_restful import Resource
from flask_restful_swagger import swagger

from app.tasks.datamover_tasks.basic_tasks.file_management import create_folders
from app.utils import MetabolightsException, metabolights_exception_handler
from app.ws.auth.permissions import (
    raise_deprecation_error,
    validate_user_has_curator_role,
)
from app.ws.settings.utils import get_cluster_settings

logger = logging.getLogger("wslog")


class DataFolders(Resource):
    @swagger.operation(
        summary="[Deprecated] Update folder or update permission",
        notes="""...""",
        parameters=[
            {
                "name": "folder_path",
                "description": "Folder path",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "folder_permission",
                "description": "Folder permission in octal. Example 770, 750, 700, ...",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
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
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
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
            {"code": 417, "message": "Unexpected result."},
        ],
    )
    @metabolights_exception_handler
    def put(self):
        raise_deprecation_error(request)
        result = validate_user_has_curator_role(request)

        folder_path = request.args.get("folder_path")
        folder_permission = request.args.get("folder_permission")
        try:
            folder_permission_int = int(folder_permission, 8)
        except Exception as exc:
            folder_permission_int = 0o700
        try:
            inputs = {
                "folder_paths": folder_path,
                "acl": folder_permission_int,
                "exist_ok": True,
            }
            task = create_folders.apply_async(kwargs=inputs, expires=60 * 5)
            cluster_settings = get_cluster_settings()
            result = task.get(timeout=cluster_settings.task_get_timeout_in_seconds * 2)

            return result
        except Exception as ex:
            raise MetabolightsException(
                http_code=500,
                message="Create folder task submission was failed",
                exception=ex,
            )
