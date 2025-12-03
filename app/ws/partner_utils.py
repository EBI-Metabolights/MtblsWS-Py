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
import os

from flask import request
from flask_restful import Resource
from flask_restful_swagger import swagger

from app.config import get_settings
from app.tasks.datamover_tasks.curation_tasks.metabolon import metabolon_confirm
from app.utils import MetabolightsException
from app.ws.auth.permissions import validate_user_has_curator_role
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient

wsc = WsClient()
iac = IsaApiClient()
logger = logging.getLogger("wslog")


class Metabolon(Resource):
    @swagger.operation(
        summary="Confirm all files are uploaded",
        notes="""Confirm that all raw/mzML files has been uploaded to this studies upload folder. </br>
        Files uploaded for clients will be added to the final study before templates are applied</br>
        </P>
        This may take some time as mzML validation and conversion to ISA-Tab will now take place""",
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
    def post(self, study_id):
        result = validate_user_has_curator_role(request, study_required=True)
        email = result.context.username

        mounted_paths = get_settings().hpc_cluster.datamover.mounted_paths
        study_location = os.path.join(
            mounted_paths.cluster_private_ftp_root_path,
            f"{result.context.study_id.lower()}-{result.context.obfuscation_code}",
        )

        try:
            inputs = {
                "study_id": study_id,
                "study_location": study_location,
                "email": email,
            }

            result = metabolon_confirm.apply_async(kwargs=inputs, expires=60 * 5)

            result = {
                "content": f"Task has been started. Result will be sent by email. Task id: {result.id}",
                "message": None,
                "err": None,
            }
            return result
        except Exception as ex:
            raise MetabolightsException(
                http_code=500,
                message="Metabolon confirm task submission was failed",
                exception=ex,
            )
