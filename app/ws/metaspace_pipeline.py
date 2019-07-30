#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Jul-24
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
from flask import request, abort
from flask.json import jsonify
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import *
from app.ws.isaApiClient import IsaApiClient
import json

logger = logging.getLogger('wslog')
wsc = WsClient()
iac = IsaApiClient()


class MetaspacePipeLine(Resource):
    @swagger.operation(
        summary="Import files files and metadata from METASPACE to a MTBLS study",
        nickname="Import data from METASPACE",
        notes="""Import files files and metadata from METASPACE to a MetaboLights study
            </p><pre><code>{
    "project": {
        "metaspace-api-key": "12489afjhadkjfhajfh",
        "metaspace-projects": [
            {
                "project_id": "2017-04-11_18h29m09s"
            },
            {
                "project_id": "2018-07-21_19h48m04s"
            }
        ]
    }
} </code></pre>""",
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
                "name": "project",
                "description": "METASPACE project info",
                "paramType": "body",
                "type": "string",
                "required": False,
                "allowMultiple": False
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

        # body content validation
        project = {}
        if request.data:
            try:
                data_dict = json.loads(request.data.decode('utf-8'))
                project = data_dict['project']
                if project:
                    metaspace_api_key = project['metaspace-api-key']
                    metaspace_projects = project['metaspace-projects']
                    for m_proj in metaspace_projects:
                        project_id = m_proj['project_id']
                        logger.info('Requesting METASPACE project ' + project_id + ' for API-key ' + metaspace_api_key)
            except KeyError:
                abort(419, "No 'project' parameter was provided.")

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        return {"Success": "Not yet implemented publicly"}




