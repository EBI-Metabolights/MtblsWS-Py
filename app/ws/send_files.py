#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Aug-02
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

from flask_restful import Resource
from flask_restful_swagger import swagger
from flask import request, send_file, safe_join, abort, make_response
from app.ws.mtblsWSclient import WsClient
from app.ws.db_connection import get_obfuscation_code
import logging

logger = logging.getLogger('wslog')
# MetaboLights (Java-Based) WebService client
wsc = WsClient()


class SendFiles(Resource):
    @swagger.operation(
        summary="Stream file(s) to the browser",
        notes="Download/Stream files from the study folder",
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
                "name": "file_name",
                "description": "File name",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "obfuscation_code",
                "description": "Study obfuscation code",
                "required": False,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": False,
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
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token"
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self, study_id, file_name, obfuscation_code=None):
        # param validation
        if study_id is None or file_name is None:
            logger.info('No study_id and/or file name given')
            abort(404)
        study_id = study_id.upper()

        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            user_token = "public_access_only"

        # check for access rights
        is_curator, read_access, write_access, db_obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)

        if not read_access:
            db_obfuscation_code_list = get_obfuscation_code(study_id)
            db_obfuscation_code = db_obfuscation_code_list[0][0]

        if db_obfuscation_code != obfuscation_code:
            abort(403)

        safe_path = safe_join(study_location, file_name)

        try:
            resp = make_response(send_file(safe_path, as_attachment=True, attachment_filename=file_name))
            # response.headers["Content-Disposition"] = "attachment; filename={}".format(file_name)
            resp.headers['Content-Type'] = 'application/octet-stream'
            return resp
        except FileNotFoundError:
            abort(404, "Could not find file " + file_name)

