#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Jun-06
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
import string
import json
from flask import request, abort
from flask_restful import Resource
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from app.ws.db_connection import update_study_status
from app.ws.validation import validate_study

logger = logging.getLogger('wslog')

# MetaboLights (Java-Based) WebService client
wsc = WsClient()


class StudyStatus(Resource):
    @swagger.operation(
        summary="Change study status",
        nickname="Change study status",
        notes='''Change study status from 'Submitted' to 'In Curation'.<br>
                <pre><code>Curators can change to any of: 'Submitted', 'In Curation', 'In Review', 'Public' or 'Dormant'
                </p>Example: { "status": "In Curation" }
                </code></pre>''',
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
                "name": "study_status",
                "description": "The status to change a study to",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
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
                "message": "OK. The Metabolite Annotation File (MAF) is returned"
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
    def put(self, study_id):

        # param validation
        if study_id is None:
            abort(404, 'Please provide valid parameter for study identifier')

        data_dict = json.loads(request.data.decode('utf-8'))
        study_status = data_dict['status']

        if study_status is None:
            abort(404, 'Please provide the new study status')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            db_study_status = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        if study_status.lower() == db_study_status.lower():
            abort(406, "Nothing to change")

        if is_curator:  # User is a curator, so just update status without any further checks
            self.update_status(study_id, study_status)
        elif write_access:
            if db_study_status != 'Submitted' and study_status != 'In Curation':
                abort(403, "You can not change to this status")

            if self.get_study_validation_status(study_id, study_location, user_token, obfuscation_code):
                self.update_status(study_id, study_status)
            else:
                abort(403, "There are validation errors. Fix any problems before attempting to change study status.")
        else:
            abort(403, "You do not have rights to change the status for this study")

        return {"Success": "Status updated from '" + db_study_status + "' to '" + study_status + "'"}

    @staticmethod
    def update_status(study_id, study_status):
        # Update database
        update_study_status(study_id, study_status)

    @staticmethod
    def get_study_validation_status(study_id, study_location, user_token, obfuscation_code):
        validates = validate_study(study_id, study_location, user_token, obfuscation_code, log_category='error')
        validations = validates['validation']
        status = validations['status']

        if status != 'error':
            return True

        return False
