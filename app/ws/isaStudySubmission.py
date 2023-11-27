#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Created: 2023-Nov-15
#  Modified by:   famaladoss
#
#  Copyright 2023 EMBL - European Bioinformatics Institute
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

import json
import os
from typing import Dict
from flask import request
from flask_restful import Resource, abort, reqparse
from marshmallow import ValidationError
from flask_restful_swagger import swagger
from app.study_folder_utils import get_all_metadata_files
from app.utils import metabolights_exception_handler
from app.ws.isa_api_client_v1 import IsaApiClientV1
from app.ws.isaApiClient import IsaApiClient
from app.ws.mm_models_v1 import IsaInvestigationSchemaV1
from app.ws.mtblsWSclient import WsClient
import logging
from app.ws.study import commons
from app.ws.study.study_service import identify_study_id
from app.ws.utils import log_request

logger = logging.getLogger('wslog')
wsc = WsClient()
isac_v1 = IsaApiClientV1()
isac = IsaApiClient()
configuration = 'MetaboLightsConfig20150707'
investigation_title = 'Investigation'
investigation_description = 'Created using isa_study endpoint'
metabolite_profiling_str = 'metabolite profiling'
        
class IsaStudySubmission(Resource):
    
    @swagger.operation(
        summary="Create Study using ISA-JSON",
        notes='''Create Study using ISA-JSON ''',
        parameters=[
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "study",
                "description": "Study data in ISA-JSON format",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 201,
                "message": "Created."
            },
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax."
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            }
        ]
    )
    def post(self):
        logger.info('Creating Stuyd using ISA-JSON')
        log_request(request)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            # user token is required
            abort(401)
            
        user_email = wsc.get_user_email(user_token)
        if user_email == False:
            abort(401)
        response = dict(targetRepository="metaboLights",
                        receipt={},
                        accessions=[])
        try:
            data = json.loads(request.data.decode('utf-8'))
            # if partial=True missing fields will be ignored
            isa_json = IsaInvestigationSchemaV1().load(data, partial=True)
            isa_json_data = isa_json.data
            
            study_id = 'MTBLS20008788'
            is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
            if not write_access:
                abort(403)

            isa_json_data = self.populate_investigation(study_id=study_id, submission_date=submission_date, 
                                                        release_date=release_date, isa_json_data=isa_json_data)
            isa_json_data = self.populate_study_details(study_id=study_id, submission_date=submission_date, 
                                                        release_date=release_date, isa_json_data=isa_json_data)
            mtbls_assays = self.filter_mtbls_assays(isa_json_data=isa_json_data)
            isa_json_data.studies[0].assays = mtbls_assays
            std_path = commons.get_study_location(study_id, user_token)
            isac_v1.write_isa_study(inv_obj=isa_json_data, api_key=user_token, std_path=std_path, 
                                    save_investigation_copy=False, save_samples_copy= False, save_assays_copy=False)
            response['receipt']['isaValidation'] = "success"
            response['investigation'] = IsaInvestigationSchemaV1().dump(isa_json_data).data
            response['receipt']['submission'] = "failure"
            
        except (ValidationError, Exception) as err:
            for arg in err.args:
                logger.error(arg)
            response['receipt']['submission'] = "failure"
            response['receipt']['isaValidation'] = "failure"
            abort(503)
        return response
    
    def get_study_filename(self, study_id):
        return 's_'+study_id+'.txt'
    
    def populate_investigation(self, study_id, submission_date, release_date, isa_json_data):
        isa_json_data.identifier = study_id
        isa_json_data.title = investigation_title
        isa_json_data.description = investigation_description
        isa_json_data.submission_date = submission_date
        isa_json_data.public_release_date = release_date
        return isa_json_data
    
    def populate_study_details(self, study_id, submission_date, release_date, isa_json_data):
        isa_json_data.studies[0].identifier = study_id
        isa_json_data.studies[0].filename = self.get_study_filename(study_id=study_id)
        isa_json_data.studies[0].submission_date = submission_date
        isa_json_data.studies[0].public_release_date = release_date
        return isa_json_data
    
    def filter_mtbls_assays(self, isa_json_data):
        mtbls_assays = []
        for assay in isa_json_data.studies[0].assays:
            measurement_type = assay.measurement_type.term
            if metabolite_profiling_str in measurement_type:
                mtbls_assays.append(assay)
        return mtbls_assays
        