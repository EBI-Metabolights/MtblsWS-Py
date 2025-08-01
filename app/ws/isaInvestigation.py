#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Mar-15
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

import json
import os
from flask import request
from flask_restful import Resource, abort, reqparse
from isatools.model import Investigation
from marshmallow import ValidationError
from flask_restful_swagger import swagger
from app.config import get_settings
from app.study_folder_utils import get_all_metadata_files
from app.utils import metabolights_exception_handler
from app.ws.db.models import StudyRevisionModel
from app.ws.db.types import CurationRequest
from app.ws.isaApiClient import IsaApiClient
from app.ws.mm_models import IsaInvestigationSchema
from app.ws.mtblsWSclient import WsClient
import logging

from app.ws.study.study_revision_service import StudyRevisionService
from app.ws.study.study_service import StudyService, identify_study_id
from app.ws.utils import log_request


logger = logging.getLogger('wslog')
iac = IsaApiClient()
wsc = WsClient()


class IsaInvestigation(Resource):

    @swagger.operation(
        summary="Get ISA Investigation",
        notes="Get the whole ISA Investigation in a single JSON. "
              "Study obfuscation code is read first and will provide read-only access to the study. "
              "User API token will give read/write access",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "investigation_only",
                "description": "Only load the main investigation file?",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True
            },
            {
                "name": "obfuscation-code",
                "description": "Study obfuscation code",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False
            },
            {
                "name": "user-token",
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
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax."
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
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
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        study_id = study_id.upper()

        logger.info('    ----    LOADING ISA STUDY %s    ----', study_id)

        # User authentication
        user_token = None
        obfuscation_code = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']

        if 'obfuscation_code' in request.headers:
            obfuscation_code = request.headers['obfuscation_code']

        # query validation
        # 
        # 
        investigation_only = True
        if request.args:
            investigation_only = request.args.get('investigation_only')

        skip_load_tables = True
        if investigation_only == 'false':
            skip_load_tables = False

        study_id, obfuscation_code = identify_study_id(study_id, obfuscation_code)
        logger.info('Getting Investigation %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token, obfuscation_code)
        if not read_access:
            abort(403, message=f"Study {study_id} is invalid or private.")
        study = StudyService.get_instance().get_study_by_acc(study_id)
        metadata_files = get_all_metadata_files(study_location)
        investigation_file = [x for x in metadata_files if os.path.basename(x).lower() == "i_investigation.txt"]
        if not investigation_file:
            abort(404, message=f"There is no i_Investigation.txt file on {study_id} folder.")
            
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id,
                                                         user_token,
                                                         skip_load_tables=skip_load_tables,
                                                         study_location=study_location)

        logger.info('Got %s', isa_inv.identifier)
        revision_status = None
        revision_comment = None
        revision_task_message = None
        if study.revision_number > 0:
            revision: StudyRevisionModel = StudyRevisionService.get_study_revision(study.acc, revision_number=study.revision_number)
            if revision:
                revision_status = revision.status.value
                revision_comment = revision.revision_comment or ""
                revision_task_message = revision.task_message or ""
        http_url = None 
        ftp_url = None
        globus_url = None
        aspera_path = None
        if study_status == "Public":
            configuration = get_settings().ftp_server.public.configuration
            http_url = os.path.join(configuration.public_studies_http_base_url, study_id)
            ftp_url = os.path.join(configuration.public_studies_ftp_base_url, study_id)
            globus_url = os.path.join(configuration.public_studies_globus_base_url, study_id)
            aspera_path = os.path.join(configuration.public_studies_aspera_base_path, study_id)
            
        response = dict(mtblsStudy={},
                        isaInvestigation={},
                        validation={})
        response['mtblsStudy']['studyStatus'] = study_status
        response['mtblsStudy']['curationRequest'] = ""
        if hasattr(study, 'curation_request'):
            response['mtblsStudy']['curationRequest'] = CurationRequest(study.curation_request).name
        response['mtblsStudy']['modifiedTime'] = study.updatedate.isoformat()
        response['mtblsStudy']['statusUpdateTime'] = study.status_date.isoformat() if study.status_date else ""
        response['mtblsStudy']['read_access'] = read_access
        response['mtblsStudy']['write_access'] = write_access
        response['mtblsStudy']['is_curator'] = is_curator
        response['mtblsStudy']['revisionNumber'] = study.revision_number
        response['mtblsStudy']['revisionDatetime'] = study.revision_datetime.isoformat() if study.revision_datetime else ""
        response['mtblsStudy']['revisionStatus'] = revision_status
        response['mtblsStudy']['revisionComment'] = revision_comment
        response['mtblsStudy']['revisionTaskMessage'] = revision_task_message

        response['mtblsStudy']['studyHttpUrl'] = http_url
        response['mtblsStudy']['studyFtpUrl'] = ftp_url
        response['mtblsStudy']['studyGlobusUrl'] = globus_url
        response['mtblsStudy']['studyAsperaPath'] = aspera_path
        
        # ToDo: Make sure this date is formatted YYYY-MM-DD and update the isa_inv, isa_study before returning
        # response['mtblsStudy']['release_date'] = release_date
        # isa_inv.public_release_date = release_date
        # isa_study.public_release_date = release_date
        if study_status == "In Review":
            response['mtblsStudy']['reviewer_link'] = 'reviewer' + obfuscation_code
        response['isaInvestigation'] = IsaInvestigationSchema().dump(isa_inv).data
        response['validation']['errors'] = []
        response['validation']['warnings'] = []

        return response

    @swagger.operation(
        summary="Update Study",
        notes='''Update Study. </p><pre><code>
This is a rather complex object to describe here. 
Please use the GET method above to retrieve the structure of your study prior to submitting this PUT operation.
        </pre></code>''',
        parameters=[
            {
                "name": "study",
                "description": "Study in ISA-JSON format",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
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
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            # user token is required
            abort(401, message="Study does not exist or your do not have access to this study. Please provide a valid user_token")

        # check for keeping copies
        save_audit_copy = False
        save_msg_str = "NOT be"
        if "save_audit_copy" in request.headers and \
                request.headers["save_audit_copy"].lower() == 'true':
            save_audit_copy = True
            save_msg_str = "be"

        # body content validation
        updated_inv = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['investigation']
            # if partial=True missing fields will be ignored
            result = IsaInvestigationSchema().load(data, partial=True)
            updated_inv = result.data
        except (ValidationError, Exception) as err:
            for arg in err.args:
                print(arg)
            abort(400)

        # update Study details
        logger.info('Updating Study Publication details for %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=False,
                                                         study_location=study_location)

        isa_inv = updated_inv

        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path,
                            save_investigation_copy=save_audit_copy,
                            save_samples_copy=save_audit_copy,
                            save_assays_copy=save_audit_copy)
        logger.info('Updated %s', updated_inv.title)

        sch = IsaInvestigationSchema()
        sch.context['investigation'] = Investigation()
        return sch.dump(updated_inv)
