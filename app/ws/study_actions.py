#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Jan-09
#  Modified by:   kenneth
#
#  Copyright 2020 EMBL - European Bioinformatics Institute
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

import datetime
import json
import logging

from flask import current_app as app
from flask import request, abort
from flask_restful import Resource
from flask_restful_swagger import swagger

from app.services.storage_service.acl import Acl
from app.services.storage_service.storage_service import StorageService
from app.utils import metabolights_exception_handler, MetabolightsException
from app.ws.db_connection import update_study_status, update_study_status_change_date
from app.ws.ftp.ftp_utils import get_ftp_folder_access_status, toogle_ftp_folder_permission
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.study.user_service import UserService
from app.ws.study.validation.commons import validate_study
from app.ws.validation_utils import ValidationReport, get_validation_report

logger = logging.getLogger('wslog')

# MetaboLights (Java-Based) WebService client
wsc = WsClient()
iac = IsaApiClient()


class StudyStatus(Resource):
    @swagger.operation(
        summary="Change study status",
        nickname="Change study status",
        notes='''Change study status from 'Submitted' to 'In Curation'.<br>
        Please note a *minimum* of 28 days is required for curation, this will be added to the release date</p>
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
    @metabolights_exception_handler
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
            raise MetabolightsException(message=f"Status is already {str(study_status)} so there is nothing to change")
        ftp_private_storage = StorageService.get_ftp_private_storage()
        ftp_private_study_folder = study_id.lower() + '-' + obfuscation_code

        # Update the last status change date field
        status_date_logged = update_study_status_change_date(study_id)
        if not status_date_logged:
            logger.error("Could not update the status_date column for " + study_id)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

        if is_curator:  # Curators can change the date to current date, submitters can not!
            new_date = datetime.datetime.now()
        else:
            new_date = datetime.datetime.now() + datetime.timedelta(+28)
        new_date = new_date.strftime('%Y-%m-%d')

        if is_curator:  # User is a curator, so just update status without any further checks
            if study_status.lower() == 'public':
                isa_inv.public_release_date = new_date
                isa_study.public_release_date = new_date
                release_date = new_date
            self.update_status(study_id, study_status, is_curator=is_curator,
                               obfuscation_code=obfuscation_code, user_token=user_token)
        elif write_access:
            if db_study_status.lower() != 'submitted':  # and study_status != 'In Curation':
                abort(403, "You can not change the study to this status")
            validation_report: ValidationReport = get_validation_report()

            if validation_report.validation.status in ("success", "warning", "info"):
                self.update_status(study_id, study_status, is_curator=is_curator,
                                   obfuscation_code=obfuscation_code, user_token=user_token)

                if release_date < new_date:  # Set the release date to a minimum of 28 days in the future
                    isa_inv.public_release_date = new_date
                    isa_study.public_release_date = new_date
                    release_date = new_date
            else:
                if validation_report.validation.status == "not ready":
                    abort(403, "Study has not been validated yet. Validate your study, fix any problems before attempting to change study status.")
                else:
                    abort(403, "There are validation errors. Fix any problems before attempting to change study status.")
        else:
            abort(403, "You do not have rights to change the status for this study")

        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=True)

        status, message = wsc.reindex_study(study_id, user_token)
        # Explictly changing the FTP folder permission for In Curation and Submitted state
        if db_study_status.lower() != study_status.lower():
            if study_status.lower() == 'in curation':
                ftp_private_storage.remote.update_folder_permission(ftp_private_study_folder, Acl.AUTHORIZED_READ)

            if study_status.lower() == 'submitted':
                ftp_private_storage.remote.update_folder_permission(ftp_private_study_folder, Acl.AUTHORIZED_READ_WRITE)

            return {"Success": "Status updated from '" + db_study_status + "' to '" + study_status + "'",
                    "release-date": release_date}
        else:
            return {"Success": f"Status updated to {study_status}",
                    "release-date": release_date}

    @staticmethod
    def update_status(study_id, study_status, is_curator=False, obfuscation_code=None, user_token=None):
        study_status = study_status.lower()
        # Update database
        update_study_status(study_id, study_status, is_curator=is_curator)

    # @staticmethod
    # def get_study_validation_status(study_id, study_location, user_token, obfuscation_code):
    #     validates = validate_study(study_id, study_location, user_token, obfuscation_code, log_category='error')
    #     validations = validates['validation']
    #     status = validations['status']

    #     if status != 'error':
    #         return True

    #     return False


class ToggleAccess(Resource):
    @swagger.operation(
        summary="[Deprecated] Change FTP study folder permission",
        nickname="Change FTP study permission",
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
                "message": "OK. FTP folder permission toggled "
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
    def put(self, study_id):

        # param validation
        if study_id is None:
            abort(404, 'Please provide valid parameter for study identifier')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        return toogle_ftp_folder_permission(app, study_id)

class ToggleAccessGet(Resource):
    @swagger.operation(
        summary="[Deprecated] Get Study FTP folder permission",
        nickname="Get FTP study permission",
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
                "message": "OK. FTP folder permission returned"
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

        # param validation
        if study_id is None:
            abort(404, 'Please provide valid parameter for study identifier')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        return get_ftp_folder_access_status(app, study_id)
