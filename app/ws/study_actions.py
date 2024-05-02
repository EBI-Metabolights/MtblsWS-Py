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
import os

from flask import current_app as app
from flask import request
from flask_restful import Resource, abort
from flask_restful_swagger import swagger
from app.config import get_settings
from app.ws.db import types

from app.services.storage_service.acl import Acl
from app.services.storage_service.storage_service import StorageService
from app.utils import metabolights_exception_handler, MetabolightsException
from app.ws.db.types import CurationRequest, UserRole
from app.ws.db_connection import update_curation_request, update_modification_time, update_study_status, update_study_status_change_date
from app.ws.ftp.ftp_utils import get_ftp_folder_access_status, toogle_ftp_folder_permission
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.study.study_service import StudyService
from app.ws.study.user_service import UserService
from app.ws.validation_utils import ValidationReportFile, get_validation_report
from isatools.model import Study
logger = logging.getLogger('wslog')

# MetaboLights (Java-Based) WebService client
wsc = WsClient()
iac = IsaApiClient()


class StudyModificationTime(Resource):
    @swagger.operation(
        summary="Change study update date and status date",
        nickname="Change study update date and status date",
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
                "message": "OK."
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
            abort(404, message='Please provide valid parameter for study identifier')
        
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        UserService.get_instance().validate_user_has_curator_role(user_token)
        try:
            current_time_value = datetime.datetime.now(datetime.timezone.utc)
            update_modification_time(study_id, current_time_value)
            update_study_status_change_date(study_id, current_time_value)
            return {"Success": f"Modification time of study {study_id} is now {current_time_value.isoformat()}"}
        except Exception as exc:
            return {"Error": f"Modification time of study {study_id} is not updated."}

class StudyStatus(Resource):
    @swagger.operation(
        summary="Change study status",
        nickname="Change study status",
        notes='''Change study status from 'Submitted' to 'In Curation'.<br>
        Please note a *minimum* of 28 days is required for curation, this will be added to the release date</p>
                <pre><code>Curators can change status to any of: 'Submitted', 'In Curation', 'In Review', 'Public' or 'Dormant'. curation_request is optional and can get the values: 'Manual Curation', 'No Curation', 'Semi-automated Curation'
                <p>Example: { "status": "In Curation" } { "status": "Public" }   {"status": "Public", "curation_request": "No Curation"}
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
            raise MetabolightsException(message='Please provide valid parameter for study identifier')

        study_status: str = None
        curation_request_str: str = None
        try:
            
            data_dict = json.loads(request.data.decode('utf-8'))
            study_status = data_dict['status']
            if 'curation_request' in data_dict:
                curation_request_str = data_dict['curation_request']
        except Exception as ex:
            pass
        

        if not study_status or study_status.upper() not in [x.upper() for x in ['Submitted', 'In Curation', 'In Review', 'Public', 'Dormant']]:
            raise MetabolightsException(message="Please provide study status: 'Submitted', 'In Curation', 'In Review', 'Public' or 'Dormant'")

        if curation_request_str and  curation_request_str.upper() not in [x.upper() for x in ['Manual Curation', 'No Curation', 'Semi-automated Curation']]:
            raise MetabolightsException(message="Please provide curation request: 'Manual Curation', 'No Curation' or 'Semi-automated Curation'")

        curation_request = CurationRequest.from_name(curation_request_str) if curation_request_str else None
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        study = StudyService.get_instance().get_study_by_acc(study_id)
        db_user = UserService.get_instance().get_db_user_by_user_token(user_token)
        is_curator = db_user.role == UserRole.ROLE_SUPER_USER.value
        obfuscation_code = study.obfuscationcode
        db_study_status =  types.StudyStatus.from_int(study.status).name
        release_date = study.releasedate.strftime('%Y-%m-%d')
        # check for access rights
        # _, _, _, _, _, _, _, _ = wsc.get_permissions(study_id, user_token)
        study_location = os.path.join(get_settings().study.mounted_paths.study_metadata_files_root_path, study_id)
        
        status_updated = False if study_status.replace(" ", "").upper() == db_study_status.upper() else True
        current_time = datetime.datetime.now(datetime.timezone.utc)
        #     raise MetabolightsException(message=f"Status is already {str(study_status)} so there is nothing to change")
        ftp_private_storage = StorageService.get_ftp_private_storage()
        ftp_private_study_folder = study_id.lower() + '-' + obfuscation_code
        if status_updated:
        # Update the last status change date field
            status_date_logged = update_study_status_change_date(study_id, current_time)
            if not status_date_logged:
                logger.error("Could not update the status_date column for " + study_id)
        inv_file_path = os.path.join(study_location, get_settings().study.investigation_file_name)
        if not os.path.exists(inv_file_path):
            raise MetabolightsException(message="There is no investigation file.")

        isa_study_item, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)
        isa_study: Study = isa_study_item
        if is_curator:  # Curators can change the date to current date, submitters can not!
            new_date = current_time
        else:
            new_date = current_time + datetime.timedelta(+28)
        new_date = new_date.strftime('%Y-%m-%d')

        if is_curator:  # User is a curator, so just update status without any further checks
            if status_updated:
                if study_status.lower() == 'public':
                    isa_inv.public_release_date = new_date
                    isa_study.public_release_date = new_date
                    release_date = new_date
                self.update_status(study_id, study_status, is_curator=is_curator,
                                obfuscation_code=obfuscation_code, user_token=user_token)
            update_curation_request(study_id, curation_request)
        else:
            if db_study_status.lower() != 'submitted':  # and study_status != 'In Curation':                
                raise MetabolightsException(http_code=403, message="You can not change the study to this status")
            
            validation_report: ValidationReportFile = get_validation_report(study_id=study_id)

            if validation_report.validation.status in ("success", "warning", "info"):
                self.update_status(study_id, study_status, is_curator=is_curator,
                                   obfuscation_code=obfuscation_code, user_token=user_token)

                if release_date < new_date:  # Set the release date to a minimum of 28 days in the future
                    isa_inv.public_release_date = new_date
                    isa_study.public_release_date = new_date
                    release_date = new_date
            else:
                if validation_report.validation.status == "not ready":
                    raise MetabolightsException(http_code=403, message="Study has not been validated yet. Validate your study, fix any problems before attempting to change study status.")
                else:
                    raise MetabolightsException(http_code=403, message="There are validation errors. Fix any problems before attempting to change study status.")

        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=True)

        status, message = wsc.reindex_study(study_id, user_token)
        # Explictly changing the FTP folder permission for In Curation and Submitted state
        if db_study_status.lower() != study_status.lower():
            if study_status.lower() == 'in curation':
                ftp_private_storage.remote.update_folder_permission(ftp_private_study_folder, Acl.AUTHORIZED_READ)

            if study_status.lower() == 'submitted':
                ftp_private_storage.remote.update_folder_permission(ftp_private_study_folder, Acl.AUTHORIZED_READ_WRITE)
            if curation_request is not None:
                return {"Success": "Status updated from '" + db_study_status + "' to '" + study_status + "'",
                    "release-date": release_date, "curation_request": curation_request_str}
            return {"Success": "Status updated from '" + db_study_status + "' to '" + study_status + "'",
                    "release-date": release_date}
        else:
            if curation_request is not None:
                return {"Success": f"Status updated to {study_status}",
                        "release-date": release_date, "curation_request": curation_request_str}
            return {"Success": f"Status updated to {study_status}", "release-date": release_date}


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
            abort(404, message='Please provide valid parameter for study identifier')

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
            abort(404, message='Please provide valid parameter for study identifier')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        return get_ftp_folder_access_status(app, study_id)
