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
import glob
import json
import logging
import os
import pathlib
import re
import shutil
import time
from typing import Dict, Tuple

from flask import current_app as app
from flask import request
from flask_restful import Resource, abort
from flask_restful_swagger import swagger
from isatools.model import Study, Assay
from pydantic import BaseModel

from app.config import get_settings
from app.config.utils import get_private_ftp_relative_root_path
from app.services.storage_service.acl import Acl
from app.services.storage_service.storage_service import StorageService
from app.tasks.common_tasks.basic_tasks.send_email import (
    get_principal_investigator_emails,
    send_email_for_new_accession_number,
    send_email_on_public,
)
from app.tasks.common_tasks.curation_tasks.study_revision import sync_study_metadata_folder
from app.tasks.datamover_tasks.basic_tasks.study_folder_maintenance import (
    create_links_on_data_storage,
    rename_folder_on_private_storage,
)
from app.utils import (
    MetabolightsException,
    current_time,
    metabolights_exception_handler,
)
from app.ws.db import schemes, types
from app.ws.db.types import CurationRequest, UserRole
from app.ws.db_connection import (
    reserve_mtbls_accession,
    update_curation_request,
    update_modification_time,
    update_study_id_from_mtbls_accession,
    update_study_status,
    update_study_status_change_date,
)
from app.ws.elasticsearch.elastic_service import ElasticsearchService
from app.ws.folder_maintenance import StudyFolderMaintenanceTask
from app.ws.ftp.ftp_utils import (
    get_ftp_folder_access_status,
    toogle_ftp_folder_permission,
)
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.study.study_service import StudyService
from app.ws.study.user_service import UserService

logger = logging.getLogger("wslog")

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
        ],
    )
    @metabolights_exception_handler
    def put(self, study_id):
        # param validation
        if study_id is None:
            abort(404, message="Please provide valid parameter for study identifier")

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        UserService.get_instance().validate_user_has_curator_role(user_token)
        try:
            current_time_value = current_time()
            update_modification_time(study_id, current_time_value)
            update_study_status_change_date(study_id, current_time_value)
            return {
                "Success": f"Modification time of study {study_id} is now {current_time_value.isoformat()}"
            }
        except Exception:
            return {"Error": f"Modification time of study {study_id} is not updated."}


class ValidationResultFile(BaseModel):
    validation_time: str = ""
    task_id: str = ""


class StudyCurationType(Resource):
    @swagger.operation(
        summary="Change study curation type (Manual Curation, No Curation) or (MetaboLights, Minimum)",
        nickname="Change study status",
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
                "name": "curation-type",
                "description": "The status to change a study to",
                "paramType": "header",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
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
            {
                "code": 200,
                "message": "OK. The Metabolite Annotation File (MAF) is returned",
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
        ],
    )
    @metabolights_exception_handler
    def put(self, study_id: str):
        # param validation
        if study_id is None:
            raise MetabolightsException(
                message="Please provide valid parameter for study identifier"
            )

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_curator_role(user_token, study_id)

        # User authentication
        curation_type_str = None
        if "curation_type" in request.headers:
            curation_type_str = request.headers["curation_type"]
        
        if curation_type_str and curation_type_str.upper() not in [
            x.upper()
            for x in ["Manual Curation", "No Curation", "MetaboLights", "Minimum"]
        ]:
            raise MetabolightsException(
                message="Please provide curation request: 'Manual Curation', 'No Curation' or 'Semi-automated Curation'"
            )
        curation_type = CurationRequest.NO_CURATION
        if curation_type_str and curation_type_str.upper() in [
            x.upper()
            for x in ["Manual Curation", "MetaboLights"]
        ]:
            curation_type = CurationRequest.MANUAL_CURATION

        
        update_curation_request(study_id, curation_type)
        
        return {"curation_type": curation_type.name}

class StudyStatus(Resource):
    @swagger.operation(
        summary="Change study status",
        nickname="Change study status",
        notes="""Change study status from 'Provisional' to 'Private' or 'Private' to 'Provisional'.<br>
        Please note a *minimum* of 28 days is required for curation, this will be added to the release date</p>
                <pre><code>Curators can change status to any of: 'Provisional', 'Private', 'In Review', 'Public' or 'Dormant'. curation_request is optional and can get the values: 'Manual Curation', 'No Curation', 'Semi-automated Curation'
                <p>Example: { "status": "Private" }   {"status": "Private", "curation_request": "No Curation"}
                </code></pre>""",
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
                "name": "study_status",
                "description": "The status to change a study to",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
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
            {
                "code": 200,
                "message": "OK. The Metabolite Annotation File (MAF) is returned",
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
        ],
    )
    @metabolights_exception_handler
    def put(self, study_id: str):
        # param validation
        if study_id is None:
            raise MetabolightsException(
                message="Please provide valid parameter for study identifier"
            )

        study_status: str = ""
        curation_request_str: str = None
        try:
            data_dict = json.loads(request.data.decode("utf-8"))
            study_status = data_dict["status"]
            if "curation_request" in data_dict:
                curation_request_str = data_dict["curation_request"]
        except Exception:
            pass
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if not study_status or study_status.upper() not in [
            x.upper()
            for x in ["Provisional", "Private", "In Review", "Public", "Dormant"]
        ]:
            raise MetabolightsException(
                message="Please provide study status: 'provisional', 'Private', 'In Review', 'Public' or 'Dormant'"
            )

        if curation_request_str and curation_request_str.upper() not in [
            x.upper()
            for x in ["Manual Curation", "No Curation", "Semi-automated Curation"]
        ]:
            raise MetabolightsException(
                message="Please provide curation request: 'Manual Curation', 'No Curation' or 'Semi-automated Curation'"
            )
        UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        
        return self.update_status_m2(user_token=user_token, study_id = study_id, study_status=study_status, curation_request_str=curation_request_str)
        # Enable for Milestone 2
        # return self.update_status_m1(user_token=user_token, study_id = study_id, study_status=study_status, curation_request_str=curation_request_str)

    def update_status_m1(self, user_token: str, study_id: str, study_status: str, curation_request_str: str):
        curation_request = (
            CurationRequest.from_name(curation_request_str)
            if curation_request_str
            else None
        )

        study = StudyService.get_instance().get_study_by_acc(study_id)
        db_user = UserService.get_instance().get_db_user_by_user_token(user_token)
        is_curator = db_user.role == UserRole.ROLE_SUPER_USER.value
        obfuscation_code = study.obfuscationcode
        db_study_status = types.StudyStatus.from_int(study.status).name
        release_date = study.releasedate.strftime("%Y-%m-%d")
        first_public_date_baseline: datetime.datetime = study.first_public_date
        first_private_date_baseline: datetime.datetime = study.first_private_date
        # check for access rights
        # _, _, _, _, _, _, _, _ = wsc.get_permissions(study_id, user_token)
        study_location = os.path.join(
            get_settings().study.mounted_paths.study_metadata_files_root_path, study_id
        )

        status_updated = (
            False
            if study_status.replace(" ", "").upper() == db_study_status.upper()
            else True
        )
        now = current_time()
        #     raise MetabolightsException(message=f"Status is already {str(study_status)} so there is nothing to change")
        ftp_private_storage = StorageService.get_ftp_private_storage()
        ftp_private_study_folder = study_id.lower() + "-" + obfuscation_code
        if status_updated:
            # Update the last status change date field
            status_date_logged = update_study_status_change_date(study_id, now)
            if not status_date_logged:
                logger.error("Could not update the status_date column for " + study_id)
        inv_file_path = os.path.join(
            study_location, get_settings().study.investigation_file_name
        )
        if not os.path.exists(inv_file_path):
            raise MetabolightsException(message="There is no investigation file.")

        isa_study_item, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )
        isa_study: Study = isa_study_item
        if study_status.lower() in {"public", "in review", "private"}:
            updated_submission_date = (
                first_private_date_baseline.strftime("%Y-%m-%d")
                if first_private_date_baseline
                else isa_inv.submission_date
            )
            isa_inv.submission_date = updated_submission_date
            isa_study.submission_date = updated_submission_date
    
        new_date = now.strftime("%Y-%m-%d")
        if study_status.lower() == "public":
            isa_inv.public_release_date = new_date
            isa_study.public_release_date = new_date
            submission = study.first_private_date.strftime("%Y-%m-%d") if study.first_private_date else study.submissiondate.strftime("%Y-%m-%d")
            isa_inv.submission_date = submission
            isa_study.submission_date = submission
            release_date = new_date
            
        if (
            is_curator
        ):  # User is a curator, so just update status without any further checks
            if status_updated:
                self.update_status(
                    study_id,
                    study_status,
                    is_curator=is_curator,
                    obfuscation_code=obfuscation_code,
                    user_token=user_token,
                    first_public_date=first_public_date_baseline,
                    first_private_date=first_private_date_baseline,
                )
            update_curation_request(study_id, curation_request)
        else:
            if not status_updated:
                raise MetabolightsException(
                            http_code=403,
                            message="Current status and requested status are same.",
                        )
            
            if db_study_status.lower() in {"public"}:
                raise MetabolightsException(
                            http_code=403,
                            message="Public studies can not be updated.",
                        )
            validated = True
            message =""
            status_levels = {"dormant": 0, "provisional": 0, "private": 1, "in review": 1, "public": 2} 
            if status_levels[study_status.lower()] > status_levels[db_study_status.lower()]:
                validated, message = self.has_validated(study_id)
            # validation_report: ValidationReportFile = get_validation_report(study_id=study_id)
            if not validated:
                if "not ready" in message:
                    raise MetabolightsException(
                        http_code=403,
                        message="Please run validation and fix any problems before attempting to change study status.",
                    )
                elif "Metadata files are updated" in message:
                   raise MetabolightsException(
                        http_code=403,
                        message="Metadata files are updated after validation. Please re-run validation and fix any issues before attempting to change study status.",
                    )
                else:
                    raise MetabolightsException(
                        http_code=403,
                        message="There are validation errors in the latest validation report. Please fix any issues before attempting to change study status.",
                    )
                    
            self.update_status(
                study_id,
                study_status,
                is_curator=is_curator,
                obfuscation_code=obfuscation_code,
                user_token=user_token,
                first_public_date=first_public_date_baseline,
                first_private_date=first_private_date_baseline,
            )


        current_study_status = types.StudyStatus.from_int(study.status)
        requested_study_status = types.StudyStatus.from_name(study_status.upper())

        updated_study_id = self.update_db_study_id(
            study_id,
            current_study_status,
            requested_study_status,
            study.reserved_accession,
        )
        study = StudyService.get_instance().get_study_by_acc(updated_study_id)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=True, save_assays_copy=True, save_samples_copy=True)

        if study_id != updated_study_id:
            self.refactor_study_folder(
                study, study_location, user_token, study_id, updated_study_id
            )
            ElasticsearchService.get_instance()._delete_study_index(
                study_id, ignore_errors=True
            )
            ftp_private_study_folder = updated_study_id.lower() + "-" + obfuscation_code
            if updated_study_id.startswith(
                get_settings().study.accession_number_prefix
            ):
                study_title = isa_study.title
                additional_cc_emails = get_principal_investigator_emails(isa_study)
                inputs = {
                    "user_token": user_token,
                    "provisional_id": study_id,
                    "study_id": updated_study_id,
                    "obfuscation_code": obfuscation_code,
                    "study_title": study_title,
                    "release_date": release_date,
                    "additional_cc_emails": additional_cc_emails,
                }
                send_email_for_new_accession_number.apply_async(kwargs=inputs)
        ElasticsearchService.get_instance()._reindex_study(updated_study_id, user_token)
    
        current_curation_request = CurationRequest(study.curation_request)
        current_status = types.StudyStatus(study.status)
        ftp_private_relative_root_path = get_private_ftp_relative_root_path()
        ftp_private_folder_path = os.path.join(
            ftp_private_relative_root_path, ftp_private_study_folder
        )

        response = {
            "release-date": release_date,
            "curation_request": current_curation_request.to_camel_case_str(),
            "assigned_study_id": updated_study_id,
            "assigned_status": current_status.to_camel_case_str(),
            "assigned_status_code": current_status.value,
            "curation_request_code": current_curation_request,
            "ftp_folder_path": ftp_private_folder_path,
            "obfuscation_code": obfuscation_code,
            "study_table_id": study.id,
        }
        # Explictly changing the FTP folder permission for Private and Provisional state
        if db_study_status.lower() != study_status.lower():
            if study_status.lower() in (
                "private",
                "public",
                "in review",
                "dormant",
            ):
                if ftp_private_storage.remote.does_folder_exist(
                    ftp_private_study_folder
                ):
                    ftp_private_storage.remote.update_folder_permission(
                        ftp_private_study_folder, Acl.AUTHORIZED_READ
                    )

            if study_status.lower() == "provisional":
                if ftp_private_storage.remote.does_folder_exist(
                    ftp_private_study_folder
                ):
                    ftp_private_storage.remote.update_folder_permission(
                        ftp_private_study_folder, Acl.AUTHORIZED_READ_WRITE
                    )

            if study_status.lower() == "public" and not first_public_date_baseline:
                release_date = study.releasedate
                inputs = {
                    "user_token": user_token,
                    "study_id": updated_study_id,
                    "release_date": new_date,
                }
                send_email_on_public.apply_async(kwargs=inputs)
                sync_study_metadata_folder.apply_async(kwargs={"study_id": study_id, "user_token": user_token})

            response.update(
                {
                    "Success": "Status updated from '"
                    + db_study_status
                    + "' to '"
                    + study_status
                    + "'"
                }
            )
            return response
        else:
            response.update(
                {
                    "Success": "Status updated from '"
                    + db_study_status
                    + "' to '"
                    + study_status
                    + "'"
                }
            )
            return response

    def update_status_m2(self, user_token: str, study_id: str, study_status: str, curation_request_str: str):
        if study_status.upper() == "PUBLIC":
            raise MetabolightsException(
                message="Please use the 'revisions' endpoint to release a study"
            )
        curation_request = (
            CurationRequest.from_name(curation_request_str)
            if curation_request_str
            else None
        )
        study = StudyService.get_instance().get_study_by_acc(study_id)
        db_user = UserService.get_instance().get_db_user_by_user_token(user_token)
        is_curator = db_user.role == UserRole.ROLE_SUPER_USER.value
        obfuscation_code = study.obfuscationcode
        db_study_status = types.StudyStatus.from_int(study.status).name
        release_date = study.releasedate.strftime("%Y-%m-%d")
        first_public_date_baseline: datetime.datetime = study.first_public_date
        first_private_date_baseline: datetime.datetime = study.first_private_date
        # check for access rights
        # _, _, _, _, _, _, _, _ = wsc.get_permissions(study_id, user_token)
        study_location = os.path.join(
            get_settings().study.mounted_paths.study_metadata_files_root_path, study_id
        )
        status_updated = (
            False
            if study_status.replace(" ", "").upper() == db_study_status.upper()
            else True
        )
        now = current_time()
        #     raise MetabolightsException(message=f"Status is already {str(study_status)} so there is nothing to change")
        ftp_private_storage = StorageService.get_ftp_private_storage()
        ftp_private_study_folder = study_id.lower() + "-" + obfuscation_code
        if status_updated:
            # Update the last status change date field
            status_date_logged = update_study_status_change_date(study_id, now)
            if not status_date_logged:
                logger.error("Could not update the status_date column for " + study_id)
        inv_file_path = os.path.join(
            study_location, get_settings().study.investigation_file_name
        )
        if not os.path.exists(inv_file_path):
            raise MetabolightsException(message="There is no investigation file.")

        isa_study_item, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )
        isa_study: Study = isa_study_item
        if study_status.lower() in {"public", "in review", "private"}:
            updated_submission_date = (
                first_private_date_baseline.strftime("%Y-%m-%d")
                if first_private_date_baseline
                else isa_inv.submission_date
            )
            isa_inv.submission_date = updated_submission_date
            isa_study.submission_date = updated_submission_date

        new_date = now.strftime("%Y-%m-%d")
        if study_status.lower() == "public":
            isa_inv.public_release_date = new_date
            isa_study.public_release_date = new_date
            submission = study.first_private_date.strftime("%Y-%m-%d") if study.first_private_date else study.submissiondate.strftime("%Y-%m-%d")
            isa_inv.submission_date = submission
            isa_study.submission_date = submission
            release_date = new_date
        if (
            is_curator
        ):  # User is a curator, so just update status without any further checks
            if status_updated:
                if study_status.lower() == "public":
                    isa_inv.public_release_date = new_date
                    isa_study.public_release_date = new_date
                    release_date = new_date
                self.update_status(
                    study_id,
                    study_status,
                    is_curator=is_curator,
                    obfuscation_code=obfuscation_code,
                    user_token=user_token,
                    first_public_date=first_public_date_baseline,
                    first_private_date=first_private_date_baseline,
                )
            update_curation_request(study_id, curation_request)
        else:
            if not status_updated:
                raise MetabolightsException(
                            http_code=403,
                            message="Current status and requested status are same.",
                        )
            if db_study_status.lower() in {"public"}:
                raise MetabolightsException(
                            http_code=403,
                            message="Public studies can not be updated.",
                        )
            logger.debug(f"Current status: {db_study_status.lower()}, Requested status: {study_status.lower()}")
            status_levels = {"dormant": 0, "provisional": 0, "private": 1, "in review": 1, "public": 2} 
            if status_levels[study_status.lower()] > status_levels[db_study_status.lower()]:
                validated, message = self.has_validated(study_id)
                if not validated:
                    if "not ready" in message:
                        raise MetabolightsException(
                            http_code=403,
                            message="Please run validation and fix any problems before attempting to change study status.",
                        )
                    elif "Metadata files are updated" in message:
                        raise MetabolightsException(
                                http_code=403,
                                message="Metadata files are updated after validation. Please re-run validation and fix any issues before attempting to change study status.",
                            )
                    else:
                        raise MetabolightsException(
                            http_code=403,
                            message="There are validation errors in the latest validation report. Please fix any issues before attempting to change study status.",
                        )
                    
            self.update_status(
                study_id,
                study_status,
                is_curator=is_curator,
                obfuscation_code=obfuscation_code,
                user_token=user_token,
                first_public_date=first_public_date_baseline,
                first_private_date=first_private_date_baseline,
            )
        current_study_status = types.StudyStatus.from_int(study.status)
        requested_study_status = types.StudyStatus.from_name(study_status.upper())

        updated_study_id = self.update_db_study_id(
            study_id,
            current_study_status,
            requested_study_status,
            study.reserved_accession,
        )

        if study_id != updated_study_id:
            iac.write_isa_study(isa_inv, user_token, study_location, save_investigation_copy=True, save_assays_copy=True, save_samples_copy=True)

            self.refactor_study_folder(
                study, study_location, user_token, study_id, updated_study_id
            )
            ElasticsearchService.get_instance()._delete_study_index(
                study_id, ignore_errors=True
            )
            
            ftp_private_study_folder = updated_study_id.lower() + "-" + obfuscation_code
            if updated_study_id.startswith(
                get_settings().study.accession_number_prefix
            ):
                study_title = isa_study.title
                inputs = {
                    "user_token": user_token,
                    "provisional_id": study_id,
                    "study_id": updated_study_id,
                    "obfuscation_code": obfuscation_code,
                    "study_title": study_title,
                    "release_date": release_date,
                }
                send_email_for_new_accession_number.apply_async(kwargs=inputs)
        ElasticsearchService.get_instance()._reindex_study(updated_study_id, user_token)
        study = StudyService.get_instance().get_study_by_acc(updated_study_id)
        current_curation_request = CurationRequest(study.curation_request)
        current_status = types.StudyStatus(study.status)
        ftp_private_relative_root_path = get_private_ftp_relative_root_path()
        ftp_private_folder_path = os.path.join(
            ftp_private_relative_root_path, ftp_private_study_folder
        )

        response = {
            "release-date": release_date,
            "curation_request": current_curation_request.to_camel_case_str(),
            "assigned_study_id": updated_study_id,
            "assigned_status": current_status.to_camel_case_str(),
            "assigned_status_code": current_status.value,
            "curation_request_code": current_curation_request,
            "ftp_folder_path": ftp_private_folder_path,
            "obfuscation_code": obfuscation_code,
            "study_table_id": study.id,
        }
        # Explictly changing the FTP folder permission for Private and Provisional state
        if db_study_status.lower() != study_status.lower():
            if study_status.lower() in (
                "private",
                "public",
                "in review",
                "dormant",
            ):
                if ftp_private_storage.remote.does_folder_exist(
                    ftp_private_study_folder
                ):
                    ftp_private_storage.remote.update_folder_permission(
                        ftp_private_study_folder, Acl.AUTHORIZED_READ
                    )

            if study_status.lower() == "provisional":
                if ftp_private_storage.remote.does_folder_exist(
                    ftp_private_study_folder
                ):
                    ftp_private_storage.remote.update_folder_permission(
                        ftp_private_study_folder, Acl.AUTHORIZED_READ_WRITE
                    )

            # if study_status.lower() == "public" and not first_public_date_baseline:
            #     release_date = study.releasedate
            #     inputs = {
            #         "user_token": user_token,
            #         "study_id": updated_study_id,
            #         "release_date": new_date,
            #     }
            #     send_email_on_public.apply_async(kwargs=inputs)

            response.update(
                {
                    "Success": "Status updated from '"
                    + db_study_status
                    + "' to '"
                    + study_status
                    + "'"
                }
            )
            return response
        else:
            response.update(
                {
                    "Success": "Status updated from '"
                    + db_study_status
                    + "' to '"
                    + study_status
                    + "'"
                }
            )
            return response

    def get_validation_summary_result_files_from_history(
        self, study_id: str
    ) -> Dict[str, Tuple[str, ValidationResultFile]]:
        internal_files_root_path = pathlib.Path(
            get_settings().study.mounted_paths.study_internal_files_root_path
        )
        files = {}
        validation_history_path: pathlib.Path = internal_files_root_path / pathlib.Path(
            f"{study_id}/validation-history"
        )
        validation_history_path.mkdir(exist_ok=True)
        result = [
            x for x in validation_history_path.glob("validation-history__*__*.json")
        ]
        for item in result:
            match = re.match(r"(.*)validation-history__(.+)__(.+).json$", str(item))
            if match:
                groups = match.groups()
                definition = ValidationResultFile(
                    validation_time=groups[1], task_id=groups[2]
                )
                files[groups[2]] = (item, definition)
        return files

    def get_all_metadata_files(self, study_metadata_files_path: str):
        metadata_files = []
        if not os.path.exists(study_metadata_files_path):
            return metadata_files
        patterns = ["a_*.txt", "s_*.txt", "i_*.txt", "m_*.tsv"]
        for pattern in patterns:
            metadata_files.extend(
                glob.glob(
                    os.path.join(study_metadata_files_path, pattern), recursive=False
                )
            )
        return metadata_files

    def get_validation_overrides(self, study_id: str) -> Dict[str, str]:
        internal_files_root_path = pathlib.Path(
            get_settings().study.mounted_paths.study_internal_files_root_path
        )
        validation_overrides_folder_path: pathlib.Path = (
            internal_files_root_path / pathlib.Path(f"{study_id}/validation-overrides")
        )
        target_path = validation_overrides_folder_path / pathlib.Path(
            "validation-overrides.json"
        )
        if target_path.exists():
            try:
                with open(target_path, "r") as f:
                    validations_obj = json.load(f)
                if validations_obj:
                    overrides = validations_obj["validation_overrides"]
                    override_summary = {}
                    for override in overrides:
                        if override["enabled"]:
                            override_summary[override["rule_id"]] = override["new_type"]
                    return override_summary
            except Exception as exc:
                logger.error(str(exc))
        return {}

    def has_validated(self, study_id: str) -> Tuple[bool, str]:
        if not study_id:
            return None, "study_id is not valid."
        metadata_root_path = (
            get_settings().study.mounted_paths.study_metadata_files_root_path
        )
        study_path = os.path.join(metadata_root_path, study_id)
        metadata_files = self.get_all_metadata_files(study_path)
        last_modified = -1
        for file in metadata_files:
            modified_time = os.path.getmtime(file)
            if modified_time > last_modified:
                last_modified = modified_time

        result: Dict[str, Tuple[str, ValidationResultFile]] = (
            self.get_validation_summary_result_files_from_history(study_id)
        )

        result_file = ""
        validation_time = ""
        if result:
            try:
                sorted_result = [result[x] for x in result]
                sorted_result.sort(
                    key=lambda x: x[1].validation_time if x and x[1] else "",
                    reverse=True,
                )
                latest_validation = sorted_result[0]

                result_file = latest_validation[0]

                content = json.loads(
                    pathlib.Path(result_file).read_text(encoding="utf-8")
                )
                start_time = datetime.datetime.fromisoformat(
                    content["start_time"]
                ).timestamp()
                # 1 sec threshold 
                if start_time < last_modified:
                    return (
                        False,
                        "Metadata files are updated after the last validation. Re-run validation.",
                    )

                if not content["study_id"]:
                    return (
                        False,
                        "Validation file content is not valid. Study id is different.",
                    )
                if content["status"] == "ERROR":
                    return (
                        False,
                        "There are validation errors. Update metadata and data files and re-run validation",
                    )
                return True, "There is no validation errors"
            except Exception as exc:
                message = f"Validation file read error. {validation_time}: {str(exc)}"
                logger.error(message)
                return False, message
        else:
            return False, "Validation report is not ready. Run validation."

    def refactor_study_folder(
        self,
        study: schemes.Study,
        study_location: str,
        user_token,
        study_id: str,
        updated_study_id: str,
    ):
        if study_id == updated_study_id:
            return
        task_name="ASSIGN_ACCESSION_NUMBER"
        maintenance_task = StudyFolderMaintenanceTask(
            updated_study_id,
            types.StudyStatus(study.status),
            study.releasedate,
            study.submissiondate,
            task_name=task_name,
            obfuscationcode=study.obfuscationcode,
            delete_unreferenced_metadata_files=False,
            settings=get_settings().study,
            apply_future_actions=True,
        )
        date_format = "%Y-%m-%d_%H-%M-%S"
        folder_name = time.strftime(date_format) +"_" + task_name
        maintenance_task.create_audit_folder(folder_name=folder_name, stage=None)

        isa_study_item, isa_inv, _ = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )
        # update investigation file
        isa_inv.identifier = updated_study_id
        if isa_inv:
            isa_study: Study = isa_study_item
            isa_study.identifier = updated_study_id
            isa_inv.identifier = updated_study_id
            study_filename: str = isa_study.filename
            isa_study.filename = study_filename.replace(
                study_id, updated_study_id, 1
            )
            for assay_item in isa_study.assays:
                assay: Assay = assay_item
                assay.filename = assay.filename.replace(study_id, updated_study_id, 1)
            iac.write_isa_study(
                isa_inv, user_token, study_location, save_investigation_copy=False, save_assays_copy=False, save_samples_copy=False
            )
        else:
            logger.error(f"i_Investigation.txt file on {study_location} does not exist.")

        # update assay file (maf file references) and rename all metadata files
        metadata_files_result = glob.iglob(os.path.join(study_location, "?_*.t??"))
        metadata_files = [x for x in metadata_files_result]
        for metadata_file in metadata_files:
            base_name = os.path.basename(metadata_file)
            if base_name.startswith("a_"):
                assay_df = maintenance_task.read_tsv_file(metadata_file)
                for column in assay_df.columns:
                    if "Metabolite Assignment File" in column:
                        assay_df[column] = assay_df[column].apply(
                            lambda x: x.replace(study_id, updated_study_id, 1)
                            if x
                            else ""
                        )
                        maintenance_task.write_tsv_file(assay_df, metadata_file)
            new_name = os.path.basename(metadata_file).replace(
                study_id, updated_study_id, 1
            )
            target_metadata_path = os.path.join(
                os.path.dirname(metadata_file), new_name
            )
            if metadata_file != target_metadata_path:
                shutil.move(metadata_file, target_metadata_path)

        # create symbolic links on rw storage
        mounted_paths = get_settings().study.mounted_paths
        managed_paths = [
            mounted_paths.study_metadata_files_root_path,
            mounted_paths.study_audit_files_root_path,
            mounted_paths.study_internal_files_root_path,
        ]

        for root_path in managed_paths:
            new_path = os.path.join(root_path, updated_study_id)
            current_path = os.path.join(root_path, study_id)
            shutil.move(current_path, new_path)
        
        maintenance_task.maintain_rw_storage_folders()
            # if not os.path.exists(new_path):
            #     maintenance_task.maintain_study_symlinks(current_path, new_path)

        # create symbolic links on services storage
        # inputs = {"updated_study_id": updated_study_id, "study_id": study_id}
        # create_links_on_data_storage.apply_async(kwargs=inputs)

        # create symbolic links on private ftp storage
        inputs = {
            "updated_study_id": updated_study_id,
            "study_id": study_id,
            "obfuscation_code": study.obfuscationcode,
        }
        rename_folder_on_private_storage.apply_async(kwargs=inputs)

    def update_db_study_id(
        self,
        current_study_id: str,
        current_study_status: types.StudyStatus,
        requested_study_status: types.StudyStatus,
        reserved_accession: str,
    ):
        mtbls_accession_states = (
            types.StudyStatus.PRIVATE,
            types.StudyStatus.INREVIEW,
            types.StudyStatus.PUBLIC,
        )
        provisional_id_states = (types.StudyStatus.PROVISIONAL, types.StudyStatus.DORMANT)
        mtbls_prefix = get_settings().study.accession_number_prefix
        target_study_id = current_study_id
        if (
            requested_study_status in mtbls_accession_states
            and current_study_status in provisional_id_states
            and not current_study_id.startswith(mtbls_prefix)
        ):
            if not reserved_accession:
                reserve_mtbls_accession(current_study_id)
            target_study_id = update_study_id_from_mtbls_accession(current_study_id)
            if not target_study_id:
                raise MetabolightsException(
                    http_code=403,
                    message=f"Error while assigning MetaboLights accession number for {current_study_id}",
                )
        if not target_study_id:
            raise MetabolightsException(message="Could not update the study id")
        return target_study_id

    @staticmethod
    def update_status(
        study_id,
        study_status,
        is_curator=False,
        obfuscation_code=None,
        user_token=None,
        first_public_date=None,
        first_private_date=None,
    ):
        study_status = study_status.lower()
        # Update database
        update_study_status(
            study_id,
            study_status,
            is_curator=is_curator,
            first_public_date=first_public_date,
            first_private_date=first_private_date,
        )

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
        summary="Change FTP study folder permission",
        nickname="Change FTP study permission",
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
            {"code": 200, "message": "OK. FTP folder permission toggled "},
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
        ],
    )
    @metabolights_exception_handler
    def put(self, study_id):
        # param validation
        if study_id is None:
            abort(404, message="Please provide valid parameter for study identifier")

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
            {"code": 200, "message": "OK. FTP folder permission returned"},
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
        ],
    )
    @metabolights_exception_handler
    def get(self, study_id):
        # param validation
        if study_id is None:
            abort(404, message="Please provide valid parameter for study identifier")

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        return get_ftp_folder_access_status(app, study_id)
