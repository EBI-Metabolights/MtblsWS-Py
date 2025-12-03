#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-May-23
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


import glob
import logging
import os
import re
from datetime import datetime
from typing import Union

from flask import jsonify, make_response, request, send_file
from flask_restful import Resource, abort
from flask_restful_swagger import swagger

from app.config import get_settings
from app.services.external.eb_eye_search import EbEyeSearchService
from app.services.storage_service.models import SyncTaskResult
from app.tasks.common_tasks.admin_tasks.es_and_db_study_synchronization import (
    sync_studies_on_es_and_db,
)
from app.tasks.common_tasks.basic_tasks.elasticsearch import (
    delete_study_index,
    reindex_all_public_studies,
    reindex_all_studies,
    reindex_study,
)
from app.tasks.common_tasks.basic_tasks.send_email import (
    send_email_for_new_provisional_study,
    send_technical_issue_email,
)
from app.tasks.common_tasks.report_tasks.eb_eye_search import (
    build_studies_for_europe_pmc,
    eb_eye_build_public_studies,
)
from app.tasks.datamover_tasks.basic_tasks.ftp_operations import (
    sync_private_ftp_data_files,
)
from app.tasks.datamover_tasks.basic_tasks.study_folder_maintenance import (
    delete_study_folders,
    maintain_storage_study_folders,
)
from app.tasks.hpc_study_rsync_client import (
    VALID_FOLDERS,
    StudyFolder,
    StudyFolderLocation,
    StudyFolderType,
    StudyRsyncClient,
)
from app.utils import (
    MetabolightsDBException,
    MetabolightsException,
    current_utc_time_without_timezone,
    metabolights_exception_handler,
)
from app.ws.auth.permissions import (
    public_endpoint,
    raise_deprecation_error,
    validate_audit_files_update,
    validate_audit_files_view,
    validate_data_files_upload,
    validate_study_index_delete,
    validate_study_index_update,
    validate_submission_update,
    validate_submission_view,
    validate_user_has_curator_role,
    validate_user_has_submitter_or_super_user_role,
)
from app.ws.db.dbmanager import DBManager
from app.ws.db.models import StudyTaskModel
from app.ws.db.permission_scopes import StudyResource, StudyResourceScope
from app.ws.db.schemes import Study, StudyTask
from app.ws.db.types import StudyStatus, StudyTaskName, StudyTaskStatus, UserRole
from app.ws.db.wrappers import (
    create_study_model_from_db_study,
    update_study_model_from_directory,
)
from app.ws.db_connection import (
    add_placeholder_flag,
    create_empty_study,
    get_all_private_studies_for_user,
    get_all_studies_for_user,
    get_id_list_by_req_id,
    get_public_studies_with_methods,
    query_study_submitters,
    study_submitters,
)
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.settings.utils import get_cluster_settings, get_study_settings
from app.ws.study import identifier_service
from app.ws.study.folder_utils import write_audit_files
from app.ws.study.study_service import StudyService
from app.ws.study.user_service import UserService
from app.ws.study.utils import get_study_metadata_path
from app.ws.utils import log_request

logger = logging.getLogger("wslog")
wsc = WsClient()
iac = IsaApiClient()


class MtblsStudies(Resource):
    @swagger.operation(
        summary="Get all public studies",
        notes="Get a list of all public Studies.",
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def get(self):
        log_request(request)
        public_endpoint(request)
        pub_list = wsc.get_public_studies()
        return jsonify(pub_list)


class EbEyeStudies(Resource):
    @swagger.operation(
        summary="Process studies for EB EYE Search",
        notes="Process studies for EB EYE Search.",
        parameters=[
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "consumer",
                "description": "Provide Consumber ebi or thomson or europe_pmc",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
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
    def get(self, consumer: str):
        log_request(request)
        result = validate_user_has_curator_role(request)
        user_token = result.context.user_api_token

        if consumer == "ebi":
            inputs = {"user_token": user_token, "thomson_reuters": False}
            task = eb_eye_build_public_studies.apply_async(
                kwargs=inputs, expires=60 * 60
            )
            response = {"Task started": f"Task id {task.id}"}
        elif consumer == "thomson":
            inputs = {"user_token": user_token, "thomson_reuters": True}
            task = eb_eye_build_public_studies.apply_async(
                kwargs=inputs, expires=60 * 60
            )
            response = {"Task started": f"Task id {task.id}"}
        elif consumer == "europe_pmc":
            inputs = {"user_token": user_token}
            task = build_studies_for_europe_pmc.apply_async(
                kwargs=inputs, expires=60 * 60
            )
            response = {"Task started": f"Task id {task.id}"}
        else:
            doc = EbEyeSearchService.get_study(study_id=consumer, thomson_reuters=False)
            xml_str = doc.toprettyxml(indent="  ")
            response = make_response(xml_str)
            response.headers["Content-Type"] = "text/xml; charset=utf-8"
        return response


class MtblsPrivateStudies(Resource):
    @swagger.operation(
        summary="Get all private studies",
        notes="Get a list of all Private Studies for Curator.",
        parameters=[
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            }
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
    def get(self):
        log_request(request)
        validate_user_has_curator_role(request)

        priv_list = wsc.get_private_studies()
        return jsonify(priv_list)


class MtblsStudiesWithMethods(Resource):
    @swagger.operation(
        summary="Get all public studies, with technology used",
        notes="Get a list of all public Studies, with the technology used.",
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def get(self):
        log_request(request)
        public_endpoint(request)

        logger.info("Getting all public studies (with methods)")
        study_list = get_public_studies_with_methods()
        studies = []
        for acc, method in study_list:
            studies.append({"accession": acc, "technology": method})
        return studies


class MyMtblsStudies(Resource):
    @swagger.operation(
        summary="Get all private studies for a user",
        notes="Get a list of all private studies for a user.",
        parameters=[
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def get(self):
        log_request(request)
        raise_deprecation_error(request)
        result = validate_user_has_submitter_or_super_user_role(request)
        user_token = result.context.user_api_token

        user_studies = get_all_private_studies_for_user(user_token)
        return jsonify({"data": user_studies})


class MyMtblsStudiesDetailed(Resource):
    @swagger.operation(
        summary="Get all studies, with details, for a user",
        notes="Get a list of all studies for a user. This also includes the status, release date, title and abstract",
        parameters=[
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def get(self):
        log_request(request)
        result = validate_user_has_submitter_or_super_user_role(request)
        user_token = result.context.user_api_token

        user_studies = get_all_studies_for_user(user_token)

        return jsonify({"data": user_studies})


class IsaTabInvestigationFile(Resource):
    @swagger.operation(
        summary="Get ISA-Tab Investigation file",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "investigation_filename",
                "description": "Investigation filename",
                "required": False,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "version",
                "description": "Version of Investigation file (audit record)",
                "required": False,
                "allowEmptyValue": False,
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
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def get(self, study_id):
        log_request(request)
        result = validate_submission_view(request)
        study_id = result.context.study_id

        inv_filename = None
        study_version = None
        inv_filename = request.args.get("investigation_filename", "").lower() or None

        study_version = request.args.get("version", "").lower() or None
        if not inv_filename:
            logger.warning(
                "Missing Investigation filename. Using default i_Investigation.txt"
            )
            inv_filename = "i_Investigation.txt"

        if study_version:
            logger.info("Loading version %s of the metadata", study_version)

        settings = get_study_settings()

        logger.info("Getting ISA-Tab Investigation file for %s", study_id)
        location = get_study_metadata_path(study_id)
        if study_version:
            location = os.path.join(
                settings.mounted_paths.study_audit_files_root_path,
                study_id,
                settings.audit_folder_name,
                study_version,
            )

        files = glob.glob(os.path.join(location, inv_filename))
        if files:
            file_path = files[0]
            filename = os.path.basename(file_path)
            try:
                return send_file(
                    file_path, max_age=0, as_attachment=True, download_name=filename
                )
            except OSError as err:
                logger.error(err)
                abort(
                    503,
                    message="Wrong investigation filename or file could not be read.",
                )
        else:
            abort(
                503, message="Wrong investigation filename or file could not be read."
            )


class IsaTabSampleFile(Resource):
    @swagger.operation(
        summary="Get ISA-Tab Sample file",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "sample_filename",
                "description": "Sample filename",
                "required": True,
                "allowEmptyValue": False,
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
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Study does not exist or your do not have access to this study.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def get(self, study_id):
        log_request(request)
        result = validate_submission_view(request)
        study_id = result.context.study_id
        location = get_study_metadata_path(study_id)
        sample_filename = request.args.get("sample_filename")
        if not sample_filename:
            logger.warning("Missing Sample filename.")
            abort(404, message="Missing Sample filename.")

        logger.info("Getting ISA-Tab Sample file %s for %s", sample_filename, study_id)

        files = glob.glob(os.path.join(location, sample_filename))
        if files:
            file_path = files[0]
            filename = os.path.basename(file_path)
            try:
                return send_file(
                    file_path, max_age=0, as_attachment=True, download_name=filename
                )
            except OSError as err:
                logger.error(err)
                abort(404, message="Wrong sample filename or file could not be read.")
        else:
            abort(404, message="Wrong sample filename or file could not be read.")


class IsaTabAssayFile(Resource):
    @swagger.operation(
        summary="Get ISA-Tab Assay file",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "assay_filename",
                "description": "Assay filename",
                "required": True,
                "allowEmptyValue": False,
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
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Study does not exist or your do not have access to this study.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def get(self, study_id):
        log_request(request)
        result = validate_submission_view(request)
        study_id = result.context.study_id
        location = get_study_metadata_path(study_id)

        assay_filename = None
        if request.args:
            assay_filename = (
                request.args.get("assay_filename")
                if request.args.get("assay_filename")
                else None
            )
        if not assay_filename:
            logger.warning("Missing Assay filename.")
            abort(404, message="Missing Assay filename.")

        logger.info("Getting ISA-Tab Assay file for %s", study_id)
        files = glob.glob(os.path.join(location, assay_filename))
        if files:
            file_path = files[0]
            filename = os.path.basename(file_path)
            try:
                return send_file(
                    file_path, max_age=0, as_attachment=True, download_name=filename
                )
            except OSError as err:
                logger.error(err)
                abort(404, message="Wrong assay filename or file could not be read.")
        else:
            abort(404, message="Wrong assay filename or file could not be read.")


class CloneAccession(Resource):
    @swagger.operation(
        summary="[Deprecated] Create a new study and upload folder",
        notes="""This will clone the default template LC-MS study if no "study_id" parameter is given
        If a "to_study_id" (destination study id) is provided, <b>data will be copied into this *existing* study.
        Please be aware that data in the destination study will be replaced with metadata files from "study_id"</b>.""",
        parameters=[
            {
                "name": "study_id",
                "description": "Existing Study to clone from",
                "required": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "to_study_id",
                "description": "Existing Study to clone into",
                "required": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "include-raw-data",
                "description": "Include raw data when cloning a study.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
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
            {"code": 200, "message": "OK."},
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Study does not exist or your do not have access to this study.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
            {
                "code": 408,
                "message": "Request Timeout. The MetaboLights queue took too long to complete.",
            },
        ],
    )
    def post(self):
        raise_deprecation_error(request)


class CreateUploadFolder(Resource):
    @swagger.operation(
        deprecated=True,
        summary="[Deprecated] Create a new study upload folder",
        parameters=[
            {
                "name": "study_id",
                "description": "Existing Study Identifier to add an upload folder to",
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
                "message": "Unauthorized. Access to the resource requires user authentication. "
                "Please provide a study id and a valid user token",
            },
            {
                "code": 403,
                "message": "Study does not exist or your do not have access to this study.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def post(self, study_id):
        result = validate_data_files_upload(request)
        study_id = result.context.study_id
        obfuscation_code = result.context.obfuscation_code
        user_token = result.context.user_api_token
        logger.info("Creating a new study upload folder for study %s", study_id)
        status = wsc.create_upload_folder(study_id, obfuscation_code, user_token)
        return status


class AuditFiles(Resource):
    @swagger.operation(
        summary="Save a copy of the metadata into an audit folder",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier to create audit record for",
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
                "message": "Unauthorized. Access to the resource requires user authentication. "
                "Please provide a study id and a valid user token",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def post(self, study_id):
        result = validate_audit_files_update(request)
        study_id = result.context.study_id
        study_location = get_study_metadata_path(study_id)
        logger.info("Creating a new study audit folder for study %s", study_id)
        status, dest_path = write_audit_files(study_location)

        if status:
            return {"Success": "Created audit record for " + study_id}
        else:
            return {"Error": "Failed to create audit folder " + dest_path}

    @swagger.operation(
        summary="Get an overview of the available audit folders for a study",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier to retrieve audit record names",
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
                "message": "Unauthorized. Access to the resource requires user authentication. "
                "Please provide a study id and a valid user token",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def get(self, study_id):
        result = validate_audit_files_view(request)
        study_id = result.context.study_id
        study_location = get_study_metadata_path(study_id)

        return jsonify(get_audit_files(study_location))


class PublicStudyDetail(Resource):
    @swagger.operation(
        summary="Returns details of a public study",
        parameters=[
            {
                "name": "study_id",
                "description": "Requested public study id",
                "paramType": "path",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def get(self, study_id):
        log_request(request)
        result = validate_submission_view(request)
        if result.context.study_status != StudyStatus.PUBLIC:
            raise MetabolightsException(message="Not valid request", http_code=403)

        with DBManager.get_instance().session_maker() as db_session:
            query = db_session.query(Study)
            query = query.filter(
                Study.status == StudyStatus.PUBLIC.value, Study.acc == study_id
            )
            study = query.first()

            if not study:
                raise MetabolightsDBException(
                    f"{study_id} does not exist or is not public"
                )

            settings = get_study_settings()
            study_folders = settings.mounted_paths.study_metadata_files_root_path
            m_study = create_study_model_from_db_study(study)

        update_study_model_from_directory(m_study, study_folders)
        dict_data = m_study.model_dump()
        result = {"content": dict_data, "message": None, "err": None}
        return result


class CreateAccession(Resource):
    @swagger.operation(
        summary="Create a new study",
        notes="""Create a new study, with upload folder</br>
        Please note that this includes an empty sample file, which will require at least
        one additional data row to be ISA-Tab compliant""",
        parameters=[
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "study-id",
                "description": "Requested study id (Leave it empty for new study id)",
                "paramType": "header",
                "type": "string",
                "required": False,
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
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def get(self):
        result = validate_user_has_submitter_or_super_user_role(request)
        user_token = result.context.user_api_token
        user_role = result.context.user_role
        partner_user = result.context.partner_user
        username = result.context.username
        studies = UserService.get_instance().get_user_studies(user_token)
        provisional_studies = []
        last_study_datetime = datetime.fromtimestamp(0)
        for study in studies:
            if study.status == StudyStatus.PROVISIONAL.value:
                provisional_studies.append(study)
            if study.submissiondate.timestamp() > last_study_datetime.timestamp():
                last_study_datetime = study.submissiondate
        study_settings = get_study_settings()
        now = current_utc_time_without_timezone()
        if (
            now - last_study_datetime
        ).total_seconds() < study_settings.min_study_creation_interval_in_mins * 60:
            logger.warning(
                f"New study creation request from user {username} in {study_settings.min_study_creation_interval_in_mins} mins"
            )
            raise MetabolightsException(
                message="Submitter can create only one study in five minutes.",
                http_code=429,
            )

        if (
            len(provisional_studies) >= study_settings.max_study_in_provisional_status
            and user_role != UserRole.ROLE_SUPER_USER
            and user_role != UserRole.SYSTEM_ADMIN
            and not partner_user
        ):
            logger.warning(
                f"New study creation request from user {username}. User has already {study_settings.max_study_in_provisional_status} study in Provisional status."
            )
            raise MetabolightsException(
                message="The user can have at most two studies in Provisional status. Please complete and update status of your current studies.",
                http_code=400,
            )

        logger.info(
            f"Step 1: New study creation request is received from user {username}"
        )
        new_accession_number = True
        study_acc: Union[None, str] = None
        if "study_id" in request.headers:
            requested_study_id = request.headers["study_id"]
            study_acc = self.validate_requested_study_id(requested_study_id, user_token)
            if study_acc:
                new_accession_number = False
                logger.warning(
                    f"A previous study creation request from the user {username}. The study {study_acc} will be created."
                )

        try:
            study_acc = create_empty_study(user_token, study_id=study_acc)
            # study = StudyService.get_instance().get_study_by_acc(study_id=study_acc)

            if study_acc:
                logger.info(f"Step 2: Study id {study_acc} is created on DB.")
            else:
                raise MetabolightsException(
                    message="Could not create a new study in db", http_code=503
                )
        except Exception as exc:
            inputs = {
                "subject": "Study id creation on DB was failed.",
                "body": f"Study id on db creation was failed: folder: {study_acc}, user: {username} <p> {str(exc)}",
            }
            send_technical_issue_email.apply_async(kwargs=inputs)
            logger.error(
                f"Study id creation on DB was failed. Temp folder: {study_acc}. {str(inputs)}"
            )
            if isinstance(exc, MetabolightsException):
                raise exc
            else:
                raise MetabolightsException(
                    message="Study id creation on db was failed.",
                    http_code=501,
                    exception=exc,
                )

        inputs = {
            "user_token": user_token,
            "study_id": study_acc,
            "send_email_to_submitter": False,
            "task_name": "INITIAL_METADATA",
            "maintain_metadata_storage": True,
            "maintain_private_ftp_storage": False,
            "force_to_maintain": True,
        }
        try:
            maintain_storage_study_folders(**inputs)
            logger.info(
                f"Step 4.1: 'Create initial files and folders' task completed for study {study_acc}"
            )
        except Exception as exc:
            logger.info(
                f"Step 4.1: 'Create initial files and folders' task failed for study {study_acc}. {str(exc)}"
            )

        inputs.update(
            {
                "maintain_metadata_storage": False,
                "maintain_private_ftp_storage": True,
                "task_name": "INITIAL_FTP_FOLDERS",
            }
        )
        create_ftp_folders_task = maintain_storage_study_folders.apply_async(
            kwargs=inputs
        )
        logger.info(
            "Step 4.3: 'Create study FTP folders' task has been started for study "
            "%s with task id: %s",
            study_acc,
            create_ftp_folders_task.id,
        )

        if new_accession_number:
            study: Study = StudyService.get_instance().get_study_by_acc(study_acc)
            ftp_folder_name = study_acc.lower() + "-" + study.obfuscationcode
            inputs = {
                "user_token": user_token,
                "study_id": study_acc,
                "folder_name": ftp_folder_name,
            }
            send_email_for_new_provisional_study.apply_async(kwargs=inputs)
            logger.info("Step 5: Sending FTP folder email for the study %s", study_acc)
        else:
            logger.info("Step 5: Skipping FTP folder email for the study %s", study_acc)

        # # Start ftp folder creation task
        # Start reindex task
        inputs = {"user_token": user_token, "study_id": study_acc}
        reindex_task = reindex_study.apply_async(kwargs=inputs)
        logger.info(
            f"Step 6: Reindex task is started for study {study_acc} with task id: {reindex_task.id}"
        )

        return {"new_study": study_acc}

    def validate_requested_study_id(self, requested_study_id, user_token):
        """
        If study_id is set, check the rules below:
        Rule 1- study_id must start with  MTBLS
        Rule 2- user must have superuser role
        Rule 3- study_id must be equal or less than last study id, and greater than 0
        Rule 4- study folder does not exist or is empty
        Rule 5- study_id must not be in database
        """
        # Rule 1
        study_id_prefix = identifier_service.default_provisional_identifier.get_prefix()
        if not requested_study_id.startswith(study_id_prefix):
            abort(
                401,
                message="Invalid provisional id format. Provisional id must start with %s"
                % study_id_prefix,
            )
        # Rule 2
        UserService.get_instance().validate_user_has_curator_role(user_token)
        # Rule 3
        # disable this rule for provisional id
        # Rule 4
        study_location = os.path.join(
            get_study_settings().mounted_paths.study_metadata_files_root_path,
            requested_study_id,
        )
        if os.path.exists(study_location):
            files = os.listdir(study_location)
            if files:
                raise MetabolightsException(
                    message="Study folder is already exist", http_code=400
                )
        # Rule 5
        try:
            id_list = get_id_list_by_req_id(requested_study_id)
        except Exception as ex:
            raise MetabolightsException(
                message=f"Requested id is not valid in db. {str(ex)}", http_code=400
            )

        if id_list:
            raise MetabolightsException(
                message="Requested id already used in DB.", http_code=400
            )
        else:
            return requested_study_id


class DeleteStudy(Resource):
    @swagger.operation(
        summary="Delete an existing study (curator only)",
        notes="""Please note that deleting a study will release the accession number back to be reused.
        This will be available for the MetaboLights team as a placeholder""",
        parameters=[
            {
                "name": "study_id",
                "description": "Existing Study to delete",
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
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def delete(self, study_id):
        result = validate_user_has_curator_role(request)
        status = result.context.study_status
        if status == StudyStatus.PUBLIC:
            abort(401, message="It is not allowed to delete a public study")

        logger.info("Deleting study %s", study_id)

        # Remove the submitter from the study
        submitter_emails = query_study_submitters(study_id)
        if submitter_emails:
            for submitter in submitter_emails:
                study_submitters(study_id, submitter[0], "delete")

        # Add the placeholder flag and MetaboLights user to the study
        mtbls_email = get_settings().auth.service_account.email
        add_placeholder_flag(study_id)
        study_submitters(study_id, mtbls_email, "add")
        delete_study_folders(
            study_id=study_id,
            force_to_maintain=True,
            delete_private_ftp_storage_folders=False,
            delete_metadata_storage_folders=True,
            task_name=f"DELETE_STUDY_{study_id}",
            failing_gracefully=False,
        )

        inputs = {
            "study_id": study_id,
            "task_name": f"DELETE_STUDY_{study_id}",
            "force_to_maintain": True,
            "delete_private_ftp_storage_folders": True,
            "delete_metadata_storage_folders": False,
        }
        cluster_settings = get_cluster_settings()
        task = delete_study_folders.apply_async(kwargs=inputs)
        task.get(timeout=cluster_settings.task_get_timeout_in_seconds * 2)

        status, message = wsc.reindex_study(study_id)
        if not status:
            abort(500, error="Could not reindex the study")

        return {"Success": "Study " + study_id + " has been removed"}


def get_audit_files(study_location):
    folder_list = []
    settings = get_study_settings()
    audit_path = os.path.join(study_location, settings.audit_files_symbolic_link_name)

    try:
        folder_list = os.listdir(audit_path)
    except Exception as ex:
        logger.error("List dir error: %s, %s", audit_path, ex)
        return folder_list
    return folder_list


class ReindexStudy(Resource):
    @swagger.operation(
        summary="Reindex a MetaboLights study (curator only)",
        notes="""Reindexing a MetaboLights study to ensure the search index is up to date""",
        parameters=[
            {
                "name": "study_id",
                "description": "Existing Study Identifier to index",
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
            {
                "name": "include_validation_results",
                "description": "run study validation and include in indexed data.",
                "required": False,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": "false",
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication. "
                "Please provide a study id and a valid user token",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
            {"code": 417, "message": "Unexpected result."},
        ],
    )
    @metabolights_exception_handler
    def post(self, study_id):
        result = validate_study_index_update(request)
        study_id = result.context.study_id
        study_index_scopes = result.permission.scopes.get(StudyResource.STUDY_INDEX)
        read_access = StudyResourceScope.VIEW in study_index_scopes
        write_access = (
            StudyResourceScope.CREATE in study_index_scopes
            and StudyResourceScope.UPDATE in study_index_scopes
        )
        status, message = wsc.reindex_study(
            study_id,
            user_token=None,
            include_validation_results=False,
            sync=True,
        )

        if not status:
            MetabolightsException(http_code=500, message=message)

        return {
            "Success": "Study " + study_id + " has been re-indexed",
            "read_access": read_access,
            "write_access": write_access,
        }

    @swagger.operation(
        summary="Delete a study index ",
        parameters=[
            {
                "name": "study_id",
                "description": "Compound Identifier",
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
    def delete(self, study_id):
        log_request(request)
        result = validate_study_index_delete(request)
        study_id = result.context.study_id

        logger.info("Deleting a study index")

        result = delete_study_index(user_token=None, study_id=study_id)

        result = {"content": result, "message": None, "err": None}
        return result


class UnindexedStudy(Resource):
    @swagger.operation(
        summary="Gets unindexed studies from database (curator only)",
        notes="""Gets MetaboLights studies that should be updated for the up-to-date search index""",
        parameters=[
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication. "
                "Please provide a study id and a valid user token",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
            {"code": 417, "message": "Unexpected result."},
        ],
    )
    @metabolights_exception_handler
    def get(self):
        validate_user_has_curator_role(request)
        try:
            with DBManager.get_instance().session_maker() as db_session:
                query = db_session.query(StudyTask)
                filtered = query.filter(
                    StudyTask.last_execution_status
                    != StudyTaskStatus.EXECUTION_SUCCESSFUL,
                    StudyTask.task_name == StudyTaskName.REINDEX,
                ).order_by(StudyTask.study_acc)
                result = filtered.all()
                result_list = []
                for task in result:
                    model: StudyTaskModel = StudyTaskModel.model_validate(task)
                    result_list.append(model.model_dump())

                if result_list:
                    return jsonify({"result": "Found", "tasks": result_list})
                return jsonify({"result": "There is no study that will be reindexed."})

        except Exception as e:
            raise MetabolightsDBException(
                message=f"Error while retreiving study tasks from database: {str(e)}",
                exception=e,
            )


class RetryReindexStudies(Resource):
    @swagger.operation(
        summary="Reindex unindexed public studies",
        parameters=[
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication. "
                "Please provide a study id and a valid user token",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
            {"code": 417, "message": "Unexpected result."},
        ],
    )
    @metabolights_exception_handler
    def post(self):
        validate_user_has_curator_role(request)
        try:
            with DBManager.get_instance().session_maker() as db_session:
                query = db_session.query(StudyTask)
                filtered = query.filter(
                    StudyTask.last_execution_status
                    != StudyTaskStatus.EXECUTION_SUCCESSFUL,
                    StudyTask.task_name == StudyTaskName.REINDEX,
                ).order_by(StudyTask.study_acc)
                tasks = filtered.all()
                indexed_studies = []
                unindexed_studies = []
                total = len(tasks)
                index = 0

                for task in tasks:
                    index += 1
                    print(f"{index}/{total} Indexing {task.study_acc}")
                    try:
                        logger.info(f"{index}/{total} Indexing {task.study_acc}")
                        wsc.reindex_study(task.study_acc, None)
                        indexed_studies.append(task.study_acc)
                        logger.info(f"Indexed study {task.study_acc}")
                    except Exception as e:
                        unindexed_studies.append(
                            {"study_id": task.study_acc, "message": str(e)}
                        )
                        logger.info(f"Unindexed study {task.study_acc}")

                return {
                    "indexed_studies": indexed_studies,
                    "unindexed_studies": unindexed_studies,
                }

        except Exception as e:
            raise MetabolightsDBException(
                message=f"Error while retreiving study tasks from database: {str(e)}",
                exception=e,
            )


class MtblsPublicStudiesIndexAll(Resource):
    @swagger.operation(
        summary="Index all public studies ",
        notes="Start a task to index all public studies and return task id. Result will be sent by email.",
        parameters=[
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=[
            {"code": 200, "message": "OK. The compound is returned"},
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
    def post(self):
        log_request(request)
        result = validate_user_has_curator_role(request)
        user_token = result.context.user_api_token

        logger.info("Indexing public studies")
        inputs = {"user_token": user_token, "send_email_to_submitter": True}
        try:
            result = reindex_all_public_studies.apply_async(
                kwargs=inputs, expires=60 * 5
            )

            result = {
                "content": f"Task has been started. Result will be sent by email. Task id: {result.id}",
                "message": None,
                "err": None,
            }
            return result
        except Exception as ex:
            raise MetabolightsException(
                http_code=500,
                message=f"Task submission was failed: {str(ex)}",
                exception=ex,
            )


class MtblsStudiesIndexAll(Resource):
    @swagger.operation(
        summary="Index all studies ",
        notes="Start a task to index all studies and return task id. Result will be sent by email.",
        parameters=[
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=[
            {"code": 200, "message": "OK. The compound is returned"},
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
    def post(self):
        log_request(request)
        result = validate_user_has_curator_role(request)
        user_token = result.context.user_api_token

        logger.info("Indexing studies.")
        inputs = {"user_token": user_token, "send_email_to_submitter": True}
        try:
            result = reindex_all_studies.apply_async(kwargs=inputs, expires=60 * 5)

            result = {
                "content": f"Task has been started. Result will be sent by email. Task id: {result.id}",
                "message": None,
                "err": None,
            }
            return result
        except Exception as ex:
            raise MetabolightsException(
                http_code=500,
                message=f"Task submission was failed: {str(ex)}",
                exception=ex,
            )


class MtblsStudiesIndexSync(Resource):
    @swagger.operation(
        summary="Sync all studies on database and elasticsearch",
        notes="Start a task to sync all studies on database and elasticsearch, and return task id. Result will be sent by email.",
        parameters=[
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=[
            {"code": 200, "message": "OK. The compound is returned"},
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
    def post(self):
        log_request(request)
        result = validate_user_has_curator_role(request)
        user_token = result.context.user_api_token

        logger.info("Indexing missing/out-of-date studies.")
        inputs = {"user_token": user_token, "send_email_to_submitter": True}
        try:
            result = sync_studies_on_es_and_db.apply_async(
                kwargs=inputs, expires=60 * 5
            )

            result = {
                "content": f"Task has been started. Result will be sent by email. Task id: {result.id}",
                "message": None,
                "err": None,
            }
            return result
        except Exception as ex:
            raise MetabolightsException(
                http_code=500,
                message=f"Task submission was failed: {str(ex)}",
                exception=ex,
            )


class MtblsStudyFolders(Resource):
    @swagger.operation(
        summary="Maintain study folders",
        notes="Start a task to maintain all study folders, and return task id. Result will be sent by email.",
        parameters=[
            {
                "name": "study_id",
                "description": "Study to validate",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "target-folder",
                "description": "Select target study folders: metadata, data and private ftp folder",
                "required": False,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
                "enum": ["metadata", "private-ftp"],
                "allowEmptyValue": False,
                "defaultValue": "metadata",
                "default": "metadata",
            },
            {
                "name": "force",
                "description": "Force to maintain",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": False,
                "default": True,
            },
            {
                "name": "task-name",
                "description": "Any task name, backup folders created with this name",
                "required": False,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
                "allowEmptyValue": False,
                "defaultValue": "MAINTAIN",
                "default": "MAINTAIN",
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
            {"code": 200, "message": "OK. The compound is returned"},
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
    def post(self, study_id):
        log_request(request)
        result = validate_submission_update(request)
        user_token = result.context.user_api_token

        target_folder = ""
        if "target_folder" in request.headers:
            target_folder = request.headers["target_folder"]

        task_name = ""
        if "task_name" in request.headers:
            task_name = request.headers["task_name"]
        if not task_name:
            task_name = "MAINTAIN"
        if target_folder not in ("metadata", "data", "private-ftp"):
            raise MetabolightsException(message="Target folder is not defined.")
        maintain_metadata_storage = False
        maintain_private_ftp_storage = False
        if target_folder == "metadata":
            maintain_metadata_storage = True
        elif target_folder == "private-ftp":
            maintain_private_ftp_storage = True

        force_to_maintain = False
        if request.args.get("force"):
            force_to_maintain = (
                True if request.args.get("force").lower() == "true" else False
            )

        logger.info("Searching study folders")
        try:
            inputs = {
                "user_token": user_token,
                "send_email_to_submitter": True,
                "study_id": study_id,
                "force_to_maintain": force_to_maintain,
                "task_name": task_name,
                "maintain_metadata_storage": maintain_metadata_storage,
                "maintain_private_ftp_storage": maintain_private_ftp_storage,
            }
            task = maintain_storage_study_folders.apply_async(kwargs=inputs)

            result = {
                "content": f"Task has been started. Result will be sent by email with task id {task.id}",
                "message": None,
                "err": None,
            }
            return result
        except Exception as ex:
            raise MetabolightsException(
                http_code=500,
                message=f"Task submission was failed: {str(ex)}",
                exception=ex,
            )


class StudyFolderSynchronization(Resource):
    @swagger.operation(
        summary="(Curator Only) If there is no rsync task, it starts new one.",
        nickname="Start sync process. New and updated files will be sync from source to target study folder",
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
                "name": "source-staging-area",
                "description": "Source study folder stage",
                "required": True,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
                "enum": ["private-ftp", "readonly-study", "rw-study", "public-ftp"],
                "allowEmptyValue": False,
                "defaultValue": "private-ftp",
                "default": "private-ftp",
            },
            {
                "name": "target-staging-area",
                "description": "Target study folder stage",
                "required": True,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
                "enum": ["private-ftp", "readonly-study", "rw-study", "public-ftp"],
                "allowEmptyValue": False,
                "defaultValue": "rw-study",
                "default": "rw-study",
            },
            {
                "name": "sync-type",
                "description": "Sync category: sync metadada or data or internal files",
                "required": True,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
                "enum": [
                    "metadata",
                    "data",
                    "internal",
                    "public-metadata-versions",
                    "integrity-check",
                    "audit",
                ],
                "allowEmptyValue": False,
                "defaultValue": "metadata",
                "default": "metadata",
            },
            {
                "name": "dry-run",
                "description": "Only check whether there is a difference ",
                "required": False,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "Boolean",
                "allowEmptyValue": False,
                "defaultValue": True,
                "default": True,
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
    def post(self, study_id):
        log_request(request)
        headers = request.headers
        sync_type = headers.get("sync_type", "").lower()
        source_staging_area = headers.get("source_staging_area", "private-ftp")
        target_staging_area = headers.get("target_staging_area", "rw_study")
        dry_run = True if headers.get("dry_run", "true").lower() == "true" else False

        if source_staging_area == "private-ftp" and (
            (sync_type == "data" and target_staging_area == "readonly-study")
            or (sync_type == "metadata" and target_staging_area == "rw-study")
        ):
            validate_data_files_upload(request)
        else:
            validate_user_has_curator_role(request)
        return self.process_rsync_request(
            study_id,
            dry_run=dry_run,
            sync_type=sync_type,
            source_staging_area=source_staging_area,
            target_staging_area=target_staging_area,
            start_new_task=True,
        )

    @swagger.operation(
        summary="(Curator Only) Returns current rsync task status.",
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
                "name": "source-staging-area",
                "description": "Source study folder stage",
                "required": True,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
                "enum": ["private-ftp", "readonly-study", "rw-study", "public-ftp"],
                "allowEmptyValue": False,
                "defaultValue": "private-ftp",
                "default": "private-ftp",
            },
            {
                "name": "target-staging-area",
                "description": "Target study folder stage",
                "required": True,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
                "enum": ["private-ftp", "readonly-study", "rw-study", "public-ftp"],
                "allowEmptyValue": False,
                "defaultValue": "rw-study",
                "default": "rw-study",
            },
            {
                "name": "sync-type",
                "description": "Sync category: sync metadada or data or internal files",
                "required": True,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
                "enum": [
                    "metadata",
                    "data",
                    "internal",
                    "public-metadata-versions",
                    "integrity-check",
                    "audit",
                ],
                "allowEmptyValue": False,
                "defaultValue": "metadata",
                "default": "metadata",
            },
            {
                "name": "dry-run",
                "description": "Only check whether there is a difference ",
                "required": False,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "Boolean",
                "allowEmptyValue": False,
                "defaultValue": True,
                "default": True,
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
    def get(self, study_id):
        log_request(request)
        headers = request.headers
        sync_type = headers.get("sync_type", "").lower()
        source_staging_area = headers.get("source_staging_area", "private-ftp")
        target_staging_area = headers.get("target_staging_area", "rw_study")
        dry_run = True if headers.get("dry_run", "true").lower() == "true" else False

        if source_staging_area == "private-ftp" and (
            (sync_type == "data" and target_staging_area == "readonly-study")
            or (sync_type == "metadata" and target_staging_area == "rw-study")
        ):
            validate_data_files_upload(request)
        else:
            validate_user_has_curator_role(request)

        return self.process_rsync_request(
            study_id,
            dry_run=dry_run,
            sync_type=sync_type,
            source_staging_area=source_staging_area,
            target_staging_area=target_staging_area,
            start_new_task=False,
        )

    def process_rsync_request(
        self,
        study_id: str,
        dry_run,
        sync_type,
        source_staging_area,
        target_staging_area,
        start_new_task: bool = False,
    ):
        if target_staging_area == source_staging_area:
            raise MetabolightsException(
                message="Source and target staging areas should be different."
            )

        if sync_type not in (
            "metadata",
            "data",
            "internal",
            "public-metadata-versions",
            "integrity-check",
            "audit",
        ):
            raise MetabolightsException(message="sync_type is invalid.")

        staging_areas = ["private-ftp", "readonly-study", "rw-study", "public-ftp"]
        if source_staging_area not in staging_areas:
            raise MetabolightsException(message="Source staging area is invalid.")

        if target_staging_area not in staging_areas:
            raise MetabolightsException(message="Target staging area is invalid.")

        folder_type = StudyFolderType(sync_type.replace("-", "_").upper())
        source_location = StudyFolderLocation(
            f"{source_staging_area.replace('-', '_').upper()}_STORAGE"
        )
        target_location = StudyFolderLocation(
            f"{target_staging_area.replace('-', '_').upper()}_STORAGE"
        )
        source = StudyFolder(location=source_location, folder_type=folder_type)
        target = StudyFolder(location=target_location, folder_type=folder_type)

        if source.folder_type not in VALID_FOLDERS[source_location]:
            raise MetabolightsException(
                message="Source folder type is not valid in the selected staging area."
            )

        if target.folder_type not in VALID_FOLDERS[target_location]:
            raise MetabolightsException(
                message="target folder type is not valid in the selected staging area."
            )

        study = StudyService.get_instance().get_study_by_acc(study_id)
        client = StudyRsyncClient(
            study_id=study_id, obfuscation_code=study.obfuscationcode
        )
        status_check_only = not start_new_task

        if (
            sync_type == "data"
            and source_staging_area == "private-ftp"
            and target_staging_area == "readonly-study"
        ):
            status = sync_private_ftp_data_files(
                study_id=study_id, obfuscation_code=study.obfuscationcode
            )

        else:
            if dry_run:
                status: SyncTaskResult = client.rsync_dry_run(
                    source, target, status_check_only=status_check_only
                )
            else:
                if (
                    start_new_task
                    and target.folder_type == StudyFolderType.METADATA
                    and target.location == StudyFolderLocation.RW_STUDY_STORAGE
                ):
                    write_audit_files(study_id)
                status: SyncTaskResult = client.rsync(
                    source, target, status_check_only=status_check_only
                )

        status.description = (
            f"{status.description[:100]} ..."
            if status.description and len(status.description) > 100
            else status.description
        )
        return status.model_dump()


class DragAndDropFolder(Resource):
    @swagger.operation(
        notes="Upload files via drag-and-drop for a given study (max 50MB per file)",
        parameters=[
            {
                "name": "study_id",
                "description": "ID of the study",
                "required": True,
                "allowMultiple": False,
                "dataType": "string",
                "paramType": "path",
            },
            {
                "name": "file",
                "description": "File to upload (max 50MB)",
                "required": True,
                "allowMultiple": False,
                "type": "file",
                "paramType": "form",
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
            {"code": 200, "message": "File uploaded successfully"},
            {"code": 400, "message": "Invalid request or file too large"},
        ],
    )
    def post(self, study_id):
        result = validate_submission_update(request)
        study_id = result.context.study_id
        try:
            logger.info("Upload metadata files against study id: %s", study_id)
            if "file" not in request.files:
                return {"error": "No file part in the request"}, 400

            file = request.files["file"]
            if file.filename == "":
                return {"error": "No selected file"}, 400

            # Check file type (only allow specific types .txt, .tsv)
            if not re.match(r"^([asi]_.+\.txt|m_.+\.tsv)$", file.filename):
                return {
                    "error": "Invalid file type/file pattern. Only .txt and .tsv files allowed."
                }, 400

            # Check file size (max 50MB)
            file.seek(0, os.SEEK_END)
            file_length = file.tell()
            file.seek(0)
            if file_length > 50 * 1024 * 1024:
                return {"error": "File size exceeds 50MB limit"}, 400

            # Construct the study folder path
            settings = get_study_settings()
            study_folder = os.path.join(
                settings.mounted_paths.study_metadata_files_root_path, study_id
            )

            # Save the file
            file_path = os.path.join(study_folder, file.filename)
            file.save(file_path)

            return {
                "message": f"File '{file.filename}' uploaded successfully to study '{study_id}'"
            }, 200
        except Exception as exc:
            logger.error("Error in DragAndDropFolder.post: %s", exc)
            return {"error": f"An unexpected error occurred: {str(exc)}"}, 500
