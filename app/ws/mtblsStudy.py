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
from datetime import datetime
from typing import Union

from flask import jsonify, request, send_file, make_response
from app.services.external.eb_eye_search import EbEyeSearchService
from flask_restful import Resource, abort, reqparse
from flask_restful_swagger import swagger
from app.config import get_settings

from app.file_utils import make_dir_with_chmod
from app.services.storage_service.models import SyncTaskResult
from app.services.storage_service.storage_service import StorageService
from app.tasks.common_tasks.basic_tasks.elasticsearch import (
    delete_study_index,
    reindex_all_public_studies,
    reindex_all_studies,
    reindex_study,
)
from app.tasks.common_tasks.basic_tasks.email import (
    send_email_for_new_provisional_study,
    send_technical_issue_email,
)
from app.tasks.common_tasks.admin_tasks.es_and_db_study_synchronization import sync_studies_on_es_and_db
from app.tasks.common_tasks.report_tasks.eb_eye_search import eb_eye_build_public_studies, build_studies_for_europe_pmc
from app.tasks.datamover_tasks.basic_tasks.study_folder_maintenance import delete_study_folders, maintain_storage_study_folders
from app.tasks.hpc_study_rsync_client import VALID_FOLDERS, StudyFolder, StudyFolderLocation, StudyFolderType, StudyRsyncClient

from app.utils import MetabolightsDBException, MetabolightsException, current_time, current_utc_time_without_timezone, metabolights_exception_handler
from app.ws import db_connection as db_proxy
from app.ws.db.dbmanager import DBManager
from app.ws.db.models import StudyTaskModel
from app.ws.db.schemes import Study, StudyTask, User
from app.ws.db.types import StudyStatus, StudyTaskName, StudyTaskStatus, UserRole
from app.ws.db.wrappers import create_study_model_from_db_study, update_study_model_from_directory
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
from app.ws.study_utilities import StudyUtils
from app.ws.utils import copy_file, copy_files_and_folders, get_timestamp, get_year_plus_one, log_request, remove_file

logger = logging.getLogger("wslog")
wsc = WsClient()
iac = IsaApiClient()


class MtblsStudies(Resource):
    @swagger.operation(
        summary="Get all public studies",
        notes="Get a list of all public Studies.",
        responseMessages=[
            {"code": 200, "message": "OK."},
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
        ],
    )
    def get(self):
        log_request(request)
        pub_list = wsc.get_public_studies()
        return jsonify(pub_list)


class EbEyeStudies(Resource):
    @swagger.operation(
        summary="Process studies for EB EYE Search",
        notes="Process studies for EB EYE Search.",
        parameters=[
            {
                "name": "user_token",
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
            }
            
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {"code": 401, "message": "Unauthorized. Access to the resource requires user authentication."},
            {"code": 403, "message": "Forbidden. Access to the study is not allowed for this user."},
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."}
        ],
    )
    def get(self, consumer: str):
        log_request(request)
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_curator_role(user_token)
        if consumer == "ebi":
            inputs = {"user_token": user_token, "thomson_reuters": False }
            task = eb_eye_build_public_studies.apply_async(kwargs=inputs, expires=60*60)
            response = {'Task started':f'Task id {task.id}'}
        elif consumer == "thomson":
            inputs = {"user_token": user_token, "thomson_reuters": True }
            task = eb_eye_build_public_studies.apply_async(kwargs=inputs, expires=60*60)
            response = {'Task started':f'Task id {task.id}'}
        elif consumer == "europe_pmc":
            inputs = {"user_token": user_token }
            task = build_studies_for_europe_pmc.apply_async(kwargs=inputs, expires=60*60)
            response = {'Task started':f'Task id {task.id}'}
        else:
            doc = EbEyeSearchService.get_study(study_id=consumer, thomson_reuters=False)
            xml_str = doc.toprettyxml(indent="  ")                                      
            response = make_response(xml_str)                                           
            response.headers['Content-Type'] = 'text/xml; charset=utf-8'            
        return response

class MtblsPrivateStudies(Resource):
    @swagger.operation(
        summary="Get all private studies",
        notes="Get a list of all Private Studies for Curator.",
        parameters=[
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {"code": 401, "message": "Unauthorized. Access to the resource requires user authentication."},
            {"code": 403, "message": "Forbidden. Access to the study is not allowed for this user."},
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
        ],
    )
    @metabolights_exception_handler
    def get(self):
        log_request(request)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_curator_role(user_token)

        priv_list = wsc.get_private_studies()
        return jsonify(priv_list)


class MtblsStudyValidationStatus(Resource):
    @swagger.operation(
        summary="Override validation status of a study",
        notes="Curator can override the validation status of a study.",
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
                "name": "validation_status",
                "description": "status of validation",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
                "enum": ["error", "warn", "success"],
            },
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {"code": 401, "message": "Unauthorized. Access to the resource requires user authentication."},
            {"code": 403, "message": "Forbidden. Access to the study is not allowed for this user."},
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
        ],
    )
    def put(self, study_id, validation_status):
        log_request(request)

        validation_status_list = ["error", "warn", "success"]

        if not validation_status in validation_status_list:
            abort(401)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            # user token is required
            abort(401)
        result = db_proxy.get_study(study_id)
        if not result:
            abort(404)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        result = db_proxy.update_validation_status(study_id=study_id, validation_status=validation_status)

        return jsonify(result)


class MtblsStudiesWithMethods(Resource):
    @swagger.operation(
        summary="Get all public studies, with technology used",
        notes="Get a list of all public Studies, with the technology used.",
        responseMessages=[
            {"code": 200, "message": "OK."},
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
        ],
    )
    def get(self):
        log_request(request)
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
        ],
    )
    def get(self):
        log_request(request)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        user_studies = get_all_private_studies_for_user(user_token)
        return jsonify({"data": user_studies})


class MyMtblsStudiesDetailed(Resource):
    @swagger.operation(
        summary="Get all studies, with details, for a user",
        notes="Get a list of all studies for a user. This also includes the status, release date, title and abstract",
        parameters=[
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
        ],
    )
    def get(self):
        log_request(request)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
        ],
    )
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(401, message="Missing study_id")

        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None:
            abort(401, message="Missing user_token")

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument("investigation_filename", help="Investigation filename")
        parser.add_argument("version", help="Version of metadata/Audit record")
        study_version = None
        inv_filename = None
        if request.args:
            args = parser.parse_args(req=request)
            inv_filename = args["investigation_filename"].lower() if args["investigation_filename"] else None
            study_version = args["version"].lower() if args["version"] else None
        if not inv_filename:
            logger.warning("Missing Investigation filename. Using default i_Investigation.txt")
            inv_filename = "i_Investigation.txt"

        if study_version:
            logger.info("Loading version " + study_version + " of the metadata")
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403, message="Study does not exist or your do not have access to this study")
        settings = get_study_settings()

        logger.info("Getting ISA-Tab Investigation file for %s", study_id)
        location = study_location
        if study_version:
            location = os.path.join(
                settings.mounted_paths.study_audit_files_root_path, study_id, settings.audit_folder_name, study_version
            )

        files = glob.glob(os.path.join(location, inv_filename))
        if files:
            file_path = files[0]
            filename = os.path.basename(file_path)
            try:
                return send_file(file_path, cache_timeout=-1, as_attachment=True, attachment_filename=filename)
            except OSError as err:
                logger.error(err)
                abort(503, message="Wrong investigation filename or file could not be read.")
        else:
            abort(503, message="Wrong investigation filename or file could not be read.")


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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {"code": 401, "message": "Unauthorized. Access to the resource requires user authentication."},
            {"code": 403, "message": "Study does not exist or your do not have access to this study."},
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
        ],
    )
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None:
            abort(401)

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument("sample_filename", help="Sample filename")
        sample_filename = None
        if request.args:
            args = parser.parse_args(req=request)
            sample_filename = args["sample_filename"]
            # sample_filename = sample_filename.lower() if args['sample_filename'] else None
        if not sample_filename:
            logger.warning("Missing Sample filename.")
            abort(404, message="Missing Sample filename.")

        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(401, message="Study does not exist or your do not have access to this study.")

        logger.info("Getting ISA-Tab Sample file %s for %s", sample_filename, study_id)
        location = study_location
        files = glob.glob(os.path.join(location, sample_filename))
        if files:
            file_path = files[0]
            filename = os.path.basename(file_path)
            try:
                return send_file(file_path, cache_timeout=-1, as_attachment=True, attachment_filename=filename)
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {"code": 401, "message": "Unauthorized. Access to the resource requires user authentication."},
            {"code": 403, "message": "Study does not exist or your do not have access to this study."},
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
        ],
    )
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None:
            abort(401)

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument("assay_filename", help="Assay filename")
        assay_filename = None
        if request.args:
            args = parser.parse_args(req=request)
            assay_filename = args["assay_filename"] if args["assay_filename"] else None
        if not assay_filename:
            logger.warning("Missing Assay filename.")
            abort(404, message="Missing Assay filename.")

        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(401, message="Study does not exist or your do not have access to this study.")

        logger.info("Getting ISA-Tab Assay file for %s", study_id)
        location = study_location

        files = glob.glob(os.path.join(location, assay_filename))
        if files:
            file_path = files[0]
            filename = os.path.basename(file_path)
            try:
                return send_file(file_path, cache_timeout=-1, as_attachment=True, attachment_filename=filename)
            except OSError as err:
                logger.error(err)
                abort(404, message="Wrong assay filename or file could not be read.")
        else:
            abort(404, message="Wrong assay filename or file could not be read.")


class CloneAccession(Resource):
    @swagger.operation(
        summary="Create a new study and upload folder",
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
                "name": "include_raw_data",
                "description": "Include raw data when cloning a study.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {"code": 401, "message": "Unauthorized. Access to the resource requires user authentication."},
            {"code": 403, "message": "Study does not exist or your do not have access to this study."},
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
            {"code": 408, "message": "Request Timeout. The MetaboLights queue took too long to complete."},
        ],
    )
    def post(self):
        # # User authentication
        # user_token = None
        # include_raw_data = False
        # bypass = False
        # lcms_default_study = "MTBLS121"  # This is the standard LC-MS study. This is private but safe for all to clone

        # if "user_token" in request.headers:
        #     user_token = request.headers["user_token"]

        # if user_token is None:
        #     abort(401)

        # if "include_raw_data" in request.headers and request.headers["include_raw_data"].lower() == "true":
        #     include_raw_data = True

        # parser = reqparse.RequestParser()
        # parser.add_argument("study_id", help="Study Identifier")
        # parser.add_argument("to_study_id", help="Study Identifier")
        # study_id = None
        # to_study_id = None

        # if request.args:
        #     args = parser.parse_args(req=request)
        #     study_id = args["study_id"]
        #     to_study_id = args["to_study_id"]

        # # param validation
        # if study_id is None:
        #     study_id = lcms_default_study

        # if study_id is lcms_default_study:
        #     bypass = True  # Users can safely clone this study, even when passing in MTBLS121

        # # Can the user read the study requested?
        # (
        #     is_curator,
        #     read_access,
        #     write_access,
        #     obfuscation_code,
        #     study_location,
        #     release_date,
        #     submission_date,
        #     study_status,
        # ) = wsc.get_permissions(study_id, user_token)

        # if not bypass:
        #     if not read_access:
        #         abort(401, message="Study does not exist or your do not have access to this study.")

        # study_id = study_id.upper()

        # # This is the existing study
        # study_to_clone = study_location

        # # If the user did not provide an existing study id to clone into, we create a new study
        # if to_study_id is None:
        #     study_date = get_year_plus_one()
        #     logger.info("Creating a new MTBLS Study (cloned from %s) with release date %s", study_id, study_date)
        #     new_folder_name = user_token + "~~" + study_date + "~" + "new_study_requested_" + get_timestamp()

        #     # study_to_clone = study_location
        #     queue_folder = wsc.get_queue_folder_removec()
        #     existing_studies = wsc.get_all_studies_for_user(user_token)
        #     logger.info("Found the following studies: " + existing_studies)

        #     logger.info(
        #         "Adding " + study_to_clone + ", using name " + new_folder_name + ", to the queue folder " + queue_folder
        #     )
        #     # copy the study onto the queue folder
        #     try:
        #         logger.info(
        #             "Attempting to copy "
        #             + study_to_clone
        #             + " to MetaboLights queue folder "
        #             + os.path.join(queue_folder, new_folder_name)
        #         )
        #         if include_raw_data:
        #             copy_tree(
        #                 study_to_clone, os.path.join(queue_folder, new_folder_name)
        #             )  # copy the folder to the queue
        #             # There is a bug in copy_tree which prevents you to use the same destination folder twice
        #         else:
        #             copy_files_and_folders(
        #                 study_to_clone,
        #                 os.path.join(queue_folder, new_folder_name),
        #                 include_raw_data=include_raw_data,
        #                 include_investigation_file=True,
        #             )
        #     except:
        #         return {"error": "Could not add study into the MetaboLights queue"}

        #     logger.info("Folder successfully added to the queue")
        #     # get a list of the users private studies to see if a new study has been created
        #     new_studies = wsc.get_all_studies_for_user(user_token)
        #     number = 0
        #     while existing_studies == new_studies:
        #         number = number + 1
        #         if number == 20:  # wait for 20 secounds for the MetaboLights queue to process the study
        #             logger.info("Waited to long for the MetaboLights queue, waiting for email now")
        #             abort(408)

        #         logger.info("Checking if the new study has been processed by the queue")
        #         time.sleep(3)  # Have to check every so many secounds to see if the queue has finished
        #         new_studies = wsc.get_all_studies_for_user(user_token)

        #     logger.info("Ok, now there is a new private study for the user")

        #     # Tidy up the response strings before converting to lists
        #     new_studies_list = new_studies.replace("[", "").replace("]", "").replace('"', "").split(",")
        #     existing_studies_list = existing_studies.replace("[", "").replace("]", "").replace('"', "").split(",")

        #     logger.info("returning the new study, %s", user_token)
        #     # return the new entry, i.e. difference between the two lists
        #     diff = list(set(new_studies_list) - set(existing_studies_list))

        #     study_id = diff[0]
        # else:  # User proved an existing study to clone into
        #     # Can the user read the study requested?
        #     (
        #         is_curator,
        #         read_access,
        #         write_access,
        #         obfuscation_code,
        #         study_location,
        #         release_date,
        #         submission_date,
        #         study_status,
        #     ) = wsc.get_permissions(to_study_id, user_token)

        #     # Can the user write into the given study?
        #     if not write_access:
        #         abort(403)

        #     copy_files_and_folders(
        #         study_to_clone, study_location, include_raw_data=include_raw_data, include_investigation_file=True
        #     )

        #     study_id = to_study_id  # Now we need to work with the new folder, not the study to clone from



        # make_dir_with_chmod(log_path, 0o777)

        # # Create an upload folder for all studies anyway
        # status = wsc.create_upload_folder(study_id, obfuscation_code, user_token)
        # upload_location = status["upload_location"]
        # return {"new_study": study_id, "upload_location": upload_location}
        raise MetabolightsException(message="Not implemented")


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
                "name": "user_token",
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
            {"code": 403, "message": "Study does not exist or your do not have access to this study."},
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
        ],
    )
    def post(self, study_id):
        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None or study_id is None:
            abort(401)

        study_id = study_id.upper()

        # param validation
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(
                401,
                message="Unauthorized. Access to the resource requires user authentication. "
                "Please provide a study id and a valid user token",
            )

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
                "name": "user_token",
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
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
        ],
    )
    def post(self, study_id):
        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None or study_id is None:
            abort(404)

        study_id = study_id.upper()

        # param validation
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(
                401,
                message="Unauthorized. Write access to the resource requires user authentication. "
                "Please provide a study id and a valid user token",
            )

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
                "name": "user_token",
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
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
        ],
    )
    def get(self, study_id):
        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None or study_id is None:
            abort(404)
        study_id = study_id.upper()

        # param validation
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(
                401,
                message="Unauthorized. Read access to the resource requires user authentication. "
                "Please provide a study id and a valid user token",
            )

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
            {"code": 401, "message": "Unauthorized. Access to the resource requires user authentication."},
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token",
            },
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
        ],
    )
    @metabolights_exception_handler
    def get(self, study_id):
        log_request(request)

        if not study_id:
            abort(401)

        with DBManager.get_instance().session_maker() as db_session:
            query = db_session.query(Study)
            query = query.filter(Study.status == StudyStatus.PUBLIC.value, Study.acc == study_id)
            study = query.first()

            if not study:
                raise MetabolightsDBException(f"{study_id} does not exist or is not public")

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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "study_id",
                "description": "Requested study id (Leave it empty for new study id)",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {"code": 401, "message": "Unauthorized. Access to the resource requires user authentication."},
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token",
            },
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
        ],
    )
    @metabolights_exception_handler
    def get(self):
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        user: User = UserService.get_instance().validate_user_has_submitter_or_super_user_role(user_token)
        studies = UserService.get_instance().get_user_studies(user.apitoken)
        provisional_studies = []
        last_study_datetime = datetime.fromtimestamp(0)
        for study in studies:
            if study.status == StudyStatus.PROVISIONAL.value:
                provisional_studies.append(study)
            if study.submissiondate.timestamp() > last_study_datetime.timestamp():
                last_study_datetime = study.submissiondate
        study_settings = get_study_settings()
        now = current_utc_time_without_timezone()
        if (now - last_study_datetime).total_seconds() < study_settings.min_study_creation_interval_in_mins * 60:
            logger.warning(
                f"New study creation request from user {user.username} in {study_settings.min_study_creation_interval_in_mins} mins"
            )
            raise MetabolightsException(message="Submitter can create only one study in five minutes.", http_code=429)

        if (
            len(provisional_studies) >= study_settings.max_study_in_provisional_status
            and user.role != UserRole.ROLE_SUPER_USER.value
            and user.role != UserRole.SYSTEM_ADMIN.value
            and user.partner > 0
        ):
            logger.warning(
                f"New study creation request from user {user.username}. User has already {study_settings.max_study_in_provisional_status} study in Provisional status."
            )
            raise MetabolightsException(
                message="The user can have at most two studies in Provisional status. Please complete and update status of your current studies.",
                http_code=400,
            )

        logger.info(f"Step 1: New study creation request is received from user {user.username}")
        new_accession_number = True
        study_acc: Union[None, str] = None
        if "study_id" in request.headers:
            requested_study_id = request.headers["study_id"]
            study_acc = self.validate_requested_study_id(requested_study_id, user_token)
            if study_acc:
                new_accession_number = False
                logger.warning(
                    f"A previous study creation request from the user {user.username}. The study {study_acc} will be created."
                )

        try:
            study_acc = create_empty_study(user_token, study_id=study_acc)
            # study = StudyService.get_instance().get_study_by_acc(study_id=study_acc)

            if study_acc:
                logger.info(f"Step 2: Study id {study_acc} is created on DB.")
            else:
                raise MetabolightsException(message="Could not create a new study in db", http_code=503)
        except Exception as exc:
            inputs = {
                "subject": "Study id creation on DB was failed.",
                "body": f"Study id on db creation was failed: folder: {study_acc}, user: {user.username} <p> {str(exc)}",
            }
            send_technical_issue_email.apply_async(kwargs=inputs)
            logger.error(f"Study id creation on DB was failed. Temp folder: {study_acc}. {str(inputs)}")
            if isinstance(exc, MetabolightsException):
                raise exc
            else:
                raise MetabolightsException(message="Study id creation on db was failed.", http_code=501, exception=exc)

        inputs = {"user_token": user_token, "study_id": study_acc, "send_email_to_submitter": False, "task_name": "INITIAL_METADATA", 
                  "maintain_metadata_storage": True, "maintain_data_storage": False, "maintain_private_ftp_storage": False}
        try:
            maintain_storage_study_folders(**inputs)
            logger.info(f"Step 4.1: 'Create initial files and folders' task completed for study {study_acc}")
        except Exception as exc:
            logger.info(f"Step 4.1: 'Create initial files and folders' task failed for study {study_acc}. {str(exc)}")
            
        # Start ftp folder creation task
        inputs.update({"maintain_metadata_storage": False, "maintain_data_storage": True, "maintain_private_ftp_storage": False,  "task_name": "INITIAL_DATA"})
        create_study_data_folders_task = maintain_storage_study_folders.apply_async(kwargs=inputs)
        logger.info(f"Step 4.2: 'Create study data files and folders' task has been started for study {study_acc} with task id: {create_study_data_folders_task.id}")
        
        inputs.update({"maintain_metadata_storage": False, "maintain_data_storage": False, "maintain_private_ftp_storage": True,  "task_name": "INITIAL_FTP_FOLDERS"})
        create_ftp_folders_task = maintain_storage_study_folders.apply_async(kwargs=inputs)
        logger.info(f"Step 4.3: 'Create study FTP folders' task has been started for study {study_acc} with task id: {create_ftp_folders_task.id}")

        if new_accession_number:
            study: Study = StudyService.get_instance().get_study_by_acc(study_acc)
            ftp_folder_name = study_acc.lower() + "-" + study.obfuscationcode
            inputs = {"user_token": user_token, "study_id": study_acc, "folder_name": ftp_folder_name}
            send_email_for_new_provisional_study.apply_async(kwargs=inputs)
            logger.info(f"Step 5: Sending FTP folder email for the study {study_acc}")
        else:
            logger.info(f"Step 5: Skipping FTP folder email for the study {study_acc}")

        # # Start ftp folder creation task

        # inputs = {"user_token": user_token, "study_id": study_acc, "send_email": new_accession_number}

        # create_ftp_folder_task = create_private_ftp_folder.apply_async(kwargs=inputs)

        # logger.info(f"Step 6: Create ftp folder task is started for study {study_acc} with task id: {create_ftp_folder_task.id}")

        # Start reindex task
        inputs = {"user_token": user_token, "study_id": study_acc}
        reindex_task = reindex_study.apply_async(kwargs=inputs)
        logger.info(f"Step 6: Reindex task is started for study {study_acc} with task id: {reindex_task.id}")

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
            abort(401, message="Invalid provisional id format. Provisional id must start with %s" % study_id_prefix)
        # Rule 2
        UserService.get_instance().validate_user_has_curator_role(user_token)
        # Rule 3
        # disable this rule for provisional id
        # Rule 4
        study_location = os.path.join(get_study_settings().mounted_paths.study_metadata_files_root_path, requested_study_id)
        if os.path.exists(study_location):
            files = os.listdir(study_location)
            if files:
                raise MetabolightsException(message="Study folder is already exist", http_code=400)
        # Rule 5
        try:
            id_list = get_id_list_by_req_id(requested_study_id)
        except Exception as ex:
            raise MetabolightsException(message=f"Requested id is not valid in db. {str(ex)}", http_code=400)
            
        if id_list:
            raise MetabolightsException(message="Requested id already used in DB.", http_code=400)
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {"code": 401, "message": "Unauthorized. Access to the resource requires user authentication."},
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token",
            },
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
        ],
    )
    def delete(self, study_id):
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if not user_token:
            abort(404)

        if user_token is None or study_id is None:
            abort(401)

        study_id = study_id.upper()

        UserService.get_instance().validate_user_has_curator_role(user_token)
        study: Study = StudyService.get_instance().get_study_by_acc(study_id)
        status = StudyStatus(study.status)
        if status == StudyStatus.PUBLIC:
            abort(401, message="It is not allowed to delete a public study")
            
        logger.info("Deleting study " + study_id)

        # Remove the submitter from the study
        submitter_emails = query_study_submitters(study_id)
        if submitter_emails:
            for submitter in submitter_emails:
                study_submitters(study_id, submitter[0], "delete")

        # Add the placeholder flag and MetaboLights user to the study
        mtbls_email = get_settings().auth.service_account.email
        add_placeholder_flag(study_id)
        study_submitters(study_id, mtbls_email, "add")
        inputs = {"user_token": user_token, "study_id": study_id, "task_name": "DELETE_STUDY"}
        cluster_settings = get_cluster_settings()
        task = delete_study_folders.apply_async(kwargs=inputs)
        output = task.get(timeout=cluster_settings.task_get_timeout_in_seconds*2)

        status, message = wsc.reindex_study(study_id, user_token)
        if not status:
            abort(500, error="Could not reindex the study")

        return {"Success": "Study " + study_id + " has been removed"}


def get_audit_files(study_location):
    folder_list = []
    settings = get_study_settings()
    audit_path = os.path.join(study_location, settings.audit_files_symbolic_link_name)

    try:
        folder_list = os.listdir(os.path.join(audit_path))
    except:
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
                "name": "user_token",
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
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
            {"code": 417, "message": "Unexpected result."},
        ],
    )
    @metabolights_exception_handler
    def post(self, study_id):
        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None or study_id is None:
            abort(404)

        study_id = study_id.upper()

        parser = reqparse.RequestParser()
        parser.add_argument("include_validation_results")
        include_validation_results = True
        if request.args:
            args = parser.parse_args(req=request)
            include_validation_results = True if args["include_validation_results"].lower() == "true" else False

        # param validation
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(401)

        status, message = wsc.reindex_study(
            study_id, user_token, include_validation_results=include_validation_results, sync=True
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {"code": 401, "message": "Unauthorized. Access to the resource requires user authentication."},
            {"code": 403, "message": "Forbidden. Access to the study is not allowed for this user."},
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
        ],
    )
    @metabolights_exception_handler
    def delete(self, study_id):
        log_request(request)
        if not study_id:
            logger.info("No study_id given")
            abort(404)
        compound_id = study_id.upper()

        # User authentication
        user_token = ""
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info("Deleting a compound")

        result = delete_study_index(user_token, compound_id)

        result = {"content": result, "message": None, "err": None}
        return result


class UnindexedStudy(Resource):
    @swagger.operation(
        summary="Gets unindexed studies from database (curator only)",
        notes="""Gets MetaboLights studies that should be updated for the up-to-date search index""",
        parameters=[
            {
                "name": "user_token",
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
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
            {"code": 417, "message": "Unexpected result."},
        ],
    )
    @metabolights_exception_handler
    def get(self):
        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_curator_role(user_token)
        try:
            with DBManager.get_instance().session_maker() as db_session:
                query = db_session.query(StudyTask)
                filtered = query.filter(
                    StudyTask.last_execution_status != StudyTaskStatus.EXECUTION_SUCCESSFUL,
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
                message=f"Error while retreiving study tasks from database: {str(e)}", exception=e
            )


class RetryReindexStudies(Resource):
    @swagger.operation(
        summary="Reindex unindexed public studies",
        parameters=[
            {
                "name": "user_token",
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
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
            {"code": 417, "message": "Unexpected result."},
        ],
    )
    @metabolights_exception_handler
    def post(self):
        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_curator_role(user_token)
        try:
            with DBManager.get_instance().session_maker() as db_session:
                query = db_session.query(StudyTask)
                filtered = query.filter(
                    StudyTask.last_execution_status != StudyTaskStatus.EXECUTION_SUCCESSFUL,
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
                        wsc.reindex_study(task.study_acc, user_token)
                        indexed_studies.append(task.study_acc)
                        logger.info(f"Indexed study {task.study_acc}")
                    except Exception as e:
                        unindexed_studies.append({"study_id": task.study_acc, "message": str(e)})
                        logger.info(f"Unindexed study {task.study_acc}")

                return {"indexed_studies": indexed_studies, "unindexed_studies": unindexed_studies}

        except Exception as e:
            raise MetabolightsDBException(
                message=f"Error while retreiving study tasks from database: {str(e)}", exception=e
            )


class MtblsPublicStudiesIndexAll(Resource):
    @swagger.operation(
        summary="Index all public studies ",
        notes="Start a task to index all public studies and return task id. Result will be sent by email.",
        parameters=[
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=[
            {"code": 200, "message": "OK. The compound is returned"},
            {"code": 401, "message": "Unauthorized. Access to the resource requires user authentication."},
            {"code": 403, "message": "Forbidden. Access to the study is not allowed for this user."},
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
        ],
    )
    @metabolights_exception_handler
    def post(self):
        log_request(request)

        # User authentication
        user_token = ""
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        UserService.get_instance().validate_user_has_curator_role(user_token)
        logger.info("Indexing public studies")
        inputs = {"user_token": user_token, "send_email_to_submitter": True}
        try:
            result = reindex_all_public_studies.apply_async(kwargs=inputs, expires=60 * 5)

            result = {
                "content": f"Task has been started. Result will be sent by email. Task id: {result.id}",
                "message": None,
                "err": None,
            }
            return result
        except Exception as ex:
            raise MetabolightsException(http_code=500, message=f"Task submission was failed: {str(ex)}", exception=ex)


class MtblsStudiesIndexAll(Resource):
    @swagger.operation(
        summary="Index all studies ",
        notes="Start a task to index all studies and return task id. Result will be sent by email.",
        parameters=[
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=[
            {"code": 200, "message": "OK. The compound is returned"},
            {"code": 401, "message": "Unauthorized. Access to the resource requires user authentication."},
            {"code": 403, "message": "Forbidden. Access to the study is not allowed for this user."},
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
        ],
    )
    @metabolights_exception_handler
    def post(self):
        log_request(request)

        # User authentication
        user_token = ""
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info(f"Indexing studies.")
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
            raise MetabolightsException(http_code=500, message=f"Task submission was failed: {str(ex)}", exception=ex)


class MtblsStudiesIndexSync(Resource):
    @swagger.operation(
        summary="Sync all studies on database and elasticsearch",
        notes="Start a task to sync all studies on database and elasticsearch, and return task id. Result will be sent by email.",
        parameters=[
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=[
            {"code": 200, "message": "OK. The compound is returned"},
            {"code": 401, "message": "Unauthorized. Access to the resource requires user authentication."},
            {"code": 403, "message": "Forbidden. Access to the study is not allowed for this user."},
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
        ],
    )
    @metabolights_exception_handler
    def post(self):
        log_request(request)

        # User authentication
        user_token = ""
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info("Indexing missing/out-of-date studies.")
        inputs = {"user_token": user_token, "send_email_to_submitter": True}
        try:
            result = sync_studies_on_es_and_db.apply_async(kwargs=inputs, expires=60 * 5)

            result = {
                "content": f"Task has been started. Result will be sent by email. Task id: {result.id}",
                "message": None,
                "err": None,
            }
            return result
        except Exception as ex:
            raise MetabolightsException(http_code=500, message=f"Task submission was failed: {str(ex)}", exception=ex)


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
                "name": "target_folder",
                "description": "Select target study folders: metadata, data and private ftp folder",
                "required": False,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
                "enum": ["metadata", "data", "private-ftp"],
                "allowEmptyValue": False,
                "defaultValue": "metadata",
                "default": "metadata"
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
                "name": "task_name",
                "description": "Any task name, backup folders created with this name",
                "required": False,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
                "allowEmptyValue": False,
                "defaultValue": "MAINTAIN",
                "default": "MAINTAIN"
            },
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK. The compound is returned"},
            {"code": 401, "message": "Unauthorized. Access to the resource requires user authentication."},
            {"code": 403, "message": "Forbidden. Access to the study is not allowed for this user."},
            {"code": 404, "message": "Not found. The requested identifier is not valid or does not exist."},
        ],
    )
    @metabolights_exception_handler
    def post(self, study_id):
        log_request(request)

        # User authentication
        user_token = ""
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

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
        maintain_data_storage = False
        maintain_private_ftp_storage = False
        if target_folder == "metadata":
            maintain_metadata_storage = True
        elif target_folder == "data":
            maintain_data_storage = True
        elif target_folder == "private-ftp":
            maintain_private_ftp_storage = True
            
        parser = reqparse.RequestParser()
        parser.add_argument("force", help="force to maintain", location="args")
        args = parser.parse_args()
        force_to_maintain = False
        if args and "force" in args and args["force"]:
            force_to_maintain = True if args["force"].lower() == "true" else False

        logger.info("Searching study folders")
        try:
            inputs = {
                "user_token": user_token,
                "send_email_to_submitter": True,
                "study_id": study_id,
                "force_to_maintain": force_to_maintain,
                "task_name": task_name,
                "maintain_metadata_storage": maintain_metadata_storage,
                "maintain_data_storage": maintain_data_storage,
                "maintain_private_ftp_storage": maintain_private_ftp_storage
            }
            task = maintain_storage_study_folders.apply_async(kwargs=inputs)

            result = {"content": f"Task has been started. Result will be sent by email with task id {task.id}", "message": None, "err": None}
            return result
        except Exception as ex:
            raise MetabolightsException(http_code=500, message=f"Task submission was failed: {str(ex)}", exception=ex)



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
                "dataType": "string"
            },
            {
                "name": "source_staging_area",
                "description": "Source study folder stage",
                "required": True,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
                "enum": ["private-ftp", "readonly-study", "rw-study", "public-ftp"],
                "allowEmptyValue": False,
                "defaultValue": "private-ftp",
                "default": "private-ftp"
            },
            {
                "name": "target_staging_area",
                "description": "Target study folder stage",
                "required": True,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
                "enum": ["private-ftp", "readonly-study", "rw-study", "public-ftp"],
                "allowEmptyValue": False,
                "defaultValue": "rw-study",
                "default": "rw-study"
            },
            {
                "name": "sync_type",
                "description": "Sync category: sync metadada or data or internal files",
                "required": True,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
                "enum": ["metadata", "data", "internal", "public-metadata-versions", "integrity-check", "audit"],
                "allowEmptyValue": False,
                "defaultValue": "metadata",
                "default": "metadata"
            },
            {
                "name": "dry_run",
                "description": "Only check whether there is a difference ",
                "required": False,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "Boolean",
                "allowEmptyValue": False,
                "defaultValue": True,
                "default": True
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
    def post(self, study_id):
        log_request(request)
        return self.proces_rsync_request(study_id, request, start_new_task=True)

    @swagger.operation(
        summary="(Curator Only) Returns current rsync task status.",
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
                "name": "source_staging_area",
                "description": "Source study folder stage",
                "required": True,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
                "enum": ["private-ftp", "readonly-study", "rw-study", "public-ftp"],
                "allowEmptyValue": False,
                "defaultValue": "private-ftp",
                "default": "private-ftp"
            },
            {
                "name": "target_staging_area",
                "description": "Target study folder stage",
                "required": True,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
                "enum": ["private-ftp", "readonly-study", "rw-study", "public-ftp"],
                "allowEmptyValue": False,
                "defaultValue": "rw-study",
                "default": "rw-study"
            },
            {
                "name": "sync_type",
                "description": "Sync category: sync metadada or data or internal files",
                "required": True,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
                "enum": ["metadata", "data", "internal", "public-metadata-versions", "integrity-check", "audit"],
                "allowEmptyValue": False,
                "defaultValue": "metadata",
                "default": "metadata"
            },
            {
                "name": "dry_run",
                "description": "Only check whether there is a difference ",
                "required": False,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "Boolean",
                "allowEmptyValue": False,
                "defaultValue": True,
                "default": True
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
    def get(self, study_id):
        log_request(request)
        return self.proces_rsync_request(study_id, request, start_new_task=False)    
    
    def proces_rsync_request(self, study_id: str, request, start_new_task:bool = False):
        # param validation
        if study_id is None:
            raise MetabolightsException(message='Please provide valid parameter for study identifier')
        study_id = study_id.upper()
        sync_type = "metadata"
        
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
            
        dry_run = True
        if "dry_run" in request.headers:
            dry_run = True if request.headers["dry_run"].lower() == "true" else False 
        
        if "sync_type" in request.headers:
            sync_type = request.headers["sync_type"]
            
        source_staging_area = "private-ftp"
        if "source_staging_area" in request.headers:
            source_staging_area = request.headers["source_staging_area"]
        
        target_staging_area = "rw_study"
        if "target_staging_area" in request.headers:
            target_staging_area = request.headers["target_staging_area"]
        if target_staging_area == source_staging_area:
            raise MetabolightsException(message="Source and target staging areas should be different.") 
                    
        if sync_type not in ("metadata", "data", "internal", "public-metadata-versions", "integrity-check", "audit"):
            raise MetabolightsException(message="sync_type is invalid.")
        
        staging_areas = ["private-ftp", "readonly-study", "rw-study", "public-ftp"]
        if source_staging_area not in staging_areas:
            raise MetabolightsException(message="Source staging area is invalid.")
        
        if target_staging_area not in staging_areas:
            raise MetabolightsException(message="Target staging area is invalid.")
        
        if source_staging_area == "private-ftp" and ((sync_type == "data" and target_staging_area == "readonly-study") or (sync_type == "metadata" and target_staging_area == "rw-study")):
            study = StudyService.get_instance().get_study_by_acc(study_id)
            UserService.get_instance().validate_user_has_write_access(user_token, study_id)
            if StudyStatus(study.status) != StudyStatus.PROVISIONAL:
                UserService.get_instance().validate_user_has_curator_role(user_token)
        else:
            UserService.get_instance().validate_user_has_curator_role(user_token)
        
        folder_type = StudyFolderType(sync_type.replace("-","_").upper())
        source_location = StudyFolderLocation(f"{source_staging_area.replace('-','_').upper()}_STORAGE")
        target_location = StudyFolderLocation(f"{target_staging_area.replace('-','_').upper()}_STORAGE")
        source = StudyFolder(location=source_location, folder_type=folder_type)
        target = StudyFolder(location=target_location, folder_type=folder_type)

        if not source.folder_type in VALID_FOLDERS[source_location]:
            raise MetabolightsException(message="Source folder type is not valid in the selected staging area.")

        if not target.folder_type in VALID_FOLDERS[target_location]:
            raise MetabolightsException(message="target folder type is not valid in the selected staging area.")
        
        study = StudyService.get_instance().get_study_by_acc(study_id)
        client = StudyRsyncClient(study_id=study_id, obfuscation_code=study.obfuscationcode)
        status_check_only = not start_new_task
        if dry_run:
            status: SyncTaskResult = client.rsync_dry_run(source, target, status_check_only=status_check_only)
        else:
            if start_new_task and target.folder_type == StudyFolderType.METADATA and target.location == StudyFolderLocation.RW_STUDY_STORAGE:
                write_audit_files(study_id)
            status: SyncTaskResult = client.rsync(source, target, status_check_only=status_check_only)

        status.description = f"{status.description[:100]} ..." if status.description and len(status.description) > 100 else status.description
        return status.model_dump()
    

    