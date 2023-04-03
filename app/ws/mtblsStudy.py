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


from datetime import datetime
import glob
import logging
import os
import time
from distutils.dir_util import copy_tree
import uuid

from flask import request, send_file, current_app as app, jsonify
from flask_restful import Resource, reqparse, abort
from flask_restful_swagger import swagger

from app.file_utils import make_dir_with_chmod
from app.services.storage_service.storage_service import StorageService
from app.tasks.periodic_tasks.study import sync_studies_on_es_and_db
from app.tasks.periodic_tasks.study_folder import maintain_study_folders
from app.utils import MetabolightsException, metabolights_exception_handler, MetabolightsDBException
from app.ws import db_connection as db_proxy
from app.ws.db.dbmanager import DBManager
from app.ws.db.models import StudyTaskModel
from app.ws.db.schemes import Study, StudyTask
from app.ws.db.settings import get_directory_settings
from app.ws.db.types import StudyStatus, StudyTaskName, StudyTaskStatus, UserRole
from app.ws.db.wrappers import create_study_model_from_db_study, update_study_model_from_directory
from app.ws.db_connection import get_all_studies_for_user, study_submitters, add_placeholder_flag, \
    query_study_submitters, get_public_studies_with_methods, get_all_private_studies_for_user, get_obfuscation_code, \
    create_empty_study
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.settings.utils import get_study_settings
from app.ws.study.folder_utils import write_audit_files
from app.ws.study.study_service import StudyService
from app.ws.study.user_service import UserService
from app.ws.study_folder_utils import create_initial_study_folder, update_initial_study_files
from app.ws.study_utilities import StudyUtils
from app.tasks.common.elasticsearch import delete_study_index, reindex_all_public_studies, reindex_all_studies, reindex_study
from app.tasks.common.email import send_email_for_study_submitted, send_technical_issue_email
from app.tasks.common.ftp_operations import create_private_ftp_folder
from app.ws.utils import get_year_plus_one, remove_file, copy_file, get_timestamp, copy_files_and_folders, \
    log_request

logger = logging.getLogger('wslog')
wsc = WsClient()
iac = IsaApiClient()


class MtblsStudies(Resource):
    @swagger.operation(
        summary="Get all public studies",
        notes="Get a list of all public Studies.",
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self):
        log_request(request)
        pub_list = wsc.get_public_studies()
        return jsonify(pub_list)


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
    def get(self):
        log_request(request)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance(app).validate_user_has_curator_role(user_token)

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
                "dataType": "string"
            },
            {
                "name": "validation_status",
                "description": "status of validation",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
                "enum": ["error", "warn", "success"]
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
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        result = db_proxy.update_validation_status(study_id=study_id, validation_status=validation_status)

        return jsonify(result)

class MtblsStudiesWithMethods(Resource):
    @swagger.operation(
        summary="Get all public studies, with technology used",
        notes="Get a list of all public Studies, with the technology used.",
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self):
        log_request(request)
        logger.info('Getting all public studies (with methods)')
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
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self):
        log_request(request)

        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']

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
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self):
        log_request(request)

        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']

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
                "dataType": "string"
            },
            {
                "name": "investigation_filename",
                "description": "Investigation filename",
                "required": False,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "version",
                "description": "Version of Investigation file (audit record)",
                "required": False,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
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
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(401, message="Missing study_id")

        study_id = study_id.upper()

        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']

        if user_token is None:
            abort(401, message="Missing user_token")

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('investigation_filename', help='Investigation filename')
        parser.add_argument('version', help='Version of metadata/Audit record')
        study_version = None
        inv_filename = None
        if request.args:
            args = parser.parse_args(req=request)
            inv_filename = args['investigation_filename'].lower() if args['investigation_filename'] else None
            study_version = args['version'].lower() if args['version'] else None
        if not inv_filename:
            logger.warning("Missing Investigation filename. Using default i_Investigation.txt")
            inv_filename = 'i_Investigation.txt'

        if study_version:
            logger.info("Loading version " + study_version + " of the metadata")
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403, message="Study does not exist or your do not have access to this study")

        logger.info('Getting ISA-Tab Investigation file for %s', study_id)
        location = study_location
        if study_version:
            audit = os.path.join(app.config.get('UPDATE_PATH_SUFFIX'), study_version)
            location = os.path.join(study_location, audit)

        files = glob.glob(os.path.join(location, inv_filename))
        if files:
            file_path = files[0]
            filename = os.path.basename(file_path)
            try:
                return send_file(file_path, cache_timeout=-1,
                                 as_attachment=True, attachment_filename=filename)
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
                "dataType": "string"
            },
            {
                "name": "sample_filename",
                "description": "Sample filename",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
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
                "message": "Study does not exist or your do not have access to this study."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']

        if user_token is None:
            abort(401)

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('sample_filename', help='Sample filename')
        sample_filename = None
        if request.args:
            args = parser.parse_args(req=request)
            sample_filename = args['sample_filename']
            # sample_filename = sample_filename.lower() if args['sample_filename'] else None
        if not sample_filename:
            logger.warning("Missing Sample filename.")
            abort(404, message="Missing Sample filename.")

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(401, message="Study does not exist or your do not have access to this study.")

        logger.info('Getting ISA-Tab Sample file %s for %s', sample_filename, study_id)
        location = study_location
        files = glob.glob(os.path.join(location, sample_filename))
        if files:
            file_path = files[0]
            filename = os.path.basename(file_path)
            try:
                return send_file(file_path, cache_timeout=-1,
                                 as_attachment=True, attachment_filename=filename)
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
                "dataType": "string"
            },
            {
                "name": "assay_filename",
                "description": "Assay filename",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
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
                "message": "Study does not exist or your do not have access to this study."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']

        if user_token is None:
            abort(401)

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('assay_filename', help='Assay filename')
        assay_filename = None
        if request.args:
            args = parser.parse_args(req=request)
            assay_filename = args['assay_filename'] if args['assay_filename'] else None
        if not assay_filename:
            logger.warning("Missing Assay filename.")
            abort(404, message="Missing Assay filename.")

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(401, message="Study does not exist or your do not have access to this study.")

        logger.info('Getting ISA-Tab Assay file for %s', study_id)
        location = study_location

        files = glob.glob(os.path.join(location, assay_filename))
        if files:
            file_path = files[0]
            filename = os.path.basename(file_path)
            try:
                return send_file(file_path, cache_timeout=-1,
                                 as_attachment=True, attachment_filename=filename)
            except OSError as err:
                logger.error(err)
                abort(404, message="Wrong assay filename or file could not be read.")
        else:
            abort(404, message="Wrong assay filename or file could not be read.")


class CloneAccession(Resource):
    @swagger.operation(
        summary="Create a new study and upload folder",
        notes='''This will clone the default template LC-MS study if no "study_id" parameter is given
        If a "to_study_id" (destination study id) is provided, <b>data will be copied into this *existing* study. 
        Please be aware that data in the destination study will be replaced with metadata files from "study_id"</b>.''',
        parameters=[
            {
                "name": "study_id",
                "description": "Existing Study to clone from",
                "required": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "to_study_id",
                "description": "Existing Study to clone into",
                "required": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "include_raw_data",
                "description": "Include raw data when cloning a study.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
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
                "message": "OK."
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
            },
            {
                "code": 408,
                "message": "Request Timeout. The MetaboLights queue took too long to complete."
            }
        ]
    )
    def post(self):

        # User authentication
        user_token = None
        include_raw_data = False
        bypass = False
        lcms_default_study = 'MTBLS121'  # This is the standard LC-MS study. This is private but safe for all to clone

        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None:
            abort(401)

        if "include_raw_data" in request.headers and request.headers["include_raw_data"].lower() == 'true':
            include_raw_data = True

        parser = reqparse.RequestParser()
        parser.add_argument('study_id', help="Study Identifier")
        parser.add_argument('to_study_id', help="Study Identifier")
        study_id = None
        to_study_id = None

        if request.args:
            args = parser.parse_args(req=request)
            study_id = args['study_id']
            to_study_id = args['to_study_id']

        # param validation
        if study_id is None:
            study_id = lcms_default_study

        if study_id is lcms_default_study:
            bypass = True  # Users can safely clone this study, even when passing in MTBLS121

        # Can the user read the study requested?
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)

        if not bypass:
            if not read_access:
                abort(401, message="Study does not exist or your do not have access to this study.")

        study_id = study_id.upper()

        # This is the existing study
        study_to_clone = study_location

        # If the user did not provide an existing study id to clone into, we create a new study
        if to_study_id is None:
            study_date = get_year_plus_one()
            logger.info('Creating a new MTBLS Study (cloned from %s) with release date %s', study_id, study_date)
            new_folder_name = user_token + '~~' + study_date + '~' + 'new_study_requested_' + get_timestamp()

            # study_to_clone = study_location
            queue_folder = wsc.get_queue_folder()
            existing_studies = wsc.get_all_studies_for_user(user_token)
            logger.info('Found the following studies: ' + existing_studies)

            logger.info('Adding ' + study_to_clone + ', using name ' + new_folder_name +
                        ', to the queue folder ' + queue_folder)
            # copy the study onto the queue folder
            try:
                logger.info('Attempting to copy ' + study_to_clone + ' to MetaboLights queue folder ' +
                            os.path.join(queue_folder, new_folder_name))
                if include_raw_data:
                    copy_tree(study_to_clone,
                              os.path.join(queue_folder, new_folder_name))  # copy the folder to the queue
                    # There is a bug in copy_tree which prevents you to use the same destination folder twice
                else:
                    copy_files_and_folders(study_to_clone,
                                           os.path.join(queue_folder, new_folder_name),
                                           include_raw_data=include_raw_data, include_investigation_file=True)
            except:
                return {"error": "Could not add study into the MetaboLights queue"}

            logger.info('Folder successfully added to the queue')
            # get a list of the users private studies to see if a new study has been created
            new_studies = wsc.get_all_studies_for_user(user_token)
            number = 0
            while existing_studies == new_studies:
                number = number + 1
                if number == 20:  # wait for 20 secounds for the MetaboLights queue to process the study
                    logger.info('Waited to long for the MetaboLights queue, waiting for email now')
                    abort(408)

                logger.info('Checking if the new study has been processed by the queue')
                time.sleep(3)  # Have to check every so many secounds to see if the queue has finished
                new_studies = wsc.get_all_studies_for_user(user_token)

            logger.info('Ok, now there is a new private study for the user')

            # Tidy up the response strings before converting to lists
            new_studies_list = new_studies.replace('[', '').replace(']', '').replace('"', '').split(',')
            existing_studies_list = existing_studies.replace('[', '').replace(']', '').replace('"', '').split(',')

            logger.info('returning the new study, %s', user_token)
            # return the new entry, i.e. difference between the two lists
            diff = list(set(new_studies_list) - set(existing_studies_list))

            study_id = diff[0]
        else:  # User proved an existing study to clone into
            # Can the user read the study requested?
            is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(to_study_id, user_token)

            # Can the user write into the given study?
            if not write_access:
                abort(403)

            copy_files_and_folders(study_to_clone, study_location,
                                   include_raw_data=include_raw_data, include_investigation_file=True)

            study_id = to_study_id  # Now we need to work with the new folder, not the study to clone from

        new_study_location = os.path.join(app.config.get('STUDY_PATH'), study_id)

        log_path = os.path.join(new_study_location, app.config.get('UPDATE_PATH_SUFFIX'), 'logs')
        make_dir_with_chmod(log_path, 0o777)

        # Create an upload folder for all studies anyway
        status = wsc.create_upload_folder(study_id, obfuscation_code, user_token)
        upload_location = status["upload_location"]
        return {'new_study': study_id, 'upload_location': upload_location}


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
                "message": "Unauthorized. Access to the resource requires user authentication. "
                           "Please provide a study id and a valid user token"
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
    def post(self, study_id):

        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None or study_id is None:
            abort(401)

        study_id = study_id.upper()

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(401, message="Unauthorized. Access to the resource requires user authentication. "
                               "Please provide a study id and a valid user token")

        logger.info('Creating a new study upload folder for study %s', study_id)
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
    def post(self, study_id):

        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None or study_id is None:
            abort(404)

        study_id = study_id.upper()

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(401, message="Unauthorized. Write access to the resource requires user authentication. "
                               "Please provide a study id and a valid user token")

        logger.info('Creating a new study audit folder for study %s', study_id)
        status, dest_path = write_audit_files(study_location)

        if status:
            return {'Success': 'Created audit record for ' + study_id}
        else:
            return {'Error': 'Failed to create audit folder ' + dest_path}

    @swagger.operation(
        summary="Get an overview of the available audit folders for a study",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier to retrieve audit record names",
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
    def get(self, study_id):
        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None or study_id is None:
            abort(404)
        study_id = study_id.upper()

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(401, message="Unauthorized. Read access to the resource requires user authentication. "
                               "Please provide a study id and a valid user token")

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
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token"
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

        if not study_id:
            abort(401)

        with DBManager.get_instance(app).session_maker() as db_session:
            query = db_session.query(Study)
            query = query.filter(Study.status == StudyStatus.PUBLIC.value,
                                 Study.acc == study_id)
            study = query.first()

            if not study:
                raise MetabolightsDBException(f"{study_id} does not exist or is not public")

            directory_settings = get_directory_settings(app)
            study_folders = directory_settings.studies_folder
            m_study = create_study_model_from_db_study(study)

        update_study_model_from_directory(m_study, study_folders)
        dict_data = m_study.dict()
        result = {'content': dict_data, 'message': None, "err": None}
        return result

class CreateAccession(Resource):
    @swagger.operation(
        summary="Create a new study",
        notes='''Create a new study, with upload folder</br>
        Please note that this includes an empty sample file, which will require at least 
        one additional data row to be ISA-Tab compliant''',
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
                "name": "study_id",
                "description": "Requested study id",
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
                "message": "Unauthorized. Access to the resource requires user authentication."
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
    @metabolights_exception_handler
    def get(self):

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        user = UserService.get_instance(app).validate_user_has_submitter_or_super_user_role(user_token)
        studies = UserService.get_instance(app).get_user_studies(user.apitoken)
        submitted_studies = []
        last_study_datetime = datetime.fromtimestamp(0)
        for study in studies:
            if study.status == StudyStatus.SUBMITTED.value:
                submitted_studies.append(study)
            if study.submissiondate.timestamp() > last_study_datetime.timestamp():
                last_study_datetime = study.submissiondate
        study_settings = get_study_settings(app)
        if (datetime.now() - last_study_datetime).total_seconds() < study_settings.min_study_creation_interval_in_mins * 60:
            logger.warning(f"New study creation request from user {user.username} in {study_settings.min_study_creation_interval_in_mins} mins")
            raise MetabolightsException(message="Submitter can create only one study in five minutes.", http_code=429)
        
        if len(submitted_studies) >= study_settings.max_study_in_submitted_status and user.role != UserRole.ROLE_SUPER_USER.value and user.role != UserRole.SYSTEM_ADMIN.value:
            logger.warning(f"New study creation request from user {user.username}. User has already {study_settings.max_study_in_submitted_status} study in Submitted status.")
            raise MetabolightsException(message="The user can have at most two studies in Submitted status. Please complete and update status of your current studies.", http_code=400)

        new_accession_number = True
        study_acc = None
        if "study_id" in request.headers:
            requested_study_id = request.headers["study_id"]
            study_acc = self.validate_requested_study_id(requested_study_id, user_token)
            if study_acc:
                new_accession_number = False
                logger.warning(f"A previous study creation request from the user {user.username}. The study {study_acc} will be created.")

        folder_name = f"temp_study_{uuid.uuid4()}" if new_accession_number else study_acc
        study_path = None
        try:
            study_path = self.create_study_folder(folder_name)
        except Exception as exc:
            inputs = {"subject": "Study folder creation was failed.",
                      "body":f"Study folder creation was failed: {folder_name}, user: {user.username} <p> {str(exc)}"}
            send_technical_issue_email.apply_async(kwargs=inputs)
            raise MetabolightsException(message="Study folder creation was failed.", http_code=501)
        
        study_acc = create_empty_study(user_token, study_id=study_acc)
        
        if not study_acc:
            inputs = {"subject": "Failed to create new study on database", 
                      "body":f"Study creation was failed on database, user: {user.username}"}
            send_technical_issue_email.apply_async(kwargs=inputs)
            logger.error('Failed to create new study.')
            raise MetabolightsException(message="Could not create a new study in db", http_code=503)
        
        try:
            # Update and rename template files
            self.update_study_template_files(study_path, study_acc, user_token)
        except Exception as exc:
            inputs = {"subject": "Failed to update study initial files", 
                      "body":f"Study file update task was failed on study folder for study {study_acc}, user: {user.username} <p> {str(exc)}"}
            send_technical_issue_email.apply_async(kwargs=inputs)
        
        try:
            # All required steps are completed. RENAME temp study folder to study accession number
            if new_accession_number:
                root_study_path = app.config.get('STUDY_PATH')
                last_study_path = os.path.join(root_study_path, study_acc)
                os.rename(study_path, last_study_path)
        except Exception as exc:
            inputs = {"subject": "Failed to rename new study folder", 
                      "body":f"Study folder rename task was failed. Rename from {folder_name} to {study_acc}, user: {user.username} <p> {str(exc)}"}
            send_technical_issue_email.apply_async(kwargs=inputs)
                                                    
        # Send email if it is new study
        if new_accession_number:
            inputs = {"user_token": user_token, "study_id": study_acc}
            new_study_email_task = send_email_for_study_submitted.apply_async(kwargs=inputs)
            logger.info(f"Sending email for new study {study_acc} with task id: {new_study_email_task.id}")
        
        # Start ftp folder creation task
        inputs = {"user_token": user_token, "study_id": study_acc, "send_email": new_accession_number}
        create_ftp_folder_task = create_private_ftp_folder.apply_async(kwargs=inputs)
        logger.info(f"Create ftp folder task started for study {study_acc} with task id: {create_ftp_folder_task.id}")
        
        # Start reindex task
        inputs = {"user_token": user_token, "study_id": study_acc}
        reindex_task = reindex_study.apply_async(kwargs=inputs)
        logger.info(f"Reindex task started for study {study_acc} with task id: {reindex_task.id}")
            
        return {"new_study": study_acc}

    def validate_requested_study_id(self, requested_study_id, user_token):
        """
        If study_id is set, check the rules below:
        Rule 1- study_id must start with  MTBLS_STABLE_ID_PREFIX
        Rule 2- user must have superuser role
        Rule 3- study_id must be equal or less than last study id, and greater than 0
        Rule 4- study folder does not exist or is empty
        Rule 5- study_id must not be in database
        """
        # Rule 1
        study_id_prefix = app.config.get("MTBLS_STABLE_ID_PREFIX")
        if not requested_study_id.startswith(study_id_prefix):
            abort(401, message="Invalid study id format. Study id must start with %s" % study_id_prefix)
        # Rule 2
        UserService.get_instance(app).validate_user_has_curator_role(user_token)
        # Rule 3
        last_stable_id = StudyService.get_instance(app).get_next_stable_study_id()
        requested_id_str = requested_study_id.upper().replace(study_id_prefix, "")
        requested_id = None
        try:
            requested_id = int(requested_id_str)
        except:
            abort(400, message="Invalid study id")
        if requested_id:
            if not (last_stable_id >= requested_id > 0):
                raise MetabolightsException(message="Requested study id must be less then last study id", http_code=400)
        # Rule 4
        study_location = os.path.join(app.config.get('STUDY_PATH'), requested_study_id)
        if os.path.exists(study_location):
            files = os.listdir(study_location)
            if files:
                raise MetabolightsException(message="Study folder is already exist", http_code=400)
        # Rule 5
        obfuscation_code = get_obfuscation_code(requested_study_id)
        if obfuscation_code:
            raise MetabolightsException(message="Study id already used in DB.", http_code=400)
        else:
            return requested_study_id

    def update_study_template_files(self, study_folder_path, study_acc, user_token):
        update_initial_study_files(study_folder_path, study_acc, user_token)
        
    def create_study_folder(self, folder_name):
        return create_initial_study_folder(folder_name, app)
        
class DeleteStudy(Resource):
    @swagger.operation(
        summary="Delete an existing study (curator only)",
        notes='''Please note that deleting a study will release the accession number back to be reused. 
        This will be available for the MetaboLights team as a placeholder''',
        parameters=[
            {
                "name": "study_id",
                "description": "Existing Study to delete",
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
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token"
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
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

        # Need to check that the user is actually an active user, ie the user_token exists
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not is_curator:
            abort(401)

        logger.info('Deleting study ' + study_id)

        # Remove the submitter from the study
        submitter_emails = query_study_submitters(study_id)
        if submitter_emails:
            for submitter in submitter_emails:
                study_submitters(study_id, submitter[0], 'delete')

        # Add the placeholder flag and MetaboLights user to the study
        mtbls_email = app.config.get("MTBLS_SUBMITTER_EMAIL")
        add_placeholder_flag(study_id)
        study_submitters(study_id, mtbls_email, 'add')

        # Remove all files in the study folder except the sample sheet and the investigation sheet.
        if not os.path.exists(study_location):
            os.makedirs(study_location, exist_ok=True)

        template_folder = os.path.join(app.config.get('STUDY_PATH'), app.config.get('DEFAULT_TEMPLATE'))
        target_file = os.path.join(study_location, 's_{0}.txt'.format(study_id))
        if not os.path.exists(target_file):
            from_path = os.path.join(template_folder, "s_Sample.txt")
            copy_file(from_path, target_file)
        target_file = os.path.join(study_location, 's_{0}.txt'.format(study_id))
        if not os.path.exists(target_file):
            from_path = os.path.join(template_folder, "i_Investigation.txt")
            copy_file(from_path, target_file)

        files = os.listdir(study_location)
        files_to_delete = [file for file in files if StudyUtils.is_template_file(file, study_id) is False]

        for file_name in files_to_delete:
            status, message = remove_file(study_location, file_name, True)

        # Remove all files in the upload folder
        ftp_private_storage = StorageService.get_ftp_private_storage(app)
        private_ftp_study_folder = study_id.lower() + "-" + obfuscation_code
        if ftp_private_storage.remote.does_folder_exist(private_ftp_study_folder):
            ftp_private_storage.remote.delete_folder(private_ftp_study_folder)

        # ftp_private_storage.remote.create_folder(private_ftp_study_folder, acl=Acl.AUTHORIZED_READ_WRITE, exist_ok=True)
        # raw_files_folder = os.path.join(private_ftp_study_folder, 'RAW_FILES')
        # derived_files_folder = os.path.join(private_ftp_study_folder, 'DERIVED_FILES')
        # ftp_private_storage.remote.create_folder(raw_files_folder, acl=Acl.AUTHORIZED_READ_WRITE, exist_ok=True)
        # ftp_private_storage.remote.create_folder(derived_files_folder, acl=Acl.AUTHORIZED_READ_WRITE, exist_ok=True)

        # Here we want to overwrite the 2 basic files,the sample sheet and the investigation sheet
        for file_name in os.listdir(study_location):

            if file_name.startswith("i_Investigation"):
                from_path = os.path.join(app.config.get('STUDY_PATH'), app.config.get('DEFAULT_TEMPLATE'),
                                         "i_Investigation.txt")

                logger.info('Attempting to copy {0} to {1}'.format(from_path, study_location))

                copy_file(from_path, study_location + '/i_Investigation.txt')
                logger.info('Restored investigation.txt file for {0} to template state.'.format(study_id))

                StudyUtils.overwrite_investigation_file(study_location=study_location, study_id=study_id)
                logger.info(
                    'Updated investigation file with values for Study Identifier and Study File Name for study: {0}'
                        .format(study_id))
            else:
                # as there are only two files in the directory this will be the sample file.
                from_path = os.path.join(app.config.get('STUDY_PATH'), app.config.get('DEFAULT_TEMPLATE'),
                                         "s_Sample.txt")
                copy_file(from_path, study_location + '/s_{0}.txt'.format(study_id))
                logger.info('Restored sample.txt file for {0} to template state.'.format(study_id))

        status, message = wsc.reindex_study(study_id, user_token)
        if not status:
            abort(500, error="Could not reindex the study")

        return {"Success": "Study " + study_id + " has been removed"}


def get_audit_files(study_location):
    folder_list = []
    audit_path = os.path.join(study_location, app.config.get('UPDATE_PATH_SUFFIX'))

    try:
        folder_list = os.listdir(os.path.join(audit_path))
    except:
        return folder_list
    return folder_list


class ReindexStudy(Resource):
    @swagger.operation(
        summary="Reindex a MetaboLights study (curator only)",
        notes='''Reindexing a MetaboLights study to ensure the search index is up to date''',
        parameters=[
            {
                "name": "study_id",
                "description": "Existing Study Identifier to index",
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
            },
            {
                "name": "include_validation_results",
                "description": "run study validation and include in indexed data.",
                "required": False,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": 'false'
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
            },
            {
                "code": 417,
                "message": "Unexpected result."
            }
        ]
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
        parser.add_argument('include_validation_results')
        include_validation_results = True
        if request.args:
            args = parser.parse_args(req=request)
            include_validation_results = True if args['include_validation_results'].lower() == 'true' else False

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(401)

        status, message = wsc.reindex_study(study_id, user_token,
                                            include_validation_results=include_validation_results, sync=True)

        if not status:
            MetabolightsException(http_code=500, message=message)

        return {"Success": "Study " + study_id + " has been re-indexed",
                "read_access": read_access, "write_access": write_access}


    @swagger.operation(
        summary="Delete a study index ",
        parameters=[
            {
                "name": "study_id",
                "description": "Compound Identifier",
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
    def delete(self, study_id):
        log_request(request)
        if not study_id:
            logger.info('No study_id given')
            abort(404)
        compound_id = study_id.upper()

        # User authentication
        user_token = ''
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
            
        logger.info('Deleting a compound')

        result = delete_study_index(user_token, compound_id)

        result = {'content': result, 'message': None, "err": None}
        return result


class UnindexedStudy(Resource):
    @swagger.operation(
        summary="Gets unindexed studies from database (curator only)",
        notes='''Gets MetaboLights studies that should be updated for the up-to-date search index''',
        parameters=[
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
            },
            {
                "code": 417,
                "message": "Unexpected result."
            }
        ]
    )
    @metabolights_exception_handler
    def get(self):

        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance(app).validate_user_has_curator_role(user_token)
        try:
            with DBManager.get_instance(app).session_maker() as db_session:
                query = db_session.query(StudyTask)
                filtered = query.filter(StudyTask.last_execution_status != StudyTaskStatus.EXECUTION_SUCCESSFUL,
                                        StudyTask.task_name == StudyTaskName.REINDEX).order_by(StudyTask.study_acc)
                result = filtered.all()
                result_list = []
                for task in result:
                    model: StudyTaskModel = StudyTaskModel.from_orm(task)
                    result_list.append(model.dict())

                if result_list:
                    return jsonify({'result': 'Found', 'tasks': result_list})
                return jsonify({'result': 'There is no study that will be reindexed.'})

        except Exception as e:
            raise MetabolightsDBException(message=f"Error while retreiving study tasks from database: {str(e)}",
                                          exception=e)

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
            },
            {
                "code": 417,
                "message": "Unexpected result."
            }
        ]
    )
    @metabolights_exception_handler
    def post(self):

        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance(app).validate_user_has_curator_role(user_token)
        try:
            with DBManager.get_instance(app).session_maker() as db_session:
                query = db_session.query(StudyTask)
                filtered = query.filter(StudyTask.last_execution_status != StudyTaskStatus.EXECUTION_SUCCESSFUL,
                                        StudyTask.task_name == StudyTaskName.REINDEX).order_by(StudyTask.study_acc)
                tasks = filtered.all()
                indexed_studies = []
                unindexed_studies = []
                total = len(tasks)
                index = 0

                for task in tasks:
                    index += 1
                    print(f'{index}/{total} Indexing {task.study_acc}')
                    try:
                        logger.info(f'{index}/{total} Indexing {task.study_acc}')
                        wsc.reindex_study(task.study_acc, user_token)
                        indexed_studies.append(task.study_acc)
                        logger.info(f'Indexed study {task.study_acc}')
                    except Exception as e:
                        unindexed_studies.append({"study_id": task.study_acc, "message": str(e)})
                        logger.info(f'Unindexed study {task.study_acc}')

                return {"indexed_studies": indexed_studies, "unindexed_studies": unindexed_studies}

        except Exception as e:
            raise MetabolightsDBException(message=f"Error while retreiving study tasks from database: {str(e)}",
                                          exception=e)

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
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The compound is returned"
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
    def post(self):
        log_request(request)
        

        # User authentication
        user_token = ''
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
            
        logger.info('Indexing a compound')
        inputs = {"user_token": user_token, "send_email_to_submitter": True}
        try: 
            result = reindex_all_public_studies.apply_async(kwargs=inputs, expires=60*5)

            result = {'content': f"Task has been started. Result will be sent by email. Task id: {result.id}", 'message': None, "err": None}
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
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The compound is returned"
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
    def post(self):
        log_request(request)
        

        # User authentication
        user_token = ''
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
            
        logger.info('Indexing a compound')
        inputs = {"user_token": user_token, "send_email_to_submitter": True}
        try: 
            result = reindex_all_studies.apply_async(kwargs=inputs, expires=60*5)

            result = {'content': f"Task has been started. Result will be sent by email. Task id: {result.id}", 'message': None, "err": None}
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
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The compound is returned"
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
    def post(self):
        log_request(request)
        

        # User authentication
        user_token = ''
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
            
        logger.info('Indexing a compound')
        inputs = {"user_token": user_token, "send_email_to_submitter": True }
        try: 
            result = sync_studies_on_es_and_db.apply_async(kwargs=inputs, expires=60*5)

            result = {'content': f"Task has been started. Result will be sent by email. Task id: {result.id}", 'message': None, "err": None}
            return result
        except Exception as ex:
            raise MetabolightsException(http_code=500, message=f"Task submission was failed: {str(ex)}", exception=ex)
        
        


class MtblsStudyFolders(Resource):
    @swagger.operation(
        summary="Maintain study folders",
        notes="Start a task to maintain all study folders, and return task id. Result will be sent by email.",
        parameters=[
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
                "message": "OK. The compound is returned"
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
    def post(self):
        log_request(request)
        

        # User authentication
        user_token = ''
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
            
        logger.info('Searching study folders')
        inputs = {"user_token": user_token, "send_email_to_submitter": True }
        try: 
            result = maintain_study_folders.apply_async(kwargs=inputs, expires=60*5)

            result = {'content': f"Task has been started. Result will be sent by email. Task id: {result.id}", 'message': None, "err": None}
            return result
        except Exception as ex:
            raise MetabolightsException(http_code=500, message=f"Task submission was failed: {str(ex)}", exception=ex)