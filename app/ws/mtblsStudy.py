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

import json
from flask import request, abort, send_file
from flask.json import jsonify
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import *
from app.ws.isaApiClient import IsaApiClient
from distutils.dir_util import copy_tree
from app.ws.db_connection import get_all_studies_for_user

logger = logging.getLogger('wslog')
wsc = WsClient()
iac = IsaApiClient()


# Allow for a more detailed logging when on DEBUG mode
def log_request(request_obj):
    if app.config.get('DEBUG'):
        if app.config.get('DEBUG_LOG_HEADERS'):
            logger.debug('REQUEST HEADERS -> %s', request_obj.headers)
        if app.config.get('DEBUG_LOG_BODY'):
            logger.debug('REQUEST BODY    -> %s', request_obj.data)
        if app.config.get('DEBUG_LOG_JSON'):
            try:
                logger.debug('REQUEST JSON    -> %s', request_obj.json)
            except:
                logger.debug('REQUEST JSON    -> EMPTY')


class MtblsStudies(Resource):
    @swagger.operation(
        summary="Get all Studies",
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
        logger.info('Getting all public studies')
        pub_list = wsc.get_public_studies()
        logger.info('... found %d public studies', len(pub_list['content']))
        return jsonify(pub_list)


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

        pub_list = wsc.get_all_studies_for_user(user_token)  # TODO, change to a lite-study representation
        existing_studies_list = pub_list.replace('[', '').replace(']', '').replace('"', '').split(',')
        return jsonify({'data': existing_studies_list})


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
            abort(401, "Missing study_id")

        study_id = study_id.upper()

        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']

        if user_token is None:
            abort(401, "Missing user_token")

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('investigation_filename', help='Investigation filename')
        inv_filename = None
        if request.args:
            args = parser.parse_args(req=request)
            inv_filename = args['investigation_filename'].lower() if args['investigation_filename'] else None
        if not inv_filename:
            logger.warning("Missing Investigation filename. Using default i_Investigation.txt")
            inv_filename = 'i_Investigation.txt'
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403, "Study does not exist or your do not have access to this study")

        logger.info('Getting ISA-Tab Investigation file for %s', study_id)
        location = study_location  # wsc.get_study_location(study_id, user_token)
        files = glob.glob(os.path.join(location, inv_filename))
        if files:
            file_path = files[0]
            filename = os.path.basename(file_path)
            try:
                return send_file(file_path, cache_timeout=-1,
                                 as_attachment=True, attachment_filename=filename)
            except OSError as err:
                logger.error(err)
                abort(503, "Wrong investigation filename or file could not be read.")
        else:
            abort(503, "Wrong investigation filename or file could not be read.")


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
            abort(404, "Missing Sample filename.")

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(401, "Study does not exist or your do not have access to this study.")

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
                abort(404, "Wrong sample filename or file could not be read.")
        else:
            abort(404, "Wrong sample filename or file could not be read.")


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
            assay_filename = args['assay_filename'].lower() if args['assay_filename'] else None
        if not assay_filename:
            logger.warning("Missing Assay filename.")
            abort(404, "Missing Assay filename.")

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(401, "Study does not exist or your do not have access to this study.")

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
                abort(404, "Wrong assay filename or file could not be read.")
        else:
            abort(404, "Wrong assay filename or file could not be read.")


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
                abort(401, "Study does not exist or your do not have access to this study.")

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
                time.sleep(5)  # Have to check every so many secounds to see if the queue has finished
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

        # Create an upload folder for all studies anyway
        status = wsc.create_upload_folder(study_id, obfuscation_code, user_token)
        upload_location = status["upload_location"]
        return {'new_study': study_id, 'upload_location': upload_location}


class CreateUploadFolder(Resource):
    @swagger.operation(
        summary="Create a new study upload folder",
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
            abort(401, "Unauthorized. Access to the resource requires user authentication. "
                       "Please provide a study id and a valid user token")

        logger.info('Creating a new study upload folder for study %s', study_id)
        status = wsc.create_upload_folder(study_id, obfuscation_code, user_token)

        data_dict = json.loads(status)
        os_upload_path = data_dict["message"]
        upload_location = os_upload_path.split('/mtblight')  # FTP/Aspera root starts here

        return {'os_upload_path': os_upload_path, 'upload_location': upload_location[1]}

    @swagger.operation(
        summary="Create a new study upload folder",
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
            abort(401)

        logger.info('Creating a new study upload folder for study %s', study_id)
        status = wsc.create_upload_folder(study_id, obfuscation_code, user_token)
        return status


class SaveAuditFiles(Resource):
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
            abort(401, "Unauthorized. Write access to the resource requires user authentication. "
                       "Please provide a study id and a valid user token")

        logger.info('Creating a new study audit folder for study %s', study_id)
        status, dest_path = write_audit_files(study_location)

        if status:
            return {'Success': 'Created audit record for ' + study_id}
        else:
            return {'Error': 'Failed to create audit folder ' + dest_path}


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
    def get(self):

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if not user_token:
            abort(404)

        # Need to check that the user is actually an active user, ie the user_token exists
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions('MTBLS1', user_token)
        if not read_access:
            abort(401)

        logger.info('Creating a new MTBLS Study')

        study_message, status_code = wsc.add_empty_study(user_token)
        if status_code != 200:
            logger.error('Failed to create new study. ' + study_message)
            abort(503, "Could not create a new study.")

        time.sleep(5)  # give the Java WebService time to recover! ;-)

        data_dict = json.loads(study_message)
        study_acc = data_dict["message"]

        # Now, if the production Tomcats have recently been restarted, we may not have a fully associated study yet
        all_studies = get_all_studies_for_user(user_token)
        found_new_study = False
        for j_study in all_studies:
            acc = j_study['accession']
            if acc == study_acc:
                found_new_study = True
                break

        if found_new_study:
            logger.info('Created new study ' + study_acc)
        else:
            logger.error('Could not find new study %s for the user', study_acc)

        # We should have a new study now, so we need to refresh the local variables based on the new study
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_acc, user_token)

        if not write_access:
            abort(409, "Something went wrong with the creation of study " + study_acc)

        study_path = app.config.get('STUDY_PATH')
        from_path = study_path + 'DUMMY'
        to_path = study_location

        try:
            copy_files_and_folders(from_path, to_path, include_raw_data=True, include_investigation_file=True)
        except Exception as e:
            logger.error('Could not copy files from %s to %s, Error ', from_path, to_path, str(e))
            abort(409, "Something went wrong with copying the ISA-Tab templates to study " + str(study_acc))

        # Create upload folder
        status = wsc.create_upload_folder(study_acc, obfuscation_code, user_token)

        if os.path.isfile(os.path.join(to_path, 'i_Investigation.txt')):
            # Get the ISA documents so we can edit the investigation file
            isa_study, isa_inv, std_path = iac.get_isa_study(study_id=study_acc, api_key=user_token,
                                                             skip_load_tables=True, study_location=study_location)

            # Also make sure the sample file is in the standard format of 's_MTBLSnnnn.txt'
            isa_study, sample_file_name = update_correct_sample_file_name(isa_study, study_location, study_acc)

            # Set publication date to one year in the future
            study_date = get_year_plus_one(isa_format=True)
            isa_study.public_release_date = study_date

            # Updated the files with the study accession
            iac.write_isa_study(
                inv_obj=isa_inv, api_key=user_token, std_path=to_path,
                save_investigation_copy=False, save_samples_copy=False, save_assays_copy=False
            )

            try:
                wsc.reindex_study(study_acc, user_token)
            except:
                logger.info("Could not index study " + study_acc)

            # For ISA-API to correctly save a set of ISA documents, we need to have one dummy sample row
            file_name = os.path.join(study_location, sample_file_name)
            try:
                sample_df = read_tsv(file_name)
            except FileNotFoundError:
                abort(400, "The file " + file_name + " was not found")

            try:
                sample_df = sample_df.drop(sample_df.index[0])  # Drop the first dummy row, if there is one
            except IndexError:
                logger.info("No empty rows in the default sample sheet template, so nothing to remove")

            write_tsv(sample_df, file_name)
        else:
            abort(409, "Could not find ISA-Tab investigation template for study " + study_acc)

        return {"new_study": study_acc}


def write_audit_files(study_location):
    """
    Write back an ISA-API Investigation object directly into ISA-Tab files
    :param study_location: the filesystem where the study is located
    :return:
    """
    # dest folder name is a timestamp
    update_path_suffix = app.config.get('UPDATE_PATH_SUFFIX')
    update_path = os.path.join(study_location, update_path_suffix)
    dest_path = new_timestamped_folder(update_path)

    try:
        # make a copy of ISA-Tab & MAF
        for isa_file in glob.glob(os.path.join(study_location, "?_*.t*")):
            isa_file_name = os.path.basename(isa_file)
            src_file = isa_file
            dest_file = os.path.join(dest_path, isa_file_name)
            logger.info("Copying %s to %s", src_file, dest_file)
            copy_file(src_file, dest_file)
    except:
        return False, dest_path

    return True, dest_path


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
            abort(401)

        status, message = wsc.reindex_study(study_id, user_token)

        if not status:
            abort(417, message)

        return {"Success": "Study " + study_id + " has been re-indexed",
                "read_access": read_access, "write_access": write_access}
