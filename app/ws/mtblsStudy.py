import glob
import logging
import os
import time
from flask import current_app as app, request, abort, send_file
from flask.json import jsonify
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import get_all_files, get_year_plus_one, get_timestamp, strip_tags
from distutils.dir_util import copy_tree


logger = logging.getLogger('wslog')
wsc = WsClient()


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
        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('investigation_filename', help='Investigation filename')
        inv_filename = None
        if request.args:
            args = parser.parse_args(req=request)
            inv_filename = args['investigation_filename'].lower() if args['investigation_filename'] else None
        if not inv_filename:
            logger.warning("Missing Investigation filename.")
            abort(400, "Missing Investigation filename.")
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)

        logger.info('Getting ISA-Tab Investigation file for %s', study_id)
        location = wsc.get_study_location(study_id, user_token)
        files = glob.glob(os.path.join(location, inv_filename))
        if files:
            file_path = files[0]
            filename = os.path.basename(file_path)
            try:
                return send_file(file_path, cache_timeout=-1,
                                 as_attachment=True, attachment_filename=filename)
            except OSError as err:
                logger.error(err)
                abort(404, "Wrong filename or file could not be read.")
        else:
            abort(404, "Wrong filename or file could not be read.")


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
                "message": "Forbidden. Access to the study is not allowed for this user."
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
        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('sample_filename', help='Sample filename')
        sample_filename = None
        if request.args:
            args = parser.parse_args(req=request)
            sample_filename = args['sample_filename'].lower() if args['sample_filename'] else None
        if not sample_filename:
            logger.warning("Missing Sample filename.")
            abort(400, "Missing Sample filename.")
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)

        logger.info('Getting ISA-Tab Sample file for %s', study_id)
        location = wsc.get_study_location(study_id, user_token)
        files = glob.glob(os.path.join(location, sample_filename))
        if files:
            file_path = files[0]
            filename = os.path.basename(file_path)
            try:
                return send_file(file_path, cache_timeout=-1,
                                 as_attachment=True, attachment_filename=filename)
            except OSError as err:
                logger.error(err)
                abort(404, "Wrong filename or file could not be read.")
        else:
            abort(404, "Wrong filename or file could not be read.")


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
                "message": "Forbidden. Access to the study is not allowed for this user."
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
        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('assay_filename', help='Assay filename')
        assay_filename = None
        if request.args:
            args = parser.parse_args(req=request)
            assay_filename = args['assay_filename'].lower() if args['assay_filename'] else None
        if not assay_filename:
            logger.warning("Missing Assay filename.")
            abort(400, "Missing Assay filename.")
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)

        logger.info('Getting ISA-Tab Assay file for %s', study_id)
        location = wsc.get_study_location(study_id, user_token)

        files = glob.glob(os.path.join(location, assay_filename))
        if files:
            file_path = files[0]
            filename = os.path.basename(file_path)
            try:
                return send_file(file_path, cache_timeout=-1,
                                 as_attachment=True, attachment_filename=filename)
            except OSError as err:
                logger.error(err)
                abort(404, "Wrong filename or file could not be read.")
        else:
            abort(404, "Wrong filename or file could not be read.")


class StudyFiles(Resource):

    @swagger.operation(
        summary="Get a list of all files in a study folder",
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
                "name": "user_token",
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
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self, study_id):

        # param validation
        if study_id is None:
            abort(404)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)

        logger.info('Getting list of all files for MTBLS Study %s, using API-Key %s', study_id, user_token)
        study_location = wsc.get_study_location(study_id, user_token)
        study_obfuscation = wsc.get_study_obfuscation(study_id, user_token)
        upload_location = app.config.get('MTBLS_FTP_ROOT') + study_id.lower() + "-" + study_obfuscation
        study_files = get_all_files(study_location)
        upload_files = get_all_files(upload_location)
        return jsonify({'studyFiles': study_files, 'uploadFiles': upload_files})


class AllocateAccession(Resource):

    @swagger.operation(
        summary="Create a new study and upload folder",
        parameters=[
            {
                "name": "study_id",
                "description": "Existing Study Identifier to clone. Will clone standard LC-MS study if no parameter given",
                "required": False,
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
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token"
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

        parser = reqparse.RequestParser()
        parser.add_argument('study_id', help="Study Identifier")
        study_id = None

        if request.args:
            args = parser.parse_args(req=request)
            study_id = args['study_id']

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # param validation
        if study_id is None:
            study_id = 'MTBLS121'  # This is the standard LC-MS study. This is private but safe for all to clone
        else:
            if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
                abort(403)

        if user_token is None:
            abort(403)

        study_date = get_year_plus_one()
        logger.info('Creating a new MTBLS Study (cloned from %s) with release date %s, using API-Key %s', study_id, study_date, user_token)
        new_folder_name = user_token + '~~' + study_date + '~' + 'new_study_requested_' + get_timestamp()

        study_to_clone = wsc.get_study_location(study_id, user_token)
        queue_folder = wsc.get_queue_folder()
        existing_studies = wsc.get_all_studies_for_user(user_token)
        logger.info('Found the following studies: ' + existing_studies + ' for user API-Key %s', user_token)

        logger.info('Adding ' + study_to_clone + ', using name ' + new_folder_name + ', to the queue folder ' + queue_folder)
        # copy the study onto the queue folder
        try:
            copy_tree(study_to_clone, os.path.join(queue_folder, new_folder_name))  # copy the entire folder to the queue
            # There is a bug in copy_tree which prevents you to use the same destination folder twice
        except:
            return {"error": "Could not add study into the MetaboLights queue"}

        logger.info('Folder successfully added to the queue')
        # get a list of the users private studies to see if a new study has been created. Have to query on a regular basis
        new_studies = wsc.get_all_studies_for_user(user_token)
        number = 0
        while existing_studies == new_studies:
            number = number + 1
            if number == 10:
                abort(408)

            logger.info('Checking if the new study has been processed by the queue, API-Key %s', user_token)
            time.sleep(5)  # Have to check every so many secounds to see if the queue has finished
            new_studies = wsc.get_all_studies_for_user(user_token)

        logger.info('Ok, now there is a new private study for the user, API-Key %s', user_token)

        # Tidy up the response strings before converting to lists
        new_studies_list = new_studies.replace('[', '').replace(']', '').replace('"', '').split(',')
        existing_studies_list = existing_studies.replace('[', '').replace(']', '').replace('"', '').split(',')

        logger.info('returning the new study, %s', user_token)
        # return the new entry, i.e. difference between the two lists
        diff = list(set(new_studies_list) - set(existing_studies_list))

        study_id = diff[0]
        status = wsc.create_upload_folder(study_id, user_token)
        # logger.info('Study upload folder creation status: ' + status)

        return {"new_study": study_id}


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
                "message": "Unauthorized. Access to the resource requires user authentication. Please provide a study id and a valid user token"
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
            abort(401)

        # param validation
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)

        logger.info('Creating a new study upload folder for study %s for the user, API-Key %s', study_id, user_token)
        status = wsc.create_upload_folder(study_id, user_token)
        no_html = strip_tags(status)  # This can alternatively be returned

        return status

