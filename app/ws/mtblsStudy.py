import glob
import logging
import os
import time
from flask import current_app as app
from flask import request, abort, send_file
from flask.json import jsonify
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import get_all_files, get_year_plus_one
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


class IsaTabInvestigation(Resource):

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
        # log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info('Getting ISA-Tab Investigation file for MTBLS Study %s, using API-Key %s', study_id, user_token)
        location = wsc.get_study_location(study_id, user_token)
        file_path = glob.glob(os.path.join(location, "i_*.txt"))[0]
        inv_filename = os.path.basename(file_path)
        try:
            return send_file(file_path, cache_timeout=-1,
                             as_attachment=True, attachment_filename=inv_filename)
        except OSError as err:
            logger.error(err)
            abort(404)


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
        location = wsc.get_study_location(study_id, user_token)
        all_files = get_all_files(location)
        return jsonify({"files": all_files})


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
            }
        ]
    )
    def get(self):

        parser = reqparse.RequestParser()
        parser.add_argument('study_id', help="The row number of the cell to remove (exclude header)")
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
        new_folder_name = user_token + '~~' + study_date + '~' + 'new_study_requested'

        study_to_clone = wsc.get_study_location(study_id, user_token)
        queue_folder = wsc.get_queue_folder()
        existing_studies = wsc.get_all_studies_for_user(user_token)

        # copy the study onto the queue folder
        copy_tree(study_to_clone, os.path.join(queue_folder.decode("utf-8"), new_folder_name))  # copy the entire folder to the queue

        # get a list of the users private studies to see if a new study has been created. Have to query on a regular basis
        new_studies = wsc.get_all_studies_for_user(user_token)

        while existing_studies == new_studies:
            logger.info('Checking if the new study has been processed by the queue, API-Key %s', user_token)
            time.sleep(2)
            new_studies = wsc.get_all_studies_for_user(user_token)

        logger.info('Ok, now there is a new private study for the user, API-Key %s', user_token)

        # Tidy up the response strings before converting to lists
        new_studies_list = new_studies.replace('[', '').replace(']', '').replace('"', '').split(',')
        existing_studies_list = existing_studies.replace('[', '').replace(']', '').replace('"', '').split(',')

        logger.info('returning the new study, %s', user_token)
        # return the new entry, i.e. difference between the two lists
        diff = list(set(new_studies_list) - set(existing_studies_list))

        study_id = diff[0]

        wsc.create_upload_folder(study_id)

        return jsonify({"new_study": diff})


