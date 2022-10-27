#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2022-Oct-25
#  Modified by:   Felix Amaladoss
#
#  Copyright 2022 EMBL - European Bioinformatics Institute
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

import logging
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from flask import request, abort, jsonify
from flask import current_app as app
from app.services.storage_service.unmounted.data_mover_client import DataMoverAvailableStorage
from app.ws.db_connection import check_access_rights

logger = logging.getLogger('wslog')

def get_permissions(study_id, user_token, obfuscation_code=None):
    """
    Check MTBLS-WS for permissions on this Study for this user

    Study       User    Submitter   Curator     Reviewer/Read-only
    SUBMITTED   ----    Read+Write  Read+Write  Read
    INCURATION  ----    Read        Read+Write  Read
    INREVIEW    ----    Read        Read+Write  Read
    PUBLIC      Read    Read        Read+Write  Read

    :param obfuscation_code:
    :param study_id:
    :param user_token:
    :return: study details and permission levels

    """
    if not user_token:
        user_token = "public_access_only"

    # Reviewer access will pass the study obfuscation code instead of api_key
    if study_id and not obfuscation_code and user_token.startswith("ocode:"):
        logger.info("Study obfuscation code passed instead of user API_CODE")
        obfuscation_code = user_token.replace("ocode:", "")

    is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
    updated_date, study_status = check_access_rights(user_token, study_id.upper(),
                                                     study_obfuscation_code=obfuscation_code)

    logger.info("Read access: " + str(read_access) + ". Write access: " + str(write_access))

    return is_curator, read_access, write_access, obfuscation_code, study_location, release_date, \
           submission_date, study_status

class FTPRemoteFileManager(Resource):
    @swagger.operation(
        summary="FTP folder operations (curator only)",
        parameters=[
            {
                "name": "study_id",
                "description": "create ftp folder",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            }, {
                "name": "source_folder",
                "description": "source folder",
                "required": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            }, {
                "name": "target_folder",
                "description": "target folder",
                "required": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            }, {
                "name": "operation",
                "description": "Type of operation",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["create ftp folder", "move ftp folder", "delete ftp folder",
                         "check folder exists", "check ftp folder status", "check sync status",
                         "sync folder from ftp", "sync folder from study", "get ftp folder permission",
                         "set ftp folder permission"]
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
                "message": "Forbidden. Please provide a valid user token"
            },
            {
                "code": 404,
                "message": "Not found. The requested job identifier is not valid or no longer exist"
            }
        ]
    )
    def post(self):
        user_token = None

        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None:
            abort(401)

        parser = reqparse.RequestParser()
        parser.add_argument('study_id', help="Study ID", location="args")
        parser.add_argument('source_folder', help="Source folder", location="args")
        parser.add_argument('target_folder', help="Source folder", location="args")
        parser.add_argument('operation', help="Type of operation", location="args")
        if request.args:
            args = parser.parse_args()
            study_id = args['study_id']
            source_folder = args['source_folder']
            target_folder = args['target_folder']
            operation = args['operation']
            if operation:
                operation = operation.strip()

        if study_id is None:
            abort(404)

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = get_permissions('MTBLS2', user_token)
        if not is_curator:
            abort(403)
        result = {'status': 'Unknown'}

        if operation == 'create ftp folder':
            try:
                logger.info('Creating ftp folder!')
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage('create_ftp_folder', study_id, app)
                status = data_mover_storage.create_ftp_folder(source_folder)
                if status:
                    result = {'status': 'Success'}
                else:
                    result = {'status': 'Failure'}
            except Exception as e:
                logger.info(e)
                print(e)
                result = {'status': 'Failure'}
        elif operation == 'move ftp folder':
            try:
                logger.info('Moving FTP folder!')
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage('move_ftp_folder', study_id, app)
                status = data_mover_storage.move_ftp_folder(source_folder, target_folder)
                if status:
                    result = {'status': 'Success'}
                else:
                    result = {'status': 'Failure'}
            except Exception as e:
                logger.info(e)
                print(e)
        elif operation == 'delete ftp folder':
            try:
                logger.info('Deleting FTP folder!')
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage('delete_ftp_folder', study_id, app)
                status = data_mover_storage.delete_ftp_folder(source_folder)
                if status:
                    result = {'status': 'Success'}
                else:
                    result = {'status': 'Failure'}
            except Exception as e:
                logger.info(e)
                print(e)
        elif operation == 'check folder exists':
            try:
                logger.info('Checking if FTP folder exists!')
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage('check_ftp_folder_exists', study_id, app)
                status = data_mover_storage.does_folder_exist(source_folder)
                if status:
                    result = {'status': 'Success'}
                else:
                    result = {'status': 'Failure'}
            except Exception as e:
                logger.info(e)
                print(e)
        elif operation == 'get ftp folder permission':
            try:
                logger.info('Getting FTP folder permission!')
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage('get_ftp_folder_permission', study_id, app)
                output = data_mover_storage.get_ftp_folder_permission(source_folder)
                if output:
                    result = {'permission': output}
                else:
                    result = {'status': 'Failure'}
            except Exception as e:
                logger.info(e)
                print(e)
        elif operation == 'set ftp folder permission':
            try:
                logger.info('Setting FTP folder permission!')
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage('get_ftp_folder_permission', study_id, app)
                chmod = int(target_folder)
                output = data_mover_storage.update_ftp_folder_permission(source_folder, chmod)
                if output:
                    result = {'status': output}
                else:
                    result = {'status': 'Failure'}
            except Exception as e:
                logger.info(e)
                print(e)
        elif operation == 'check ftp folder status':
            try:
                logger.info('Checking FTP folder status!')
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage('check_ftp_status',
                                                                                          study_id, app)
                output = data_mover_storage.check_calculate_sync_status(source_folder, False)
                result = {'result': output.dict()}
            except Exception as e:
                logger.info(e)
                print(e)
        elif operation == 'check sync status':
            try:
                logger.info('Checking Sync status!')
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage('check_ftp_sync_status',
                                                                                          study_id, app)
                output = data_mover_storage.check_folder_sync_status()
                result = {'result': output.dict()}
            except Exception as e:
                logger.info('Exception ' + str(e))
                print(e)
                result = {'status': str(e)}
        elif operation == 'sync folder from ftp':
            try:
                logger.info('Syncing FTP folder !')
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage("study_id",
                                                                                          study_id, app)
                status = data_mover_storage.sync_from_ftp_folder(source_folder)
                if status:
                    result = {'status': 'Success'}
                else:
                    result = {'status': 'Failure'}
            except Exception as e:
                logger.info(e)
                print(e)
        elif operation == 'sync folder from study':
            try:
                logger.info('Syncing study folder !')
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage("None",
                                                                                          study_id, app)
                status = data_mover_storage.sync_from_studies_folder(target_folder)
                if status:
                    result = {'status': 'Success'}
                else:
                    result = {'status': 'Failure'}
            except Exception as e:
                logger.info(e)
                print(e)

        return jsonify(result)

