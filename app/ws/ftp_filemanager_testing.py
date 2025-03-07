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
from flask_restful import Resource, reqparse, abort
from flask_restful_swagger import swagger
from flask import request, jsonify
from flask import current_app as app
from app.services.storage_service.unmounted.data_mover_client import DataMoverAvailableStorage
from app.services.storage_service.models import SyncTaskStatus
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
                         "sync folder from ftp", "sync folder from study", "sync to public ftp", "get ftp folder permission",
                         "set ftp folder permission"]
            },
            {
                "name": "user-token",
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
        
        
        if request.args:
            
            study_id = request.args.get('study_id')
            source_folder = request.args.get('source_folder')
            target_folder = request.args.get('target_folder')
            operation = request.args.get('operation')
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
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage('ftp_filemanager_testing', study_id, app)
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
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage('ftp_filemanager_testing', study_id, app)
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
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage('ftp_filemanager_testing', study_id, app)
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
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage('ftp_filemanager_testing', study_id, app)
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
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage('ftp_filemanager_testing', study_id, app)
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
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage('ftp_filemanager_testing', study_id, app)
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
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage('ftp_filemanager_testing',
                                                                                          study_id, app)
                meta_calc_result,rdfiles_calc_result = data_mover_storage.sync_anaysis_job_results(source_ftp_folder=source_folder, force=False)
                result = {'result':{'meta_calc_result':meta_calc_result.model_dump(),'rdfiles_calc_result': rdfiles_calc_result.model_dump()}}
            except Exception as e:
                logger.info(e)
                print(e)
        elif operation == 'check sync status':
            try:
                logger.info('Checking Sync status!')
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage('ftp_filemanager_testing',
                                                                                          study_id, app)
                sync_metafiles_result,sync_rdfiles_result = data_mover_storage.get_folder_sync_results()
                result = {'result':{'sync_metafiles_result':sync_metafiles_result.model_dump(),'sync_rdfiles_result': sync_rdfiles_result.model_dump()}}
            except Exception as e:
                logger.info('Exception ' + str(e))
                print(e)
                result = {'status': str(e)}
        elif operation == 'sync folder from ftp':
            try:
                logger.info('Syncing FTP folder !')
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage("ftp_filemanager_testing",
                                                                                          study_id, app)
                meta_sync_status,rdfiles_sync_status = data_mover_storage.sync_from_ftp_folder(source_folder)
                result = {'meta_sync_status': meta_sync_status, 'rdfiles_sync_status':rdfiles_sync_status}
            except Exception as e:
                logger.info(e)
                print(e)
        elif operation == 'sync folder from study':
            try:
                logger.info('Syncing study folder !')
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage("ftp_filemanager_testing",
                                                                                          study_id, app)
                meta_sync_status,files_sync_status,chebi_sync_status = data_mover_storage.sync_from_studies_folder(target_folder)
                result = {'meta_sync_status': meta_sync_status, 'files_sync_status':files_sync_status, 'chebi_sync_status':chebi_sync_status}
            except Exception as e:
                logger.info(e)
                print(e)
                
        elif operation == 'sync to public ftp':
            try:
                logger.info('Syncing study to public ftp !')
                data_mover_storage: DataMoverAvailableStorage = DataMoverAvailableStorage("ftp_filemanager_testing",
                                                                                          study_id, app)
                meta_public_sync_status,files_public_sync_status = data_mover_storage.sync_public_study_to_ftp()
                result = {'meta_sync_status': meta_public_sync_status, 'files_sync_status':files_public_sync_status}
            except Exception as e:
                logger.info(e)
                print(e)

        return jsonify(result)

