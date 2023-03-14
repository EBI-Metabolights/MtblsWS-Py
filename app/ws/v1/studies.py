
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
from app.utils import MetabolightsException, metabolights_exception_handler, MetabolightsFileOperationException, \
    MetabolightsDBException
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
from app.ws.utils import get_year_plus_one, update_correct_sample_file_name, read_tsv, remove_file, copy_file, \
    get_timestamp, copy_files_and_folders, write_tsv, log_request

logger = logging.getLogger('wslog')


class V1StudyDetail(Resource):
    @swagger.operation(
        summary="Returns details of a study",
        parameters=[
            {
                "name": "study_id",
                "description": "Requested public study id",
                "paramType": "path",
                "type": "string",
                "required": True,
                "allowMultiple": False
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
            
        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
                
        
        with DBManager.get_instance(app).session_maker() as db_session:
            query = db_session.query(Study)
            query = query.filter(Study.acc == study_id)
            study = query.first()

            if not study:
                raise MetabolightsDBException(f"{study_id} does not exist or is not public")

            if StudyStatus(study.status) != StudyStatus.PUBLIC:
                if user_token:
                    UserService.get_instance(app).validate_user_has_write_access(user_token, study_id)
                else:
                    abort(http_status_code=403)
                    
            directory_settings = get_directory_settings(app)
            study_folders = directory_settings.studies_folder
            m_study = create_study_model_from_db_study(study)

        update_study_model_from_directory(m_study, study_folders, optimize_for_es_indexing=True)
        dict_data = m_study.dict()
        result = {'content': dict_data, 'message': None, "err": None}
        return result