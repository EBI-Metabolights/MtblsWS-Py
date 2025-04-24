from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
import glob
import json
import logging
import os.path
import time

from flask import request, current_app as app
from flask_restful import Resource
from flask_restful_swagger import swagger
from app.config import get_settings

from app.utils import current_time, metabolights_exception_handler, MetabolightsDBException
from app.ws.db.dbmanager import DBManager
from app.ws.db.models import StudyModel
from app.ws.db.schemes import Study
from app.ws.db.types import StudyStatus
from app.ws.db.wrappers import create_study_model_from_db_study, update_study_model_from_directory
from app.ws.settings.utils import get_study_settings
from app.ws.study.user_service import UserService
from app.ws.utils import log_request

logger = logging.getLogger('wslog')

def update_extension_set(assay, file_indices, file_extensions):
    if not file_indices:
        return
    if "assayTable" in assay and assay["assayTable"] and "data" in assay["assayTable"] and assay["assayTable"]["data"]:
        for item in assay["assayTable"]["data"]:
            for index in file_indices:
                if index < len(item) and item[index]:
                    name_parts = os.path.basename(item[index]).lstrip(".").split('.')
                    extension = ""
                    if len(name_parts) > 2:
                        extension = f".{'.'.join(name_parts[-2:])}"
                    elif len(name_parts) > 1:
                        extension = f".{name_parts[-1]}"
                    else:
                        extension = "<without extension>"
                    file_extensions.add(extension)
                    

class StudyJsonExporter(Resource):
    @swagger.operation(
        summary="[Deprecated] Export all studies in a folder (Curator only)",
        parameters=[
            {
                "name": "user-token",
                "description": "user token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "study-id",
                "description": "Requested study id",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False
            },
            {
                "name": "json-folder",
                "description": "current json folder if resume process.",
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
        log_request(request)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        json_folder = None
        if "json_folder" in request.headers:
            json_folder = request.headers["json_folder"]
            
        study_id = None
        if "study_id" in request.headers:
            study_id = request.headers["study_id"]

        def get_study_id(study: StudyModel):
            study_id = study.studyIdentifier
            study_id = study_id.replace("MTBLS_DEV", '')
            study_id = study_id.replace("MTBLS", '')
            if study_id.isnumeric():
                return int(study_id)
            return 0

        UserService.get_instance().validate_user_has_curator_role(user_token)
        study_settings = get_study_settings()
        study_folders = study_settings.mounted_paths.study_metadata_files_root_path
        if not json_folder:
            now = current_time()
            dt = time.gmtime(now.timestamp())
            json_folder = time.strftime("%Y%m%d_%H%M%S", dt)

        json_path = os.path.join(study_settings.mounted_paths.reports_root_path, get_settings().report.report_base_folder_name, "INDEXED_ALL_STUDIES", json_folder)

        os.makedirs(json_path, exist_ok=True)
        skip_files = set()
        
        for file in glob.glob(f'{json_path}/*.json'):
            if self.validate_json_file(file, True):
                skip_files.add(file)
        m_study_list = []
        with DBManager.get_instance().session_maker() as db_session:
            query = db_session.query(Study)
            if not study_id:
                query = query.order_by(Study.acc)
            else:
                query = query.filter(Study.acc == study_id)
            studies = query.all()

            if not studies:
                raise MetabolightsDBException(f"There is no study")

            
            for study in studies:
                file_path = os.path.join(json_path, study.acc + '.json')
                if file_path in skip_files:
                    continue
                m_study = create_study_model_from_db_study(study)
                m_study_list.append(m_study)
        m_study_list.sort(key=get_study_id)
        # for m_study in m_study_list:
        #     self.read_study_folder(study_folders, json_path, m_study)
        executor = ThreadPoolExecutor(max_workers=20)
            
        futures = [executor.submit(self.read_study_folder, study_folders, json_path, m_study) for m_study in m_study_list]
        for future in as_completed(futures):
            # get the result for the next completed task
            file, result = future.result() # blocks   
            if result:
                self.validate_json_file(file)
            else:
                print(f'{file} is failed')
        
        return json_path

    def validate_json_file(self, file, log_only_error=False):
        valid = False
        if not os.path.exists(file):
            return False
        try:
            f = open(file)
            if json.load(f):
                if not log_only_error:
                    print(f'{os.path.basename(file)} is valid')
                valid = True
            else:
                print(f'{file} is not correct')
        except Exception as ex:
            print(f'{file} is not read')
        if not valid:
            os.remove(file)
        return valid
    
    def read_study_folder(self, study_folders, json_path, m_study):
        try:
            file_path = os.path.join(json_path, m_study.studyIdentifier + '.json')
            update_study_model_from_directory(m_study, study_folders)
            dict_data = m_study.model_dump()
            with open(file_path, "w") as f:
                f.write(json.dumps(dict_data))
        except Exception as ex:
            logger.error(f"Failed to create json file for {m_study.studyIdentifier}")
            return file_path, False
        return file_path, True

class PublicStudyJsonExporter(Resource):
    @swagger.operation(
        summary="[Deprecated] Export all public studies in a folder (Curator only)",
        parameters=[
            {
                "name": "user-token",
                "description": "user token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "study-id",
                "description": "Requested public study id",
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
    @metabolights_exception_handler
    def get(self):
        log_request(request)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        study_id = None
        if "study_id" in request.headers:
            study_id = request.headers["study_id"]

        def get_study_id(study: StudyModel):
            study_id = study.studyIdentifier
            study_id = study_id.replace("MTBLS_DEV", '')
            study_id = study_id.replace("MTBLS", '')
            if study_id.isnumeric():
                return int(study_id)
            return 0

        UserService.get_instance().validate_user_has_curator_role(user_token)
        m_study_list = []
        with DBManager.get_instance().session_maker() as db_session:
            query = db_session.query(Study)
            if not study_id:
                query = query.filter(Study.status == StudyStatus.PUBLIC.value).order_by(Study.acc)
            else:
                query = query.filter(Study.status == StudyStatus.PUBLIC.value, Study.acc == study_id)
            studies = query.all()

            if not studies:
                raise MetabolightsDBException(f"There is no public study")

            settings = get_study_settings()
            study_folders = settings.mounted_paths.study_metadata_files_root_path
            for study in studies:
                m_study = create_study_model_from_db_study(study)
                m_study_list.append(m_study)
        m_study_list.sort(key=get_study_id)
        now = current_time()
        dt = time.gmtime(now.timestamp())
        file_time = time.strftime("%Y-%m-%d_%H%M%S", dt)
        json_path = os.path.join(settings.mounted_paths.reports_root_path, get_settings().report.report_base_folder_name, "INDEXED_PUBLIC_STUDIES", file_time)
        os.makedirs(json_path, exist_ok=True)
        for m_study in m_study_list:
            update_study_model_from_directory(m_study, study_folders)
            dict_data = m_study.model_dump()
            file_path = os.path.join(json_path, m_study.studyIdentifier + '.json')
            with open(file_path, "w") as f:
                f.write(json.dumps(dict_data))
            print(f'{file_path} is written')

        return json_path
