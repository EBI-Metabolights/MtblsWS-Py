import datetime
import json
import logging
import os.path
import time

from flask import request, current_app as app
from flask_restful import Resource, abort
from flask_restful_swagger import swagger

from app.utils import metabolights_exception_handler, MetabolightsDBException
from app.ws.db.dbmanager import DBManager
from app.ws.db.models import StudyModel
from app.ws.db.schemes import Study
from app.ws.db.settings import get_directory_settings
from app.ws.db.types import StudyStatus
from app.ws.db.wrappers import create_study_model_from_db_study, update_study_model_from_directory
from app.ws.study.user_service import UserService
from app.ws.utils import log_request

logger = logging.getLogger('wslog')


class PublicStudyJsonExporter(Resource):
    @swagger.operation(
        summary="Export all public studies in a folder (Curator only)",
        parameters=[
            {
                "name": "user_token",
                "description": "user token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "study_id",
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

        UserService.get_instance(app).validate_user_has_curator_role(user_token)
        m_study_list = []
        with DBManager.get_instance(app).session_maker() as db_session:
            query = db_session.query(Study)
            if not study_id:
                query = query.filter(Study.status == StudyStatus.PUBLIC.value).order_by(Study.acc)
            else:
                query = query.filter(Study.status == StudyStatus.PUBLIC.value, Study.acc == study_id)
            studies = query.all()

            if not studies:
                raise MetabolightsDBException(f"There is no public study")

            directory_settings = get_directory_settings(app)
            study_folders = directory_settings.studies_folder
            for study in studies:
                m_study = create_study_model_from_db_study(study)
                m_study_list.append(m_study)
        m_study_list.sort(key=get_study_id)
        now = datetime.datetime.now()
        dt = time.gmtime(now.timestamp())
        file_time = time.strftime("%Y%m%d%H%M%S", dt)
        json_path = os.path.join(study_folders, "INDEXED_STUDIES", file_time)
        os.makedirs(json_path, exist_ok=True)
        for m_study in m_study_list:
            update_study_model_from_directory(m_study, study_folders)
            dict_data = m_study.dict()
            file_path = os.path.join(json_path, m_study.studyIdentifier + '.json')
            with open(file_path, "w") as f:
                f.write(json.dumps(dict_data))
            print(f'{file_path} is written')

        return json_path
