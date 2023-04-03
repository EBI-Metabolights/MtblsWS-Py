
import logging

from flask import current_app as app, request
from flask_restful import Resource, abort
from flask_restful_swagger import swagger

from app.utils import metabolights_exception_handler, MetabolightsDBException
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study
from app.ws.db.settings import get_directory_settings
from app.ws.db.types import StudyStatus
from app.ws.db.wrappers import create_study_model_from_db_study, update_study_model_from_directory
from app.ws.study.user_service import UserService
from app.ws.utils import log_request

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