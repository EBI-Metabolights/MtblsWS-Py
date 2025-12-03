import logging

from flask import request
from flask_restful import Resource
from flask_restful_swagger import swagger

from app.utils import metabolights_exception_handler
from app.ws.auth.permissions import raise_deprecation_error, validate_submission_view
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study
from app.ws.db.wrappers import (
    create_study_model_from_db_study,
    update_study_model_from_directory,
)
from app.ws.settings.utils import get_study_settings
from app.ws.utils import log_request

logger = logging.getLogger("wslog")


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
                "allowMultiple": False,
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def get(self, study_id):
        log_request(request)
        raise_deprecation_error(request)
        result = validate_submission_view(request)
        study_id = result.context.study_id
        with DBManager.get_instance().session_maker() as db_session:
            query = db_session.query(Study)
            query = query.filter(Study.acc == study_id)
            study = query.first()
            study_settings = get_study_settings()
            study_folders = study_settings.mounted_paths.study_metadata_files_root_path
            m_study = create_study_model_from_db_study(study)

        update_study_model_from_directory(
            m_study, study_folders, optimize_for_es_indexing=True
        )
        dict_data = m_study.model_dump()
        result = {"content": dict_data, "message": None, "err": None}
        return result
