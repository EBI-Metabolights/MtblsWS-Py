from flask import request
from app.tasks.common_tasks.curation_tasks.submission_model import (
    RevalidateStudyParameters,
)
from app.tasks.common_tasks.curation_tasks.submission_pipeline import make_study_private
from app.utils import MetabolightsException, metabolights_exception_handler
from flask_restful import Resource
from flask_restful_swagger import swagger

from app.ws.study.study_service import StudyService
from app.ws.study.user_service import UserService


class PrivateStudy(Resource):
    @swagger.operation(
        summary="Change study status to private",
        nickname="Change study status",
        notes="""Change study status from 'Provisional' to 'Private'""",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The Metabolite Annotation File (MAF) is returned",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def post(self, study_id: str):
        # param validation
        if study_id is None:
            raise MetabolightsException(
                message="Please provide valid parameter for study identifier"
            )
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        study = StudyService.get_instance().get_study_by_acc(study_id)

        params = RevalidateStudyParameters(
            study_id=study_id, obfuscation_code=study.obfuscationcode
        )
        # inputs = {"root_path": root_path, "folder_paths": absolute_folder_path, }
        task_id = make_study_private(params.model_dump())

        return {
            "task_name": "make study private",
            "task_id": task_id,
            "message": f"{study_id} will be private.",
        }
