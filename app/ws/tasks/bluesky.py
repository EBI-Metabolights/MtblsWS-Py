import logging

from flask import request
from flask_restful import Resource, abort
from flask_restful_swagger import swagger


from app.tasks.common_tasks.basic_tasks.bluesky import create_bluesky_post_for_public_study
from app.utils import metabolights_exception_handler
from app.ws.study.user_service import UserService
from app.ws.utils import log_request

logger = logging.getLogger('wslog')



class PublicStudyAnnouncement(Resource):
    @swagger.operation(
        summary="Create a post on BlueSky",
        parameters=[
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax."
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication. "
                           "Please provide a study id and a valid user token"
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
    def post(self, study_id):
        log_request(request)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            abort(401)
    

        UserService.get_instance().validate_user_has_curator_role(user_token)
        kwargs = {
            "study_id": study_id,
            "user_token": user_token
        }
        task = create_bluesky_post_for_public_study.apply_async(kwargs=kwargs)
        return {"task_id": task.id, "message": "New task started."}