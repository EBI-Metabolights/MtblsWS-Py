
from flask import current_app as app, request
from flask_restful import Resource
from flask_restful_swagger import swagger
from app.tasks.common_tasks.basic_tasks.email import send_test_email

from app.utils import metabolights_exception_handler
from app.ws.study.user_service import UserService

class SystemTestEmail(Resource):

    @swagger.operation(
        summary="Send Test email",
        parameters=[
            {
                "name": "user_token",
                "description": "Admin/Curator API token",
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
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    @metabolights_exception_handler
    def post(self):

        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"] 
                 
        UserService.get_instance().validate_user_has_curator_role(user_token)
        inputs = {"user_token": user_token}
        task = send_test_email.apply_async(kwargs=inputs)
        return {"message": f"Sent test email task is stated with id : {task.id}"}