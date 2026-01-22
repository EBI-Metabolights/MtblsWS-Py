from flask import request
from flask_restful import Resource
from flask_restful_swagger import swagger

from app.tasks.common_tasks.basic_tasks.send_email import send_test_email
from app.utils import metabolights_exception_handler
from app.ws.auth.permissions import validate_user_has_curator_role


class SystemTestEmail(Resource):
    @swagger.operation(
        summary="Send Test email",
        parameters=[
            {
                "name": "user-token",
                "description": "Admin/Curator API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
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
    def post(self):
        result = validate_user_has_curator_role(request)
        email = result.context.username
        inputs = {"email": email}
        task = send_test_email.apply_async(kwargs=inputs)
        return {"message": f"Sent test email task is stated with id : {task.id}"}
