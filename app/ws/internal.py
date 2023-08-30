
from flask import current_app as app
from flask import request
from flask_restful import Resource
from flask_restful_swagger import swagger

from app.utils import (MetabolightsException,
                       metabolights_exception_handler)
from app.ws.redis.redis import get_redis_server
from app.ws.study.user_service import UserService
from app.ws.utils import log_request
    
class BannerMessage(Resource):
    @swagger.operation(
        summary="Returns banner message",
        parameters=[
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
    def get(self):
        message = None
        try: 
            redis = get_redis_server()
            message = redis.get_value("metabolights:banner:message")
            
            if message:
                return {"message": None, "content": message.decode("utf-8"), "error": None }
            else:
                return {"message": None, "content": "", "error": None }
        except Exception as ex:
            # no cache or invalid cache
            return {"message": None, "content": message, "error": None }

    @swagger.operation(
        summary="Update application banner message",
        notes="json ",
        parameters=[
            {
                "name": "message",
                "description": 'message content {\"message\":\"Message content\"}',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "user_token",
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
    def put(self):
        # User authentication
        log_request(request)
        try:
            content = request.json
        except:
            raise MetabolightsException(message="body content is invalid")

        if not content or "message" not in content :
            raise  MetabolightsException(message="body content has no message")

        message = content["message"]
      # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        
        UserService.get_instance().validate_user_has_curator_role(user_token)
        
        redis = get_redis_server()
        
        if message:
            redis.set_value("metabolights:banner:message", message)
        else:
            redis.set_value("metabolights:banner:message", "")
        
        return {"message": None, "content": content, "error": None }

