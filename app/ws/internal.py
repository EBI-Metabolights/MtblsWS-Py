
from datetime import datetime
from flask import current_app as app
from flask import request
from flask_restful import Resource
from flask_restful_swagger import swagger
from app.config import get_settings

from app.utils import (MetabolightsException,
                       metabolights_exception_handler)
from app.ws.redis.redis import get_redis_server
from app.ws.study.user_service import UserService
from app.ws.utils import log_request

_banner: str = None
_last_banner_check_timestamp: int=0

def get_banner():
    global _banner
    global _last_banner_check_timestamp
    
    update_check_time_delta = 60
    settings = get_settings()
    if settings:
        update_check_time_delta = settings.server.service.banner_check_period_in_seconds    
    now = int(datetime.now().timestamp())
    current_banner = _banner
    if now - _last_banner_check_timestamp > update_check_time_delta:
        _banner = None
        _last_banner_check_timestamp = now
    
    if not _banner:
        print("Banner will be checked.")
        try:
            redis = get_redis_server()
            new_banner = redis.get_value("metabolights:banner:message")
            if new_banner:
                new_banner = new_banner.decode("utf-8")
                if new_banner != _banner:
                    _banner = new_banner
                    print(f"Banner is updated. New banner message: {_banner}")
        except Exception as ex:
            print("Failed to load banner")
            _banner = current_banner
    return _banner


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
            message = get_banner()
            
            if message:
                return {"message": None, "content": message, "error": None }
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

