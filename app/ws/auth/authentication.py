import json
import logging
import re
from uuid import uuid4
import uuid

from flask import request, jsonify, make_response, current_app as app
from flask_restful import Resource
from flask_restful_swagger import swagger
from pydantic import BaseModel

from app.utils import MetabolightsAuthorizationException, MetabolightsDBException, metabolights_exception_handler, MetabolightsException
from app.ws.auth.auth_manager import AuthenticationManager, get_security_settings
from app.ws.db.dbmanager import DBManager
from app.ws.db.models import StudyAccessPermission, UserModel
from app.ws.db.schemes import Study, User
from app.ws.db.types import StudyStatus, UserRole, UserStatus
from app.ws.redis.redis import RedisStorage, get_redis_server
from app.ws.study.user_service import UserService
from app.ws.utils import log_request

logger = logging.getLogger('wslog')


def validate_token_in_request_body(content):
    if not content \
            or ("jwt" not in content and "Jwt" not in content) \
            or ("user" not in content and "User" not in content ):
        return make_response(jsonify({"content": False,
                                      "message": "Invalid request. token and user inputs are required",
                                      "err": None}), 400)

    username = content["user"] if "user" in content else content["user"]
    jwt_token = content["jwt"] if "jwt" in content else content["jwt"]

    try:
        user_in_token = AuthenticationManager.get_instance(app).validate_oauth2_token(token=jwt_token)
    except MetabolightsAuthorizationException as e:
        return make_response(jsonify({"content": "invalid", "message": e.message, "err": e}), 401)
    except MetabolightsException as e:
        return make_response(jsonify({"content": "invalid", "message": e.message, "err": e}), 401)
    except Exception as e:
        return make_response(jsonify({"content": "invalid", "message": "Authenticaion Failed", "err": e}), 401)

    if not user_in_token or user_in_token.userName != username:
        return make_response(jsonify({"content": "invalid", "message": "Not a valid token for user", "err": None}), 401)

    response = make_response(jsonify({"content": "true", "message": "Authentication successful", "err": None}), 200)
    response.headers["Access-Control-Expose-Headers"] = "Jwt, User"
    response.headers["jwt"] = jwt_token
    response.headers["user"] = username
    return response


def parse_response_body(request):
    pattern_string = r"[\|{|\|}|\s|\"|']"
    pattern = re.compile(pattern_string)

    request_body = request.data.decode()
    request_body = re.sub(pattern, '', request_body)
    split_body = re.split(',', request_body)
    results = {}
    for item in split_body:
        if ":" in item:
            param = item.split(":")
            results[param[0]] = param[1]
    return results


response_messages = [
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
    }]


class AuthLoginWithToken(Resource):
    @swagger.operation(
        summary="Authenticate user with apitoken & user and returns authentication token for valid parameters",
        notes="json ",
        parameters=[
            {
                "name": "authdata",
                "description": 'Registered user token and username {\"token\":\"api token here\", \"user\":{\"userName\":\"email address here\"}}',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=response_messages
    )
    @metabolights_exception_handler
    def post(self):
        # User authentication
        log_request(request)
        try:
            content = request.json
        except:
            content = parse_response_body(request)

        if not content or "token" not in content:
            return make_response(jsonify({"content": False,
                                          "message": "Invalid request. token and user inputs are required",
                                          "err": None}), 400)

        api_token = content["token"]
        user = UserService.get_instance(app).validate_user_has_submitter_or_super_user_role(api_token)
        settings = get_security_settings(app)
        if UserRole(user['role']) == UserRole.ROLE_SUPER_USER:
            exp = settings.admin_jwt_token_expires_in_mins
        else:
            exp = settings.access_token_expires_delta
            
        try:
            token = AuthenticationManager.get_instance(app).create_oauth2_token_by_api_token(api_token, exp_period_in_mins=exp)
        except MetabolightsException as e:
            return make_response(jsonify({"content": "invalid", "message": e.message, "err": e}), e.http_code)
        except Exception as e:
            return make_response(jsonify({"content": "invalid", "message": "Authentication failed", "err": str(e)}), 403)

        if not token:
            return make_response(jsonify({"content": "invalid", "message": "Authentication failed", "err": None}), 403)

        resp = make_response(jsonify({"content": True, "message": "Authentication successful", "err": None}), 200)
        resp.headers["Access-Control-Expose-Headers"] = "Jwt, User"
        resp.headers["jwt"] = token
        resp.headers["user"] = user.username

        return resp
class AuthLogin(Resource):
    @swagger.operation(
        summary="Authenticate user with username and password and returns authentication token for valid parameters",
        notes="json ",
        parameters=[
            {
                "name": "authdata",
                "description": 'Registered user address and password {\"email\":\"email here\", \"secret\":\"password here\"}',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=response_messages
    )
    @metabolights_exception_handler
    def post(self):
        # User authentication
        log_request(request)
        try:
            content = request.json
        except:
            content = parse_response_body(request)

        if not content or "email" not in content or "secret" not in content:
            return make_response(jsonify({"content": False,
                                          "message": "Invalid request. email and secret inputs are required",
                                          "err": None}), 400)

        username = content["email"]
        password = content["secret"]

        try:
            token = AuthenticationManager.get_instance(app).create_oauth2_token(username, password)
        except MetabolightsAuthorizationException as e:
            return make_response(jsonify({"content": "invalid", "message": e.message, "err": str(e)}), e.http_code)
        except MetabolightsException as e:
            return make_response(jsonify({"content": "invalid", "message": e.message, "err": str(e)}), e.http_code)
        except Exception as e:
            return make_response(jsonify({"content": "invalid", "message": "Authenticaion Failed", "err": str(e)}), 401)
        
        if not token:
            return make_response(jsonify({"content": "invalid", "message": "Authentication failed", "err": None}), 403)

        user: UserModel = UserService.get_instance(app).get_db_user_by_user_name(username)
        
        resp = make_response(jsonify({"content": user.dict(), "message": "Authentication successful", "err": None}), 200)
        resp.headers["Access-Control-Expose-Headers"] = "Jwt, User"
        resp.headers["jwt"] = token
        resp.headers["user"] = username

        return resp




class AuthValidation(Resource):
    @swagger.operation(
        summary="Validate authentication token",
        notes="Validate jwt authentication token",
        parameters=[
            {
                "name": "Authentication token",
                "description": 'Registered user  and login token {\"Jwt\":\"jwt token\", \"User\":\"email here\"}',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=response_messages
    )
    @metabolights_exception_handler
    def post(self):
        log_request(request)
        try:
            content = request.json
        except:
            content = parse_response_body(request)

        response = validate_token_in_request_body(content)
        return response
    
class OneTimeTokenValidation(Resource):
    @swagger.operation(
        summary="Get current JWT token from one time token",
        notes="Get current JWT token from one time token",
        parameters=[
            {
                "name": "one_time_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=response_messages
    )
    @metabolights_exception_handler
    def get(self):
        log_request(request)
        # User authentication
        one_time_token = None
        if "one_time_token" in request.headers:
            one_time_token = request.headers["one_time_token"]
        if not one_time_token:
            raise MetabolightsAuthorizationException(message="invalid token")

        try:
            redis: RedisStorage = get_redis_server()
            token_key = f"one-time-token-request:token:{one_time_token}"
            
            jwt = redis.get_value(token_key)
            if not jwt:
                raise MetabolightsAuthorizationException(message="invalid token")
            jwt = jwt.decode("utf-8")
            jwt_key = f"one-time-token-request:jwt:{jwt}"
            redis.delete_value(token_key)
            redis.delete_value(jwt_key)
            
            return jsonify({"jwt":  jwt })
        except Exception as ex:
            raise ex

class OneTimeTokenCreation(Resource):
    @swagger.operation(
        summary="Validate authentication token",
        notes="Validate jwt authentication token",
        parameters=[
            {
                "name": "Authorization",
                "description": "Authorization jwt token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=response_messages
    )
    @metabolights_exception_handler
    def get(self):
        log_request(request)
        # User authentication
        jwt = None
        if "Authorization" in request.headers:
            jwt = request.headers["Authorization"]
        if not jwt:
            raise MetabolightsAuthorizationException(message="invalid token")
        jwt = str(jwt).replace("Bearer ", "")
        
        try:
            AuthenticationManager.get_instance(app).validate_oauth2_token(token=jwt)
        except Exception as ex:
            raise MetabolightsAuthorizationException(message="User token is not valid or expired")
        jwt_key = f"one-time-token-request:jwt:{jwt}"
        
        redis: RedisStorage = get_redis_server()
        token = redis.get_value(jwt_key)
        if token:
            token_key = f"one-time-token-request:token:{token}"
            redis.delete_value(token_key)
        token = str(uuid.uuid4())
        token_key = f"one-time-token-request:token:{token}"
        ex = get_security_settings(app).one_time_token_expires_in_seconds
        redis.set_value(jwt_key, token, ex=ex)
        redis.set_value(token_key, jwt, ex=ex)
        return jsonify({"one_time_token": token})
        
class AuthUser(Resource):
    @swagger.operation(
        summary="Initialize user with authentication token",
        notes="Returns initial data for user",
        parameters=[
            {
                "name": "Authentication token",
                "description": 'Registered user  and login token {\"Jwt\":\"jwt token\", \"User\":\"email here\"}',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=response_messages
    )
    @metabolights_exception_handler
    def post(self):
        log_request(request)
        try:
            content = request.json
        except:
            content = parse_response_body(request)

        response = validate_token_in_request_body(content)
        if "jwt" in response.headers and "user" in response.headers and response.headers["jwt"]:
            username = response.headers["user"]
            try:
                UserService.get_instance(app).validate_username_with_submitter_or_super_user_role(username)
                m_user = UserService.get_instance(app).get_simplified_user_by_username(username)
                response_data = {"content": json.dumps({"owner": m_user.dict()}), "message": None, "err": None}
                response = make_response(response_data, 200)
                response.headers["Access-Control-Allow-Origin"] = "*"
            except Exception as e:
                return make_response(jsonify({"content": "invalid", "message": None, "err": None}), 403)
        return response



class AuthUserStudyPermissions(Resource):
    @swagger.operation(
        summary="Get permissions for a study",
        parameters=[
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": False,
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
    def get(self, study_id):
        log_request(request)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        permission = StudyAccessPermission()
        if not study_id:
            return jsonify(permission.dict()) 
                  
        filter_clause = lambda query: query.filter(Study.acc == study_id)
        return update_study_permission(permission, user_token, filter_clause)

class AuthUserStudyPermissions2(Resource):
    @swagger.operation(
        summary="Get permissions for a study",
        parameters=[
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False
            },
            {
                "name": "obfuscation_code",
                "description": "Obfuscation Code",
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
    def get(self, obfuscation_code):
        log_request(request)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        permission = StudyAccessPermission()
        if not obfuscation_code:
            return jsonify(permission.dict()) 
                  
        filter_clause = lambda query: query.filter(Study.obfuscationcode == obfuscation_code)
        return update_study_permission(permission, user_token, filter_clause, view_in_review_studies=True)
                      

def update_study_permission(permission: StudyAccessPermission, user_token, filter, view_in_review_studies=False):
    with DBManager.get_instance(app).session_maker() as db_session:
        query = db_session.query(Study.acc, Study.status, Study.obfuscationcode)
        study = filter(query).first()
        if not study:
            return jsonify(permission.dict())
        study_id = study['acc']
        permission.studyId = study_id
        
        if view_in_review_studies and StudyStatus(study['status']) == StudyStatus.INREVIEW:
            permission.studyStatus = StudyStatus(study['status']).name
            permission.obfuscationCode = study['obfuscationcode']
            permission.view = True
            permission.edit = False
            permission.delete = False
            return jsonify(permission.dict())
        
        anonymous_user = True
        try:
            user = None
            if user_token: 
                user = UserService.get_instance(app).validate_user_has_submitter_or_super_user_role(user_token)
            if user:
                anonymous_user = False
        except Exception:
            pass
        
        if anonymous_user:
            if StudyStatus(study['status']) == StudyStatus.PUBLIC:
                permission.studyStatus = StudyStatus(study['status']).name
                permission.view = True
                permission.edit = False
                permission.delete = False
                return jsonify(permission.dict())
            else:
                return jsonify(permission.dict())
                        
        permission.userName = user['username']
        base_query = db_session.query(User.id, User.username, User.role, User.status)
        query = base_query.join(Study, User.studies)
        owner = query.filter(Study.acc == study_id, User.apitoken == user['apitoken'],
                            User.status == UserStatus.ACTIVE.value).first()

        if owner:
            permission.submitterOfStudy = True
            
        permission.studyStatus = StudyStatus(study['status']).name
        permission.userRole = UserRole(user['role']).name
        if UserRole(user['role']) == UserRole.ROLE_SUPER_USER:
            permission.obfuscationCode = study['obfuscationcode']
            permission.view = True
            permission.edit = True
            permission.delete = True
            return jsonify(permission.dict())
            
        if owner:
            permission.obfuscationCode = study['obfuscationcode']
            permission.view = True
            permission.edit = True
            permission.delete = True
            return jsonify(permission.dict())             
        else:
            if StudyStatus(study['status']) == StudyStatus.PUBLIC:
                permission.view = True
                permission.edit = False
                permission.delete = False
                return jsonify(permission.dict())
            else:
                return jsonify(permission.dict())