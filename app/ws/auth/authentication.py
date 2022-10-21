import json
import logging
import re

from flask import request, jsonify, make_response, current_app as app
from flask_restful import Resource
from flask_restful_swagger import swagger

from app.utils import metabolights_exception_handler, MetabolightsException
from app.ws.auth.auth_manager import AuthenticationManager
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

    username = content["user"] if "user" in content else content["User"]
    jwt_token = content["jwt"] if "jwt" in content else content["Jwt"]

    try:
        user_in_token = AuthenticationManager.get_instance(app).validate_oauth2_token(token=jwt_token)
    except Exception as e:
        return make_response(jsonify({"content": "invalid", "message": "Invalid token", "err": None}), 403)

    if not user_in_token or user_in_token.userName != username:
        return make_response(jsonify({"content": "invalid", "message": "Not a valid token for user", "err": None}), 403)

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

        if not content or "token" not in content or "user" not in content or ("user" in content and "userName" not in content["user"]):
            return make_response(jsonify({"content": False,
                                          "message": "Invalid request. token and user inputs are required",
                                          "err": None}), 400)

        username = content["user"]["userName"]
        api_token = content["token"]
        user = UserService.get_instance(app).validate_user_has_submitter_or_super_user_role(api_token)
        if user.username != username:
            return make_response(jsonify({"content": "invalid", "message": "Authentication failed. Username or api token invalid", "err": ''}),
                                 403)
        try:
            token = AuthenticationManager.get_instance(app).create_oauth2_token_by_api_token(api_token)
        except MetabolightsException as e:
            return make_response(jsonify({"content": "invalid", "message": e.message, "err": e.exception}), e.http_code)
        except Exception as e:
            return make_response(jsonify({"content": "invalid", "message": "Authentication failed", "err": str(e)}), 403)

        if not token:
            return make_response(jsonify({"content": "invalid", "message": "Authentication failed", "err": None}), 403)

        resp = make_response(jsonify({"content": True, "message": "Authentication successful", "err": None}), 200)
        resp.headers["Access-Control-Expose-Headers"] = "Jwt, User"
        resp.headers["jwt"] = token
        resp.headers["user"] = username

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
        except MetabolightsException as e:
            return make_response(jsonify({"content": "invalid", "message": e.message, "err": e.exception}), e.http_code)
        except Exception as e:
            return make_response(jsonify({"content": "invalid", "message": "Authentication failed", "err": str(e)}), 403)

        if not token:
            return make_response(jsonify({"content": "invalid", "message": "Authentication failed", "err": None}), 403)

        resp = make_response(jsonify({"content": True, "message": "Authentication successful", "err": None}), 200)
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
