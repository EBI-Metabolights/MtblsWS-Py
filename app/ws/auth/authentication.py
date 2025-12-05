import logging
import re

import jwt
from flask import jsonify, make_response, request
from flask_restful import Resource
from flask_restful_swagger import swagger

from app.config import get_settings
from app.utils import (
    MetabolightsAuthorizationException,
    MetabolightsException,
    metabolights_exception_handler,
)
from app.ws.auth.auth_manager import AuthenticationManager
from app.ws.auth.one_time_token import (
    create_one_time_token,
    get_jwt_with_one_time_token,
)
from app.ws.auth.permissions import (
    auth_endpoint,
    get_auth_data,
    validate_submission_view,
    validate_user_has_submitter_or_super_user_role,
)
from app.ws.db.models import UserModel
from app.ws.db.types import UserRole
from app.ws.study.user_service import UserService

logger = logging.getLogger("wslog")


def validate_token_in_request_body(content):
    if not content or "jwt" not in content:
        return make_response(
            jsonify(
                {
                    "content": False,
                    "message": "Invalid request. token and user inputs are required",
                    "err": None,
                }
            ),
            400,
        )
    jwt_token = content.get("jwt")
    username = content.get("user") or ""

    try:
        AuthenticationManager.get_instance().validate_oauth2_token(token=jwt_token)
    except MetabolightsAuthorizationException as e:
        return make_response(
            jsonify({"content": "invalid", "message": e.message, "err": str(e)}), 401
        )
    except MetabolightsException as e:
        return make_response(
            jsonify({"content": "invalid", "message": e.message, "err": str(e)}), 401
        )
    except Exception as e:
        return make_response(
            jsonify(
                {"content": "invalid", "message": "Authenticaion Failed", "err": str(e)}
            ),
            401,
        )

    response = make_response(
        jsonify(
            {"content": "true", "message": "Authentication successful", "err": None}
        ),
        200,
    )
    response.headers["Access-Control-Expose-Headers"] = "Jwt, User"
    response.headers["Jwt"] = jwt_token
    response.headers["User"] = username
    return response


def parse_response_body(request):
    pattern_string = r"[\|{|\|}|\s|\"|']"
    pattern = re.compile(pattern_string)

    request_body = request.data.decode()
    request_body = re.sub(pattern, "", request_body)
    split_body = re.split(",", request_body)
    results = {}
    for item in split_body:
        if ":" in item:
            param = item.split(":")
            results[param[0]] = param[1]
    return results


response_messages = [
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
]


class AuthLoginWithToken(Resource):
    @swagger.operation(
        summary="Authenticate user with apitoken and returns JWT token in response header and user content in response body.",
        parameters=[
            {
                "name": "authdata",
                "description": 'User MetaboLights API token {"token":"api token here"}',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=response_messages,
    )
    @metabolights_exception_handler
    def post(self):
        auth_endpoint(request)
        try:
            content = request.json
        except:
            content = parse_response_body(request)

        if not content or "token" not in content:
            return make_response(
                jsonify(
                    {
                        "content": False,
                        "message": "Invalid request. token and user inputs are required",
                        "err": None,
                    }
                ),
                400,
            )

        api_token = content["token"]
        auth_manager = AuthenticationManager.get_instance()
        user = UserService.get_instance(
            auth_manager
        ).validate_user_has_submitter_or_super_user_role(api_token)
        settings = get_settings().auth.configuration
        if UserRole(user["role"]) == UserRole.ROLE_SUPER_USER:
            exp = settings.admin_jwt_token_expires_in_mins
        else:
            exp = settings.access_token_expires_delta

        try:
            token, refresh_token = (
                AuthenticationManager.get_instance().create_oauth2_token_by_api_token(
                    api_token, exp_period_in_mins=exp
                )
            )
        except MetabolightsException as e:
            return make_response(
                jsonify({"content": "invalid", "message": e.message, "err": str(e)}),
                e.http_code,
            )
        except Exception as e:
            return make_response(
                jsonify(
                    {
                        "content": "invalid",
                        "message": "Authentication failed",
                        "err": str(e),
                    }
                ),
                403,
            )
        resp = make_response(
            jsonify(
                {
                    "content": user.model_dump(),
                    "message": "Authentication successful",
                    "err": None,
                }
            ),
            200,
        )
        resp.headers["Access-Control-Expose-Headers"] = "Jwt, User, Refresh-Token"
        resp.headers["Jwt"] = token
        resp.headers["Refresh-Token"] = refresh_token
        resp.headers["User"] = user["username"]


class AuthLogin(Resource):
    @swagger.operation(
        summary="Authenticate user with username and password and returns authentication token in response header.",
        parameters=[
            {
                "name": "authdata",
                "description": 'Registered user address and password {"email":"email here", "secret":"password here"}',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=response_messages,
    )
    @metabolights_exception_handler
    def post(self):
        auth_endpoint(request)
        try:
            content = request.json
        except:
            content = parse_response_body(request)

        if not content or "email" not in content or "secret" not in content:
            return make_response(
                jsonify(
                    {
                        "content": False,
                        "message": "Invalid request. email and secret inputs are required",
                        "err": None,
                    }
                ),
                400,
            )

        username = content["email"]
        password = content["secret"]
        token = None
        refresh_token = None
        try:
            token, refresh_token = (
                AuthenticationManager.get_instance().create_oauth2_token(
                    username, password
                )
            )
        except MetabolightsAuthorizationException as e:
            return make_response(
                jsonify({"content": "invalid", "message": e.message, "err": str(e)}),
                e.http_code,
            )
        except MetabolightsException as e:
            return make_response(
                jsonify({"content": "invalid", "message": e.message, "err": str(e)}),
                e.http_code,
            )
        except Exception as e:
            return make_response(
                jsonify(
                    {
                        "content": "invalid",
                        "message": "Authenticaion Failed",
                        "err": str(e),
                    }
                ),
                401,
            )

        if not token:
            return make_response(
                jsonify(
                    {
                        "content": "invalid",
                        "message": "Authentication failed",
                        "err": None,
                    }
                ),
                403,
            )
        auth_manager = AuthenticationManager.get_instance()
        user: UserModel = UserService.get_instance(
            auth_manager
        ).get_db_user_by_user_name(username)

        resp = make_response(
            jsonify(
                {
                    "content": user.model_dump(),
                    "message": "Authentication successful",
                    "err": None,
                }
            ),
            200,
        )
        resp.headers["Access-Control-Expose-Headers"] = "Jwt, User, Refresh-Token"
        resp.headers["Jwt"] = token
        resp.headers["Refresh-Token"] = refresh_token
        resp.headers["User"] = username

        return resp


class RefreshToken(Resource):
    @swagger.operation(
        summary="Create new JWT token with valid refresh token",
        notes="Create new JWT token with valid refresh token",
        parameters=[
            {
                "name": "Authentication token",
                "description": 'Registered user  and login token {"Jwt":"jwt token", "User":"email here"}',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=response_messages,
    )
    @metabolights_exception_handler
    def post(self):
        auth_endpoint(request)
        try:
            content = request.json
        except:
            content = parse_response_body(request)
        response = None
        content = content or {}
        jwt_content = {
            k.lower(): v
            for k, v in content.items()
            if k and k.lower() in {"jwt", "user"}
        }
        jwt_token = jwt_content.get("jwt")
        username = jwt_content.get("user")
        response = validate_token_in_request_body(jwt_content)
        access_token = None
        refresh_token = None
        email = None
        if response.status_code == 200:
            try:
                access_token, refresh_token = (
                    AuthenticationManager.get_instance().refresh_token(jwt_token)
                )
                options = {"verify_signature": False}
                payload = jwt.decode(
                    access_token,
                    options=options,
                )
                email = payload.get("email", "")

            except MetabolightsAuthorizationException as e:
                return make_response(
                    jsonify(
                        {"content": "invalid", "message": e.message, "err": str(e)}
                    ),
                    e.http_code,
                )
            except MetabolightsException as e:
                return make_response(
                    jsonify(
                        {"content": "invalid", "message": e.message, "err": str(e)}
                    ),
                    e.http_code,
                )
            except Exception as e:
                return make_response(
                    jsonify(
                        {
                            "content": "invalid",
                            "message": "Authenticaion Failed",
                            "err": str(e),
                        }
                    ),
                    401,
                )
        response.headers["Access-Control-Expose-Headers"] = "Jwt, User, Refresh-Token"
        response.headers["Jwt"] = access_token
        response.headers["Refresh-Token"] = refresh_token
        response.headers["User"] = email
        return response


class AuthValidation(Resource):
    @swagger.operation(
        summary="Validate authentication token",
        notes="Validate jwt authentication token",
        parameters=[
            {
                "name": "Authentication token",
                "description": 'Registered user  and login token {"Jwt":"jwt token", "User":"email here"}',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=response_messages,
    )
    @metabolights_exception_handler
    def post(self):
        auth_endpoint(request)
        try:
            content = request.json
        except:
            content = parse_response_body(request)
        response = None
        content = content or {}
        jwt_content = {
            k.lower(): v
            for k, v in content.items()
            if k and k.lower() in {"jwt", "user"}
        }
        jwt_token = jwt_content.get("jwt")
        username = jwt_content.get("user")
        response = validate_token_in_request_body(jwt_content)
        if not response:
            return make_response(
                jsonify({"content": "invalid", "message": "", "err": ""}), 401
            )
        response.headers["Access-Control-Expose-Headers"] = "Jwt, User"
        response.headers["jwt"] = jwt_token
        response.headers["user"] = username
        return response


class OneTimeTokenValidation(Resource):
    @swagger.operation(
        summary="Get current JWT token from one time token",
        notes="Get current JWT token from one time token",
        parameters=[
            {
                "name": "one-time-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=response_messages,
    )
    @metabolights_exception_handler
    def get(self):
        auth_endpoint(request)
        one_time_token = None
        if "one_time_token" in request.headers:
            one_time_token = request.headers["one_time_token"]
        if not one_time_token:
            raise MetabolightsAuthorizationException(message="invalid token")
        jwt = get_jwt_with_one_time_token(one_time_token)
        if not jwt:
            raise MetabolightsException(message="Not valid JWT or token", http_code=404)
        return jsonify({"jwt": jwt})


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
                "allowMultiple": False,
            }
        ],
        responseMessages=response_messages,
    )
    @metabolights_exception_handler
    def get(self):
        auth_endpoint(request)
        result = validate_user_has_submitter_or_super_user_role(request)
        jwt = result.context.validated_jwt
        one_time_token = create_one_time_token(jwt)
        if not one_time_token:
            raise MetabolightsException(message="One time token is not created.")
        return jsonify({"one_time_token": one_time_token})


class AuthUser(Resource):
    @swagger.operation(
        summary="Initialize user with authentication token",
        notes="Returns initial data for user",
        parameters=[
            {
                "name": "Authentication token",
                "description": 'Registered user  and login token {"Jwt":"jwt token", "User":"email here"}',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            }
        ],
        responseMessages=response_messages,
    )
    @metabolights_exception_handler
    def post(self):
        auth_endpoint(request)
        auth_data = get_auth_data(request)
        jwt = auth_data.jwt
        username = None
        if not jwt:
            try:
                content = request.json
            except:
                content = parse_response_body(request)
            filtered_header = {
                k.lower(): v
                for k, v in content.items()
                if k and k.lower() in {"user", "jwt"}
            }
            jwt = filtered_header.get("jwt")
            username = request.headers.get("user")
            if not username:
                username = filtered_header.get("user")
        if not jwt:
            return make_response(
                jsonify({"content": "invalid jwt", "message": None, "err": ""}),
                401,
            ), None
        try:
            user = AuthenticationManager.get_instance().validate_oauth2_token(jwt)
            resp = make_response(
                jsonify(
                    {
                        "content": user.model_dump(),
                        "message": "Authentication successful",
                        "err": None,
                    }
                ),
                200,
            )
            resp.headers["Access-Control-Expose-Headers"] = "Jwt, User"
            resp.headers["Jwt"] = jwt
            resp.headers["User"] = user.userName
            return resp
        except Exception as ex:
            return make_response(
                jsonify({"content": "invalid jwt", "message": None, "err": str(ex)}),
                401,
            )


class AuthUserStudyPermissions(Resource):
    @swagger.operation(
        summary="Return study permissions of user.",
        notes="""
        If user has read permision for study, response contains obfuscation code as well. Example response format:
        {
            "delete": false,
            "edit": false,
            "obfuscationCode": "",
            "studyId": "MTBLS10",
            "studyStatus": "",
            "partner": false,
            "submitterOfStudy": false,
            "userName": "",
            "userRole": "",
            "view": false,
            "scopes": {"metadata-files": ["read"]}
            }
        """,
        parameters=[
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
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
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def get(self, study_id):
        auth_endpoint(request)
        result = validate_submission_view(
            request, fail_silently=True, user_required=False
        )
        return jsonify(result.permission.model_dump(by_alias=True))


class AuthUserStudyPermissions2(Resource):
    @swagger.operation(
        summary="Return study permissions of user with study obfuscation code.",
        parameters=[
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
            {
                "name": "obfuscation_code",
                "description": "Obfuscation Code",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
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
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def get(self, obfuscation_code):
        auth_endpoint(request)
        result = validate_submission_view(
            request, fail_silently=True, user_required=False
        )

        return jsonify(result.permission.model_dump(by_alias=True))
