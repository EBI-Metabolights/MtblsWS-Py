from functools import lru_cache
from http.client import HTTPException
import json
import logging
import re
import uuid

from flask import current_app as app
from flask import jsonify, make_response, request
from flask_restful import Resource
from flask_restful_swagger import swagger
from keycloak import KeycloakAuthenticationError, KeycloakOpenID

from app.config import get_settings
from app.utils import (
    MetabolightsAuthorizationException,
    MetabolightsException,
    metabolights_exception_handler,
)
from app.ws.auth.auth_manager import AuthenticationManager
from app.ws.auth.utils import (
    get_permission_by_obfuscation_code,
    get_permission_by_study_id,
)
from app.ws.db.models import StudyAccessPermission, UserModel
from app.ws.db.types import StudyStatus, UserRole, UserStatus
from app.ws.db_connection import create_user
from app.ws.redis.redis import RedisStorage, get_redis_server
from app.ws.study.user_service import UserService

logger = logging.getLogger("wslog")


@lru_cache(1)
def get_keycloak_openid() -> KeycloakOpenID:
    settings = get_settings().auth.openid_connect_client
    keycloak_openid = KeycloakOpenID(
        server_url=settings.server_url,
        realm_name=settings.realm_name,
        client_id=settings.client_id,
        client_secret_key=settings.client_secret,
    )
    return keycloak_openid


def validate_token_in_request_body(content):
    if (
        not content
        or ("jwt" not in content and "Jwt" not in content)
        or ("user" not in content and "User" not in content)
    ):
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
    jwt_token = ""
    username = ""
    for key in content:
        if key.lower() == "jwt":
            jwt_token = content[key]
        elif key.lower() == "user":
            username = content[key]

    try:
        user_in_token = AuthenticationManager.get_instance().validate_oauth2_token(
            token=jwt_token
        )
    except MetabolightsAuthorizationException as e:
        return make_response(
            jsonify({"content": "invalid", "message": e.message, "err": e}), 401
        )
    except MetabolightsException as e:
        return make_response(
            jsonify({"content": "invalid", "message": e.message, "err": e}), 401
        )
    except Exception as e:
        return make_response(
            jsonify(
                {"content": "invalid", "message": "Authenticaion Failed", "err": e}
            ),
            401,
        )

    if not user_in_token or user_in_token.userName != username:
        return make_response(
            jsonify(
                {
                    "content": "invalid",
                    "message": "Not a valid token for user",
                    "err": None,
                }
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
    response.headers["jwt"] = jwt_token
    response.headers["user"] = username
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
        user = (
            UserService.get_instance().validate_user_has_submitter_or_super_user_role(
                api_token
            )
        )
        settings = get_settings().auth.configuration
        if UserRole(user["role"]) == UserRole.ROLE_SUPER_USER:
            exp = settings.admin_jwt_token_expires_in_mins
        else:
            exp = settings.access_token_expires_delta

        try:
            token = (
                AuthenticationManager.get_instance().create_oauth2_token_by_api_token(
                    api_token, exp_period_in_mins=exp
                )
            )
        except MetabolightsException as e:
            return make_response(
                jsonify({"content": "invalid", "message": e.message, "err": e}),
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

        resp = make_response(
            jsonify(
                {"content": True, "message": "Authentication successful", "err": None}
            ),
            200,
        )
        resp.headers["Access-Control-Expose-Headers"] = "Jwt, User"
        resp.headers["jwt"] = token
        resp.headers["user"] = user.username

        return resp


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

        try:
            token = AuthenticationManager.get_instance().create_oauth2_token(
                username, password
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

        user: UserModel = UserService.get_instance().get_db_user_by_user_name(username)

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
        try:
            content = request.json
        except:
            content = parse_response_body(request)

        response = validate_token_in_request_body(content)
        return response


class OneTimeTokenValidation(Resource):
    @swagger.operation(
        summary="[Deprecated] Get current JWT token from one time token",
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

            return jsonify({"jwt": jwt})
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
                "allowMultiple": False,
            }
        ],
        responseMessages=response_messages,
    )
    @metabolights_exception_handler
    def get(self):
        jwt = None
        if "Authorization" in request.headers:
            jwt = request.headers["Authorization"]
        if not jwt:
            raise MetabolightsAuthorizationException(message="invalid token")
        jwt = str(jwt).replace("Bearer ", "")

        try:
            AuthenticationManager.get_instance().validate_oauth2_token(token=jwt)
        except Exception as ex:
            raise MetabolightsAuthorizationException(
                message="User token is not valid or expired"
            )
        jwt_key = f"one-time-token-request:jwt:{jwt}"

        redis: RedisStorage = get_redis_server()
        token = redis.get_value(jwt_key)
        if token:
            token_key = f"one-time-token-request:token:{token}"
            redis.delete_value(token_key)
        token = str(uuid.uuid4())
        token_key = f"one-time-token-request:token:{token}"
        ex = get_settings().auth.configuration.one_time_token_expires_in_seconds
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
        try:
            content = request.json
        except:
            try:
                content = parse_response_body(request)
            except Exception as e:
                logger.warning(str(e))
                content = {}

        # response = validate_token_in_request_body(content)
        jwt = request.headers.get("jwt")
        if not jwt:
            jwt = content.get("jwt")

        username = request.headers.get("user")
        if not username:
            username = content.get("user")
        response, email = AuthUser.verify_token(jwt, username)
        if jwt and email:
            try:
                # UserService.get_instance().validate_username_with_submitter_or_super_user_role(username)

                m_user = UserService.get_instance().get_simplified_user_by_username(
                    email
                )
                response_data = {
                    "content": json.dumps({"owner": m_user.model_dump()}),
                    "message": None,
                    "err": None,
                }
                response = make_response(response_data, 200)
                response.headers["Access-Control-Allow-Origin"] = "*"
            except Exception as e:
                return make_response(
                    jsonify({"content": "invalid", "message": None, "err": None}), 401
                )
        return response

    @staticmethod
    def verify_token(token: str, email: str):
        """
        Verify the given token and return user information.
        """
        try:
            user_info = get_keycloak_openid().userinfo(token)
            if not user_info or not user_info.get("email"):
                raise HTTPException(status_code=401, detail="Invalid token")
            username = user_info.get("email")
            if username != email:
                return make_response(jsonify({"content": "invalid", "message": "jwt token user and input user is not same", "err": None}), 401)
            try:
                # check user is created
                UserService.get_instance().get_db_user_by_user_name(username)
            except MetabolightsAuthorizationException as ex:
                # user is defined in keycloak but it is not defined in db
                email_verified = user_info.get("email_verified", False)
                if not email_verified:
                    raise HTTPException(status_code=401, detail="Email address is not verified")
                orcid = user_info.get("orcid", "") or ""
                orcid = orcid.replace("https://orcid.org/", "") or None
                success, message = create_user(
                    first_name=user_info.get("given_name"),
                    last_name=user_info.get("family_name"),
                    email=user_info.get("email"),
                    affiliation=user_info.get("affiliation"),
                    affiliation_url=user_info.get("affiliationUrl"),
                    address=user_info.get("address", {}).get("country", None),
                    orcid=orcid,
                    api_token=str(uuid.uuid4()),
                    password_encoded=str(uuid.uuid4()),
                    metaspace_api_key=None,
                )
                if success:
                    logger.info(message)
                else:
                    logger.warning(message)
                
            response = make_response(
                jsonify(
                    {
                        "content": "true",
                        "message": "Authentication successful",
                        "err": None,
                    }
                ),
                200,
            )
            response.headers["Access-Control-Expose-Headers"] = "Jwt, User"
            response.headers["jwt"] = token
            response.headers["user"] = username

                
            return response, user_info.get("email")

        except KeycloakAuthenticationError as ex:
            return make_response(
                jsonify({"content": "invalid", "message": None, "err": None}), 401
            ), None
        except Exception as ex:
            import traceback

            traceback.print_exc()
            return make_response(
                jsonify({"content": "invalid", "message": None, "err": None}), 401
            ), None


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
            "submitterOfStudy": false,
            "userName": "",
            "userRole": "",
            "view": false
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
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        permission = StudyAccessPermission()
        if not study_id:
            return jsonify(permission.model_dump())

        permission = get_permission_by_study_id(study_id, user_token)
        return jsonify(permission.model_dump())


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
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        permission = StudyAccessPermission()
        if not obfuscation_code:
            return jsonify(permission.model_dump())

        permission = get_permission_by_obfuscation_code(user_token, obfuscation_code)
        return jsonify(permission.model_dump())
