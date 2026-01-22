import json
from typing import List, Union

from flask import jsonify, request
from flask_restful import Resource
from flask_restful_swagger import swagger

from app.utils import (
    MetabolightsAuthenticationException,
    MetabolightsDBException,
    MetabolightsException,
    metabolights_exception_handler,
)
from app.ws.auth.auth_manager import AuthenticationManager
from app.ws.auth.permissions import (
    auth_endpoint,
    raise_deprecation_error,
    validate_user_has_curator_role,
    validate_user_has_submitter_or_super_user_role,
)
from app.ws.db.dbmanager import DBManager
from app.ws.db.models import NewUserModel, UserModel
from app.ws.db.schemes import User
from app.ws.db.types import UserRole, UserStatus
from app.ws.study.user_service import UserService


class UserAccounts(Resource):
    @swagger.operation(
        summary="Get Metabolights user account",
        parameters=[
            {
                "name": "user_name",
                "description": "Metabolights User Name",
                "paramType": "query",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
            {
                "name": "email",
                "description": "Metabolights User email",
                "paramType": "query",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
            {
                "name": "user_id",
                "description": "Metabolights User id",
                "paramType": "query",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
            {
                "name": "user-token",
                "description": "Admin/Curator API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
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
    def get(self):
        auth_endpoint(request)
        result = validate_user_has_submitter_or_super_user_role(request)

        user_name = request.args.get("user_name")
        email = request.args.get("email")
        user_id = request.args.get("user_id")
        if result.context.user_role not in {
            UserRole.ROLE_SUPER_USER,
            UserRole.SYSTEM_ADMIN,
        }:
            if not user_name or result.context.username not in {user_name, email}:
                raise MetabolightsAuthenticationException(
                    message="User has not permission"
                )

        try:
            if user_name or user_id or email:
                if user_name:
                    filter_clause = lambda query: query.filter(
                        User.username == user_name
                    )
                elif email:
                    filter_clause = lambda query: query.filter(User.email == email)
                elif user_id:
                    filter_clause = lambda query: query.filter(User.id == user_id)
                auth_manager = AuthenticationManager.get_instance()
                user: UserModel = UserService.get_instance(
                    auth_manager
                ).get_db_user_by_filter_clause(filter_clause)
                if user:
                    if (
                        user.role == UserRole.ROLE_SUPER_USER.value
                        or user.role == UserRole.SYSTEM_ADMIN.value
                    ):
                        user.curator = True
                    user.fullName = f"{user.firstName} {user.lastName}"
                    user.status = UserStatus(user.status).name

                return jsonify(
                    {"content": user.model_dump(), "message": None, "error": None}
                )
            else:
                auth_manager = AuthenticationManager.get_instance()
                users: List[UserModel] = UserService.get_instance(
                    auth_manager
                ).get_db_users_by_filter_clause()
                user_dict = []
                for user in users:
                    if user:
                        if (
                            user.role == UserRole.ROLE_SUPER_USER.value
                            or user.role == UserRole.SYSTEM_ADMIN.value
                        ):
                            user.curator = True
                        user.fullName = f"{user.firstName} {user.lastName}"
                        user.status = UserStatus(user.status).name
                    user_dict.append(user.model_dump())

                return jsonify({"content": user_dict, "message": None, "error": None})

        except Exception as ex:
            raise ex

    @swagger.operation(
        summary="Update Metabolights user account",
        parameters=[
            {
                "name": "user_data",
                "description": "Metabolights User Name",
                "paramType": "body",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "user-token",
                "description": "Admin/Curator API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
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
    def put(self):
        raise_deprecation_error(request)
        auth_endpoint(request)
        validate_user_has_curator_role(request)

        user: Union[None, UserModel] = None
        try:
            data_dict = json.loads(request.data.decode("utf-8"))
            user = UserModel.model_validate(data_dict)
            if hasattr(user, "status") and isinstance(user.status, str):
                user.status = UserStatus.from_name(user.status).value

            if hasattr(user, "role") and isinstance(user.role, str):
                user.role = UserRole.from_name(user.role).value

        except Exception as ex:
            raise MetabolightsException(
                http_code=404, message="Invalid user data input", exception=ex
            )
        db_session = DBManager.get_instance().session_maker()
        try:
            with db_session:
                query = db_session.query(User)
                query_filter = query.filter(User.id == user.userId)
                db_user = query_filter.first()
                if db_user:
                    m_user = UserModel.model_validate(db_user)

                    user_updates_data = user.model_dump(
                        exclude_unset=True, by_alias=True
                    )
                    updated_user_data = m_user.model_copy(update=user_updates_data)
                    updated_values = updated_user_data.model_dump(by_alias=True)
                    for key, val in updated_values.items():
                        if hasattr(db_user, key):
                            setattr(db_user, key, val) if val else None
                    db_session.add(db_user)
                    db_session.commit()
                    db_session.refresh(db_user)
                    m_user = UserModel.model_validate(db_user)
                    return jsonify(
                        {"content": m_user.model_dump(), "message": None, "error": None}
                    )
                else:
                    raise MetabolightsException(
                        http_code=400, message="Invalid user id"
                    )
        except Exception as ex:
            db_session.rollback()
            raise MetabolightsException(
                http_code=400, message="DB update error", exception=ex
            )

    @swagger.operation(
        summary="Add new Metabolights user account",
        notes="""
        Example request body
        <pre><code>
            {
                "address": "GB",
                "affiliation": "EBI",
                "affiliationUrl": "www.ebi.ac.uk",
                "apiToken": "",
                "curator": false,
                "dbPassword": "",
                "email": "",
                "firstName": "",
                "fullName": "",
                "joinDate": "2023-03-24T14:55:04.881000",
                "lastName": "",
                "mobilePhoneNumber": null,
                "officePhoneNumber": null,
                "orcid": "",
                "role": 0,
                "status": "ACTIVE",
                "userName": ""
            }
            </code></pre>
        """,
        parameters=[
            {
                "name": "user_data",
                "description": "Metabolights User Name",
                "paramType": "body",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "user-token",
                "description": "Admin/Curator API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
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
    def post(self):
        raise_deprecation_error(request)
        auth_endpoint(request)
        validate_user_has_curator_role(request)

        user: Union[None, NewUserModel] = None
        try:
            data_dict = json.loads(request.data.decode("utf-8"))
            user = NewUserModel.model_validate(data_dict)
            if hasattr(user, "status") and isinstance(user.status, str):
                user.status = UserStatus.from_name(user.status).value

            if hasattr(user, "role") and isinstance(user.role, str):
                user.role = UserRole.from_name(user.role).value

            if user.userId:
                user.userId = None

        except Exception as ex:
            raise MetabolightsException(
                http_code=400, message="Invalid user data input", exception=ex
            )
        db_session = DBManager.get_instance().session_maker()
        try:
            with db_session:
                db_user = User()

                user_data = user.model_dump(by_alias=True)
                for key, val in user_data.items():
                    if hasattr(db_user, key):
                        setattr(db_user, key, val)
                db_session.add(db_user)
                db_session.commit()
                db_session.refresh(db_user)
                m_user = NewUserModel.model_validate(db_user)
                return jsonify(
                    {"content": m_user.model_dump(), "message": None, "error": None}
                )
        except Exception as ex:
            db_session.rollback()
            raise MetabolightsException(http_code=400, message="DB error", exception=ex)

    @swagger.operation(
        summary="Delete Metabolights user account",
        parameters=[
            {
                "name": "user_name",
                "description": "Metabolights User Name",
                "paramType": "query",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
            {
                "name": "email",
                "description": "Metabolights User email",
                "paramType": "query",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
            {
                "name": "user_id",
                "description": "Metabolights User id",
                "paramType": "query",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
            {
                "name": "user-token",
                "description": "Admin/Curator API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
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
    def delete(self):
        raise_deprecation_error(request)
        auth_endpoint(request)
        validate_user_has_curator_role(request)

        user_name = request.args.get("user_name")
        email = request.args.get("email")
        user_id = request.args.get("user_id")
        try:
            if user_name or user_id or email:
                if user_name:
                    filter_clause = lambda query: query.filter(
                        User.username == user_name
                    )
                elif email:
                    filter_clause = lambda query: query.filter(User.email == email)
                elif user_id:
                    filter_clause = lambda query: query.filter(User.id == user_id)
                db_session = DBManager.get_instance().session_maker()
                try:
                    with db_session:
                        query = db_session.query(User)
                        db_user: Union[None, User] = filter_clause(query).first()

                        if db_user:
                            db_session.delete(db_user)
                            db_session.commit()
                        else:
                            raise MetabolightsDBException(
                                http_code=404, message="Not a valid user."
                            )
                        username = db_user.username
                        user_id = db_user.id
                        return jsonify(
                            {
                                "content": f"The selected user is deleted. Deleted user's username: {username}, id: {user_id}",
                                "message": None,
                                "error": None,
                            }
                        )
                except Exception as e:
                    raise MetabolightsDBException(
                        message="Error while retreiving user from database",
                        exception=e,
                    )

            else:
                raise MetabolightsException(
                    http_code=400,
                    message="Select at least one parameter: user_name, email, id",
                )

        except Exception as ex:
            raise ex
