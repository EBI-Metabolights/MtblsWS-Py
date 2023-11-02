import json
from typing import Union, List

from flask import jsonify, request
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger

from app.utils import MetabolightsDBException, MetabolightsException, metabolights_exception_handler
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
                "allowMultiple": False
            },
            {
                "name": "email",
                "description": "Metabolights User email",
                "paramType": "query",
                "type": "string",
                "required": False,
                "allowMultiple": False
            },
            {
                "name": "user_id",
                "description": "Metabolights User id",
                "paramType": "query",
                "type": "string",
                "required": False,
                "allowMultiple": False
            },
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
    def get(self):
        
        parser = reqparse.RequestParser()
        parser.add_argument('user_name', help="The row number of the cell to update (exclude header)")
        parser.add_argument('email', help="The column name of the cell to update")
        parser.add_argument('user_id', help="The column name of the cell to update")
        user_name = None
        email = None
        user_id = None
        if request.args:
            args = parser.parse_args(req=request)
            user_name = args['user_name']
            email = args['email']
            user_id = args['user_id']

        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        
        UserService.get_instance().validate_user_has_curator_role(user_token)
        
        try:
            if user_name or user_id or email:
                if user_name:
                    filter_clause = lambda query: query.filter(User.username == user_name)
                elif email:
                    filter_clause = lambda query: query.filter(User.email == email)
                elif user_id:
                    filter_clause = lambda query: query.filter(User.id == user_id)
                    
                user: UserModel = UserService.get_instance().get_db_user_by_filter_clause(filter_clause)
                if user:
                    if user.role  == UserRole.ROLE_SUPER_USER.value or user.role  == UserRole.SYSTEM_ADMIN.value:
                        user.curator = True
                    user.fullName = f"{user.firstName} {user.lastName}"
                    user.status = UserStatus(user.status).name
                        
                return jsonify({"content": user.model_dump(), "message": None, "error": None})
            else:
                users: List[UserModel] = UserService.get_instance().get_db_users_by_filter_clause()
                user_dict = []
                for user in users:
                    if user:
                        if user.role  == UserRole.ROLE_SUPER_USER.value or user.role  == UserRole.SYSTEM_ADMIN.value:
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
                "allowMultiple": False
            },
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
    def put(self):

        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
            
        UserService.get_instance().validate_user_has_curator_role(user_token)
        user: Union[None, UserModel] = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            user = UserModel.model_validate(data_dict)
            if hasattr(user, 'status') and isinstance(user.status, str):
                user.status = UserStatus.from_name(user.status).value
            
            if hasattr(user, 'role') and isinstance(user.role, str):
                user.role = UserRole.from_name(user.role).value
                
        except (Exception) as ex:
            raise MetabolightsException(http_code=404, message="Invalid user data input", exception=ex)
        db_session = DBManager.get_instance().session_maker()
        try:
            with db_session:
                query = db_session.query(User)
                query_filter = query.filter(User.id == user.userId)
                db_user = query_filter.first()
                if db_user:
                    m_user = UserModel.model_validate(db_user)
                    
                    
                    user_updates_data = user.dict(exclude_unset=True, by_alias=True)
                    updated_user_data = m_user.copy(update=user_updates_data)     
                    updated_values = updated_user_data.dict(by_alias=True)
                    for key, val in updated_values.items():
                        if hasattr(db_user, key):
                            setattr(db_user, key, val) if val else None
                    db_session.add(db_user)
                    db_session.commit()  
                    db_session.refresh(db_user)
                    m_user = UserModel.model_validate(db_user)
                    return jsonify({"content": m_user.model_dump(), "message": None, "error": None})
                else:
                    raise MetabolightsException(http_code=400, message="Invalid user id")   
        except (Exception) as ex:
            db_session.rollback()
            raise MetabolightsException(http_code=400, message="DB update error", exception=ex)


    
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
                "allowMultiple": False
            },
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
        user: Union[None, NewUserModel] = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            user = NewUserModel.model_validate(data_dict)
            if hasattr(user, 'status') and isinstance(user.status, str):
                user.status = UserStatus.from_name(user.status).value
            
            if hasattr(user, 'role') and isinstance(user.role, str):
                user.role = UserRole.from_name(user.role).value
            
            if user.userId:
                user.userId = None
                
        except (Exception) as ex:
            raise MetabolightsException(http_code=400, message="Invalid user data input", exception=ex)
        db_session = DBManager.get_instance().session_maker()
        try:
            with db_session:
                db_user = User()
                  
                user_data = user.dict(by_alias=True)
                for key, val in user_data.items():
                    if hasattr(db_user, key):
                        setattr(db_user, key, val)
                db_session.add(db_user)
                db_session.commit()  
                db_session.refresh(db_user)
                m_user = NewUserModel.model_validate(db_user)
                return jsonify({"content": m_user.model_dump(), "message": None, "error": None})
        except (Exception) as ex:
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
                "allowMultiple": False
            },
            {
                "name": "email",
                "description": "Metabolights User email",
                "paramType": "query",
                "type": "string",
                "required": False,
                "allowMultiple": False
            },
            {
                "name": "user_id",
                "description": "Metabolights User id",
                "paramType": "query",
                "type": "string",
                "required": False,
                "allowMultiple": False
            },
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
    def delete(self):
        
        user_name = None
        email = None
        user_id = None
        if "user_name" in request.args:
            user_name = request.args['user_name']
        
        if "email" in request.args:
            email = request.args['email']
            
        if "user_id" in request.args:
            user_id = request.args['user_id']

        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        
        UserService.get_instance().validate_user_has_curator_role(user_token)
        
        try:
            if user_name or user_id or email:
                if user_name:
                    filter_clause = lambda query: query.filter(User.username == user_name)
                elif email:
                    filter_clause = lambda query: query.filter(User.email == email)
                elif user_id:
                    filter_clause = lambda query: query.filter(User.id == user_id)
                db_session = DBManager.get_instance().session_maker()
                try:
                    with db_session:
                        query = db_session.query(User)
                        db_user: User = filter_clause(query).first()

                        if db_user:
                            db_session.delete(db_user)
                            db_session.commit()
                        else:
                            raise MetabolightsDBException(http_code=404, message=f"Not a valid user.")
                        username = db_user.username
                        user_id = db_user.id                        
                        return jsonify({"content": f"The selected user is deleted. Deleted user's username: {username}, id: {user_id}", "message": None, "error": None})
                except Exception as e:
                    raise MetabolightsDBException(message=f"Error while retreiving user from database", exception=e)
                
                
            else:
                raise MetabolightsException(http_code=400, message=f"Select at least one parameter: user_name, email, id")

        except Exception as ex:
            raise ex