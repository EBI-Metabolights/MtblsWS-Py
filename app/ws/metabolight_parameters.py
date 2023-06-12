import json

from flask import current_app as app, jsonify, request
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger

from app.utils import MetabolightsDBException, metabolights_exception_handler, MetabolightsException
from app.ws.db.dbmanager import DBManager
from app.ws.db.models import MetabolightsParameterModel, UserModel
from app.ws.db.schemes import MetabolightsParameter
from app.ws.study.user_service import UserService

class MetabolightsParameters(Resource):
    @swagger.operation(
        summary="Get Metabolights parameter",
        parameters=[
            {
                "name": "name",
                "description": "Metabolights Parameter Name",
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
        parser.add_argument('name')
        name = None
        if request.args:
            args = parser.parse_args(req=request)
            name = args['name']
            

        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        
        UserService.get_instance().validate_user_has_curator_role(user_token)
        db_session = DBManager.get_instance().session_maker()
        try:
            if name:
                filter_clause = lambda query: query.filter(MetabolightsParameter.name == name)
                
                try:
                    with db_session:
                        query = db_session.query(MetabolightsParameter)
                        db_param: MetabolightsParameter = filter_clause(query).first()
                        
                        m_param = MetabolightsParameterModel.from_orm(db_param)
                        return jsonify({"content": m_param.dict(), "message": None, "error": None})
                except Exception as e:
                    raise MetabolightsDBException(message=f"Error while retrieving parameter from database", exception=e)
            else:
                try:
                    with db_session:
                        query = db_session.query(MetabolightsParameter)
                        db_parameters: MetabolightsParameter = query.order_by(MetabolightsParameter.name.asc()).all()
                        
                        m_params = [ MetabolightsParameterModel.from_orm(db_param).dict() for db_param in db_parameters ]
                        return jsonify({"content": m_params, "message": None, "error": None})
                except Exception as e:
                    raise MetabolightsDBException(message=f"Error while retreiving parameter from database", exception=e)

        except Exception as ex:
            raise ex
    
    @swagger.operation(
        summary="Update Metabolights parameter ",
        notes="""
        Example parameter data input
        <code>
        {
            "name": "test-param",
            "value": "test-data"
        }
        </code>
        """,
        parameters=[
            {
                "name": "name",
                "description": "Metabolights parameter name",
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
        user: UserModel = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            param = MetabolightsParameterModel.parse_obj(data_dict)
        except (Exception) as ex:
            raise MetabolightsException(http_code=404, message="Invalid parameter data input", exception=ex)
        db_session = DBManager.get_instance().session_maker()
        try:
            with db_session:
                query = db_session.query(MetabolightsParameter)
                query_filter = query.filter(MetabolightsParameter.name == param.name)
                db_param = query_filter.first()
                
                if db_param:
                    db_param.value = param.value
                    
                    db_session.add(db_param)
                    db_session.commit()  
                    db_session.refresh(db_param)
                    m_param = MetabolightsParameterModel.from_orm(db_param)
                    return jsonify({"content": m_param.dict(), "message": None, "error": None})
                else:
                    raise MetabolightsException(http_code=400, message="Invalid parameter name")   
        except (Exception) as ex:
            db_session.rollback()
            raise MetabolightsException(http_code=400, message="DB update error", exception=ex)


    
    @swagger.operation(
        summary="Add new Metabolights parameter",
        notes="""
        Example parameter data input
        <code>
        {
            "name": "test-param",
            "value": "test-data"
        }
        </code>
        """,
        parameters=[
            {
                "name": "parameter_data",
                "description": "Metabolights Parameter Name",
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
        param: MetabolightsParameterModel = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            param = MetabolightsParameterModel.parse_obj(data_dict)
                
        except (Exception) as ex:
            raise MetabolightsException(http_code=400, message="Invalid parameter data input", exception=ex)
        db_session = DBManager.get_instance().session_maker()
        try:
            with db_session:
                db_param = MetabolightsParameter()
                db_param.name = param.name
                db_param.value = param.value
            
                db_session.add(db_param)
                db_session.commit()  
                db_session.refresh(db_param)
                m_param = MetabolightsParameterModel.from_orm(db_param)
                return jsonify({"content": m_param.dict(), "message": None, "error": None})
        except (Exception) as ex:
            db_session.rollback()
            raise MetabolightsException(http_code=400, message="DB error", exception=ex)
        
        
    @swagger.operation(
        summary="Delete Metabolights parameter",
        parameters=[
            {
                "name": "name",
                "description": "Metabolights Parameter Name",
                "paramType": "query",
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
    def delete(self):
        
        name = None
        if "name" in request.args:
            name = request.args['name']


        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        
        UserService.get_instance().validate_user_has_curator_role(user_token)
        
        try:
            if name:
                filter_clause = lambda query: query.filter(MetabolightsParameter.name == name)
                db_session = DBManager.get_instance().session_maker()
                try:
                    with db_session:
                        query = db_session.query(MetabolightsParameter)
                        db_param: MetabolightsParameter = filter_clause(query).first()
                        name = db_param.name
                        if db_param:
                            db_session.delete(db_param)
                            db_session.commit()
                        else:
                            raise MetabolightsDBException(http_code=404, message=f"Parameter name is not found.")
                        
                        return jsonify({"content": f"The selected paramter is deleted. Deleted parameter name: {name}", "message": None, "error": None})
                except Exception as e:
                    raise MetabolightsDBException(message=f"Error while retreiving parameter from database", exception=e)
                
                
            else:
                raise MetabolightsException(http_code=400, message=f"Select parameter name.")

        except Exception as ex:
            raise ex