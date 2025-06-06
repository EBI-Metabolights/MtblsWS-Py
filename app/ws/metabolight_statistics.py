from typing import List

from flask import current_app as app, jsonify, request
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger

from app.utils import metabolights_exception_handler
from app.ws.db.dbmanager import DBManager
from app.ws.db.models import MetabolightsStatisticsModel
from app.ws.db.schemes import MlStat
from app.ws.study.user_service import UserService

class MetabolightsStatistics(Resource):
    @swagger.operation(
        summary="Get Metabolights statistics",
        parameters=[
            {
                "name": "category_name",
                "description": "Metabolights Statistics",
                "paramType": "query",
                "type": "string",
                "required": False,
                "allowMultiple": False
            },
            {
                "name": "user-token",
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
        category_name = None
        if request.args:
            
            category_name = request.args.get('category_name')
            

        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        
        UserService.get_instance().validate_user_has_curator_role(user_token)
        db_session = DBManager.get_instance().session_maker()
        try:
            with db_session:
                query = db_session.query(MlStat)
                db_parameters: List[MlStat] = []
                if category_name:
                    filter_clause = lambda query: query.filter(MlStat.page_section == category_name)
                    db_parameters: List[MlStat] = filter_clause(query).order_by(MlStat.sort_order.asc(), MlStat.str_name.asc()).all() 
                else:
                    db_parameters: List[MlStat] = query.order_by(MlStat.page_section.asc(), MlStat.sort_order.asc(), MlStat.str_name.asc()).all() 
                m_params = [ MetabolightsStatisticsModel.model_validate(db_param).model_dump() for db_param in db_parameters ]
                return jsonify({"content": m_params, "message": None, "error": None})                    
        except Exception as ex:
            raise ex
    