
from flask import current_app as app, jsonify, request
from flask_restful import Resource
from flask_restful_swagger import swagger
from app.utils import metabolights_exception_handler
from app.ws.elasticsearch.elastic_service import ElasticsearchService
from app.ws.elasticsearch.schemes import SearchQuery
from app.ws.study.user_service import UserService
from app.ws.utils import log_request
    
class ElasticSearchQuery(Resource):
    @swagger.operation(
        summary="Elasticsearch query",
        parameters=[
            {
                "name": "query",
                "description": "Search query body",
                "paramType": "body",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "user-token",
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
    def post(self):
                
        log_request(request)
        content = None
        try:
            content = request.json
        except:
            content = {}

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        is_curator = False
        try:
            user = UserService.get_instance().validate_user_has_curator_role(user_token)
            is_curator = True
            user_name = user['username']
        except:
            is_curator = False
            user_name = "metabolights-anonymous"
        
        if content:
            query: SearchQuery = SearchQuery.model_validate(content)
        else:
            query = SearchQuery()
            
        if is_curator:
            query.searchUser.id = user_name
            query.searchUser.isAdmin = True
        else:
            query.searchUser.id = user_name
            query.searchUser.isAdmin = False

        result = ElasticsearchService.get_instance().search(query)
        
        return jsonify(result)