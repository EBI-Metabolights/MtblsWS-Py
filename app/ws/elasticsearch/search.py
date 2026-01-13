from flask import jsonify, request
from flask_restful import Resource
from flask_restful_swagger import swagger

from app.utils import metabolights_exception_handler
from app.ws.auth.permissions import public_endpoint, validate_user_has_curator_role
from app.ws.db.types import UserRole
from app.ws.elasticsearch.elastic_service import ElasticsearchService
from app.ws.elasticsearch.schemes import SearchQuery
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
                "allowMultiple": False,
            },
            {
                "name": "user-token",
                "description": "User API token",
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
        log_request(request)
        public_endpoint(request)
        content = None
        try:
            content = request.json
        except:
            content = {}
        result = validate_user_has_curator_role(request, fail_silently=True)
        is_curator = result.context.user_role in {
            UserRole.ROLE_SUPER_USER,
            UserRole.SYSTEM_ADMIN,
        }
        user_name = result.context.username or "metabolights-anonymous"

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
