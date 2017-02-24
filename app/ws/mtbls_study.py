from flask import request, abort
from flask_restful import Resource
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient

"""
MTBLS Study

Manage MTBLS studies obtained from Java-based MTBLS WS

author: jrmacias@ebi.ac.uk
date: 2017-02-23
"""

# MetaboLights (Java-Based) WebService client
wsc = WsClient()


class MtblsStudy(Resource):
    """Get the Study in different formats"""
    @swagger.operation(
        summary="Get MTBLS Study",
        notes="Get the MTBLS Study with {study_id} in JSON format.",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "user_token",
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
                "message": "OK. The Study is returned, JSON format."
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
    def get(self, study_id):
        # param validation
        if study_id is None:
            abort(404)

        # User authentication
        if "user_token" not in request.headers:
            abort(401)
        user_token = request.headers["user_token"]

        # get study from MetaboLights WS
        return wsc.get_study(study_id, user_token)
