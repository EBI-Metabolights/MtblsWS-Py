import logging
import os
from flask import request, abort, send_file
from flask_restful import Resource
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient


logger = logging.getLogger('wslog')
wsc = WsClient()


class IsaTabInvestigation(Resource):

    @swagger.operation(
        summary="Get ISA-Tab Investigation file",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier",
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
                "required": False,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self, study_id):
        # log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info('Getting ISA-Tab Investigation file for MTBLS Study %s, using API-Key %s', study_id, user_token)
        location = wsc.get_study_location(study_id, user_token)
        file = os.path.join(location, "i_Investigation.txt")
        return send_file(file)
