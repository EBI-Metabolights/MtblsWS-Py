import logging
from flask import request, abort
from flask_restful import Resource
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient

"""
MTBLS MAF

Manage the metabolite annotation files (MAF) from a MTBLS studies.  MAF JSON obtained from the Java-based MTBLS WS
"""

logger = logging.getLogger('wslog')
# MetaboLights (Java-Based) WebService client
wsc = WsClient()


class MtblsMAF(Resource):
    """Get MAF from studies (assays)"""
    @swagger.operation(
        summary="Get MAF for MTBLS Assay",
        nickname="Get MAF for MTBLS Assay",
        notes="Get a given MAF associated with assay {assay_id} for a MTBLS Study with {study_id} in JSON format.",
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
                "name": "assay_id",
                "description": "MTBLS Assay Identifier",
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
                "message": "OK. The MAF is returned, JSON format."
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
    def get(self, study_id, assay_id):
        """
        Get MAF from from MetaboLights WS
        :param study_id: MTBLS study identifier
        :param assay_id: The number of the assay for the given study_id
        :return: a JSON representation of the MTBLS Study object
        """

        # param validation
        if study_id is None:
            abort(404)

        if assay_id is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info('Getting MAF from Assay %s from MTBLS Study %s, using API-Key %s', assay_id, study_id, user_token)
        maf = wsc.get_study_maf(study_id, assay_id, user_token)
        return maf
