import logging
from flask import request, abort
from flask_restful import Resource
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient

"""
MTBLS Study

Manage MTBLS studies obtained from Java-based MTBLS WS
"""

logger = logging.getLogger('wslog')
# MetaboLights (Java-Based) WebService client
wsc = WsClient()


class MtblsStudy(Resource):
    def get(self, study_id):
        """
        Get study from MetaboLights WS
        :param study_id: MTBLS study identifier
        :return: a JSON representation of the MTBLS Study object
        """

        # param validation
        if study_id is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info('Getting MTBLS Study %s, using API-Key %s', study_id, user_token)
        study = wsc.get_study(study_id, user_token)
        return study
