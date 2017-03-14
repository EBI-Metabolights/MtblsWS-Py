import config
import logging
from flask_restful import Resource, fields, marshal_with
from flask_restful_swagger import swagger

"""
MtblsWS-Py About

Basic description of the Web Service

author: jrmacias@ebi.ac.uk
date: 2017-03-06
"""

logger = logging.getLogger('wslog')


class About(Resource):
    """Basic description of the Web Service"""
    @swagger.operation(
        summary="About this Web Service",
        notes="Basic description of the Web Service",
        nickname="about",
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            }
        ]
    )
    def get(self):
        """Get a basic description of the Web Service"""
        logger.info('Getting WS-about information')
        return {"WS-Name": config.WS_APP_NAME,
                "WS-Description": config.WS_APP_DESCRIPTION,
                "WS-API": {
                    "Version": config.API_VERSION,
                    "Documentation": config.WS_APP_BASE_LINK + config.API_DOC + ".html",
                    "Specification": config.WS_APP_BASE_LINK + config.API_DOC + ".json",
                },
                "WS-URL": config.WS_APP_BASE_LINK + config.RESOURCES_PATH,
                }
