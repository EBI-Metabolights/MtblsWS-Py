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

app_fields = {
    'WS-Name': fields.String,
    'WS-Version': fields.String,
    'WS-Description': fields.String,
    'WS-URL': fields.String,
}

api_fields = {
    'API-Version': fields.String,
    'API-Documentation': fields.String,
    'API-Specification': fields.String,
}

about_fields = {
    'WS-App': fields.Nested(app_fields),
    'API': fields.Nested(api_fields),
}


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
    @marshal_with(about_fields, envelope='About-WS')
    def get(self):
        """Get a basic description of the Web Service"""
        logger.info('Getting WS-about information')
        api = {"API-Version": config.API_VERSION,
               "API-Documentation": config.WS_APP_BASE_LINK + config.API_DOC + ".html",
               "API-Specification": config.WS_APP_BASE_LINK + config.API_DOC + ".json"
               }
        app = {"WS-Name": config.WS_APP_NAME,
               "WS-Version": config.WS_APP_VERSION,
               "WS-Description": config.WS_APP_DESCRIPTION,
               "WS-URL": config.WS_APP_BASE_LINK + config.RESOURCES_PATH,
               }
        about = {'WS-App': app, 'API': api}
        return about
