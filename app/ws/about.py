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
    'Name': fields.String,
    'Version': fields.String,
    'Description': fields.String,
    'URL': fields.String,
}

api_fields = {
    'Version': fields.String,
    'Documentation': fields.String,
    'Specification': fields.String,
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
        api = {"Version": config.API_VERSION,
               "Documentation": config.WS_APP_BASE_LINK + config.API_DOC + ".html",
               "Specification": config.WS_APP_BASE_LINK + config.API_DOC + ".json"
               }
        app = {"Name": config.WS_APP_NAME,
               "Version": config.WS_APP_VERSION,
               "Description": config.WS_APP_DESCRIPTION,
               "URL": config.WS_APP_BASE_LINK + config.RESOURCES_PATH,
               }
        about = {'WS-App': app, 'API': api}
        return about
