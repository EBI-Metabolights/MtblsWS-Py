import config
import logging
from flask_restful import Resource, fields, marshal_with
from flask_restful_swagger import swagger

"""
MtblsWS-Py About

Basic description of the Web Service
"""

logger = logging.getLogger('wslog')

app_fields = {
    'WsName': fields.String,
    'WsVersion': fields.String,
    'WsDescription': fields.String,
    'WsURL': fields.String,
}

api_fields = {
    'ApiVersion': fields.String,
    'ApiDocumentation': fields.String,
    'ApiSpecification': fields.String,
}

about_fields = {
    'WsApp': fields.Nested(app_fields),
    'WsApi': fields.Nested(api_fields),
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
    @marshal_with(about_fields, envelope='AboutWS')
    def get(self):

        from flask import current_app as app

        """Get a basic description of the Web Service"""
        logger.info('Getting WS-about onto_information')
        api = {"ApiVersion": app.config.get('API_VERSION'),
               "ApiDocumentation": app.config.get('WS_APP_BASE_LINK') + app.config.get('API_DOC') + ".html",
               "ApiSpecification": app.config.get('WS_APP_BASE_LINK') + app.config.get('API_DOC') + ".json"
               }
        appl = {"WsName": app.config.get('WS_APP_NAME'),
                "WsVersion": app.config.get('WS_APP_VERSION'),
                "WsDescription": app.config.get('WS_APP_DESCRIPTION'),
                "WsURL": app.config.get('WS_APP_BASE_LINK') + app.config.get('RESOURCES_PATH')
                }
        about = {'WsApp': appl, 'WsApi': api}
        return about
