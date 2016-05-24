from flask import Flask, request, abort
from flask_restful import Api, Resource
from flask_restful_swagger import swagger
import config
from mtblsWSclient import WsClient

"""
MetaboLights WS-Py

MTBLS Python-based REST Web Service

author: jrmacias
date: 20160520
"""


wsc = WsClient()


class About(Resource):
    """Basic description of the Web Service"""
    @swagger.operation(
        summary='About this WS',
        notes='Basic description of the Web Service',
        nickname='about',
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            }
        ]
    )
    def get(self):
        return {'WS name': config.APP_NAME,
                'API': {
                    'version': config.APP_VERSION,
                    'documentation': config.APP_BASE_LINK + config.API_DOC + '.html',
                    'specification': config.APP_BASE_LINK + config.API_DOC + '.json',
                },
                'URL': config.APP_BASE_LINK + config.RESOURCES_PATH,
                }


class GetStudyLocation(Resource):
    def get(self, study_id):

        # param validation
        if study_id is None:
            abort(404)

        # User authentication
        user_token = request.headers['user_token']
        if user_token is None:
            abort(401)

        # get study info from MetaboLights WS
        return wsc.get_study_location(study_id, user_token)


app = Flask(__name__)
app.config.from_object(config)

api = swagger.docs(Api(app),
                   apiVersion=config.APP_VERSION,
                   basePath=config.APP_BASE_LINK,
                   api_spec_url=config.API_DOC,
                   resourcePath=config.RESOURCES_PATH)

api.add_resource(About, config.RESOURCES_PATH)
api.add_resource(GetStudyLocation, config.RESOURCES_PATH + '/study/<study_id>/location')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=config.PORT, debug=config.DEBUG)
