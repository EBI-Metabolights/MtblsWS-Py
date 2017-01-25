from flask import Flask, request, abort, jsonify
from flask_restful import Api, Resource
from flask_restful_swagger import swagger
import config
from isaApiClient import IsaApiClient
from mtblsWSclient import WsClient

"""
MetaboLights WS-Py

MTBLS Python-based REST Web Service

author: jrmacias
date: 20160520
"""


wsc = WsClient()
iac = IsaApiClient()


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


class GetStudy(Resource):
    """Get the Study in different formats"""
    @swagger.operation(
        summary='Get MTBLS Study',
        notes='Get the current MTBLS Study in JSON format.',
        parameters=[
            {
                "name": "study_id",
                "in": "path",
                "description": "Identifier of the study",
                "required": True,
                "allowMultiple": False,
                "paramType": "MetaboLights identifier",
                "dataType": "String"
            },
            {
                "name": "user_token",
                "in": "header",
                "description": "User API token, mandatory. Used to check for permissions.",
                "paramType": "API key",
                "type": "String",
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
        if 'user_token' not in request.headers:
            abort(401)
        else:
            user_token = request.headers['user_token']

        # get study from MetaboLights WS
        return wsc.get_study(study_id, user_token)


class StudyTitle(Resource):
    """Manage the Study title"""
    @swagger.operation(
        summary='Get MTBLS Study title',
        notes='Get the current MTBLS Study title in JSON format.',
        parameters=[
            {
                "name": "study_id",
                "in": "path",
                "description": "Identifier of the study",
                "required": True,
                "allowMultiple": False,
                "paramType": "MetaboLights identifier",
                "dataType": "String"
            },
            {
                "name": "user_token",
                "in": "header",
                "description": "User API token, mandatory. Used to check for permissions.",
                "paramType": "API key",
                "type": "String",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The Study title is returned, JSON format."
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
        if 'user_token' not in request.headers:
            abort(401)
        else:
            user_token = request.headers['user_token']

        title = iac.get_study_title(study_id, user_token)

        return jsonify({'Study-title': title})


app = Flask(__name__)
app.config.from_object(config)

api = swagger.docs(Api(app),
                   apiVersion=config.APP_VERSION,
                   basePath=config.APP_BASE_LINK,
                   api_spec_url=config.API_DOC,
                   resourcePath=config.RESOURCES_PATH)

api.add_resource(About, config.RESOURCES_PATH)
api.add_resource(GetStudy, config.RESOURCES_PATH + '/study/<study_id>')
api.add_resource(StudyTitle, config.RESOURCES_PATH + '/study/<study_id>/title')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=config.PORT, debug=config.DEBUG)
