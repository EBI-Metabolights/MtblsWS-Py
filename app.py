import json
import config
from flask import Flask, request, abort, jsonify
from flask_restful import Api, Resource
from flask_restful_swagger import swagger
from flask_cors import CORS, cross_origin
from app.ws.isaApiClient import IsaApiClient, WsClient

"""
MetaboLights WS-Py

MTBLS Python-based REST Web Service

author: jrmacias@ebi.ac.uk
date: 20160520
"""


wsc = WsClient()
iac = IsaApiClient()


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
        return {"WS name": config.APP_NAME,
                "API": {
                    "version": config.APP_VERSION,
                    "documentation": config.APP_BASE_LINK + config.API_DOC + ".html",
                    "specification": config.APP_BASE_LINK + config.API_DOC + ".json",
                },
                "URL": config.APP_BASE_LINK + config.RESOURCES_PATH,
                }


class GetStudy(Resource):
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
        else:
            user_token = request.headers["user_token"]

        # get study from MetaboLights WS
        return wsc.get_study(study_id, user_token)


class Study(Resource):
    @swagger.operation(
        summary="Get ISA object from MTBLS Study",
        notes="Get the MTBLS Study with {study_id} as ISA object.",
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
                "message": "OK. The Study title is returned, JSON format."
            },
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax."
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
        else:
            user_token = request.headers["user_token"]

        isaObj = iac.get_isa_json(study_id, user_token)

        return jsonify({"ISA-Tab_Investigation": isaObj})


class StudyTitle(Resource):
    """Manage the Study title"""
    @swagger.operation(
        summary="Get MTBLS Study title",
        notes="Get the title of the MTBLS Study with {study_id} in JSON format.",
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
            },
            {
                "name": "method",
                "in": "path",
                "description": "Use 'direct' to access i_*.txt without calling ISA-API.",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["ISA-API", "direct"],
                "default": "ISA-API"
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The Study title is returned, JSON format."
            },
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax."
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
        else:
            user_token = request.headers["user_token"]

        args = request.args

        if "direct" in args['method']:
            title = iac.get_study_title_noISATools(study_id, user_token)
        else:
            title = iac.get_study_title(study_id, user_token)

        return jsonify({"Study-title": title})

    @swagger.operation(
        summary='Update MTBLS Study title',
        notes="""Update the title of the MTBLS Study with {study_id}.
              Only the new title (in JSON format) must be provided in the body of the request.
              i.e.: { "title": "New Study title..." }""",
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
            },
            {
                "name": "title",
                "description": """New title in JSON format.</br>
                 i.e.: { "title": "New Study title..." }""",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax."
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
    def put(self, study_id):
        # param validation
        if study_id is None:
            abort(404)

        # body content validation
        if request.data is None:
            abort(400)
        else:
            dataDict = request.get_json(force=True)
            new_title = dataDict['title']

        # User authentication
        if 'user_token' not in request.headers:
            abort(401)
        else:
            user_token = request.headers['user_token']

        # update study title
        # return iac.write_study_json_title(study_id, user_token, new_title)
        return iac.write_study_title(study_id, user_token, new_title)


class StudyDescription(Resource):
    """Manage the Study title"""
    @swagger.operation(
        summary="Get MTBLS Study description",
        notes="Get the description of the MTBLS Study with {study_id} in JSON format.",
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
            },
            {
                "name": "method",
                "description": "Use 'direct' to access i_*.txt without calling ISA-API.",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["ISA-API", "direct"],
                "default": "ISA-API"
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The Study description is returned, JSON format."
            },
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax."
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
        else:
            user_token = request.headers["user_token"]

        args = request.args

        if "direct" in args['method']:
            description = iac.get_study_description_noISATools(study_id, user_token)
        else:
            description = iac.get_study_description(study_id, user_token)

        return jsonify({"Study-description": description})

    @swagger.operation(
        summary='Update MTBLS Study description',
        notes="""Update the description of the MTBLS Study with {study_id}.
                  Only the new description (in JSON format) must be provided in the body of the request.
                  i.e.: { "description": "New Study description..." }""",
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
            },
            {
                "name": "description",
                "description": """New description in JSON format.</br>
                 i.e.: { "description": "New Study description..." }""",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The Study description is returned, JSON format."
            },
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax."
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
    def put(self, study_id):
        # param validation
        if study_id is None:
            abort(404)

        # body content validation
        if request.data is None:
            abort(400)
        else:
            dataDict = json.loads(request.data.decode('utf-8'))
            new_description = dataDict['description']

        # User authentication
        if 'user_token' not in request.headers:
            abort(401)
        else:
            user_token = request.headers['user_token']

        # update study description
        return iac.write_study_description(study_id, user_token, new_description)

app = Flask(__name__)
CORS(app, resources={r'/mtbls/ws/v1/study/*': {"origins": "http://localhost:4200"}})
app.config.from_object(config)

api = swagger.docs(Api(app),
                   apiVersion=config.APP_VERSION,
                   basePath=config.APP_BASE_LINK,
                   api_spec_url=config.API_DOC,
                   resourcePath=config.RESOURCES_PATH)

api.add_resource(About, config.RESOURCES_PATH)
api.add_resource(GetStudy, config.RESOURCES_PATH + "/study/<study_id>")
api.add_resource(Study, config.RESOURCES_PATH + "/study/<study_id>/isa_json")
api.add_resource(StudyTitle, config.RESOURCES_PATH + "/study/<study_id>/title")
api.add_resource(StudyDescription, config.RESOURCES_PATH + "/study/<study_id>/description")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=config.PORT, debug=config.DEBUG)
