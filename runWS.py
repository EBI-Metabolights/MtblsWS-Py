import logging
import config
from flask import Flask
from flask_restful import Api, Resource
from flask_restful_swagger import swagger
from flask_cors import CORS
from app.ws.mtbls_study import MtblsStudy
from app.ws.isaStudy import Study, StudyTitle, StudyDescription

"""
MetaboLights WS-Py

MTBLS Python-based REST Web Service

author: jrmacias@ebi.ac.uk
date: 20160520
"""


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
        return {"WS name": config.WS_APP_NAME,
                "WS description": config.WS_APP_DESCRIPTION,
                "API": {
                    "version": config.API_VERSION,
                    "documentation": config.WS_APP_BASE_LINK + config.API_DOC + ".html",
                    "specification": config.WS_APP_BASE_LINK + config.API_DOC + ".json",
                },
                "URL": config.WS_APP_BASE_LINK + config.RESOURCES_PATH,
                }


app = Flask(__name__, instance_relative_config=True)
app.config.from_object(config)
app.config.from_pyfile('config.py')

# basic Logging
logging.basicConfig(filename=config.WS_APP_NAME + ".log",
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    level=logging.DEBUG)
logging.info("%s v%s Started!", config.WS_APP_NAME, config.WS_APP_VERSION)

CORS(app, resources={r'/mtbls/ws/study/*': {"origins": "http://localhost:4200"}})

api = swagger.docs(Api(app),
                   apiVersion=config.API_VERSION,
                   basePath=config.WS_APP_BASE_LINK,
                   api_spec_url=config.API_DOC,
                   resourcePath=config.RESOURCES_PATH)

api.add_resource(About, config.RESOURCES_PATH)
api.add_resource(MtblsStudy, config.RESOURCES_PATH + "/study/<study_id>")
api.add_resource(Study, config.RESOURCES_PATH + "/study/<study_id>/isa_json")
api.add_resource(StudyTitle, config.RESOURCES_PATH + "/study/<study_id>/title")
api.add_resource(StudyDescription, config.RESOURCES_PATH + "/study/<study_id>/description")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=config.PORT, debug=config.DEBUG)
