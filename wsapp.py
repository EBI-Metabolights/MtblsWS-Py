import logging.config
import config
from flask import Flask
from flask_restful import Api
from flask_restful_swagger import swagger
from flask_cors import CORS
from app.ws.about import About
from app.ws.mtbls_study import MtblsStudy
from app.ws.isaStudy import Study, StudyTitle, StudyDescription, StudyPubList, StudyNew, StudyProtocols
from app.ws.isaStudy import StudyContacts, StudyFactors, StudyDescriptors, StudyPublications

"""
MTBLS WS-Py

MetaboLights Python-based REST Web Service
"""

app = Flask(__name__, instance_relative_config=True)

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('wslog')


def configure_app(flask_app):
    flask_app.config.from_object(config)
    flask_app.config.from_pyfile('config.py', silent=True)


def initialize_app(flask_app):
    configure_app(flask_app)

    CORS(app, resources={r'/mtbls/ws/*'},
         origins={"http://localhost:4200",
                  "http://localhost:8080",
                  "http://localhost.ebi.ac.uk:8080",
                  "http://wwwdev.ebi.ac.uk"},
         methods={"GET, HEAD, POST, OPTIONS, PUT"}
         )

    api = swagger.docs(Api(app),
                       apiVersion=config.API_VERSION,
                       basePath=config.WS_APP_BASE_LINK,
                       api_spec_url=config.API_DOC,
                       resourcePath=config.RESOURCES_PATH)
    api.add_resource(About, config.RESOURCES_PATH)
    api.add_resource(MtblsStudy, config.RESOURCES_PATH + "/study/<string:study_id>")
    api.add_resource(StudyPubList, config.RESOURCES_PATH + "/study/list")
    api.add_resource(Study, config.RESOURCES_PATH + "/study/<string:study_id>/isa_json")
    api.add_resource(StudyTitle, config.RESOURCES_PATH + "/study/<string:study_id>/title")
    api.add_resource(StudyDescription, config.RESOURCES_PATH + "/study/<string:study_id>/description")
    api.add_resource(StudyNew, config.RESOURCES_PATH + "/study/new")
    api.add_resource(StudyProtocols, config.RESOURCES_PATH + "/study/<string:study_id>/protocols")
    api.add_resource(StudyContacts, config.RESOURCES_PATH + "/study/<string:study_id>/contacts")
    api.add_resource(StudyFactors, config.RESOURCES_PATH + "/study/<string:study_id>/factors")
    api.add_resource(StudyDescriptors, config.RESOURCES_PATH + "/study/<string:study_id>/descriptors")
    api.add_resource(StudyPublications, config.RESOURCES_PATH + "/study/<string:study_id>/publications")


def main():
    initialize_app(app)

    logger.info("Starting server %s v%s", config.WS_APP_NAME, config.WS_APP_VERSION)
    # app.run(host="0.0.0.0", port=config.PORT, debug=config.DEBUG, ssl_context=context)
    app.run(host="0.0.0.0", port=config.PORT, debug=config.DEBUG)
    logger.info("Finished server %s v%s", config.WS_APP_NAME, config.WS_APP_VERSION)


if __name__ == "__main__":
    context = ('ssl/wsapp.crt', 'ssl/wsapp.key')  # SSL certificate and key files
    main()
