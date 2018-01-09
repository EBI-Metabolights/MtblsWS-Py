import logging.config
import config
from flask import Flask
from flask_restful import Api
from flask_cors import CORS
from app.ws.about import About
from app.ws.mtbls_study import MtblsStudy
from app.ws.mtbls_maf import MtblsMAF
from app.ws.isaStudy import *

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
         origins={"http://localhost:8000",
                  "http://localhost:4200",
                  "http://localhost:8080",
                  "http://localhost.ebi.ac.uk:8080",
                  "http://wwwdev.ebi.ac.uk"},
         methods={"GET, HEAD, POST, OPTIONS, PUT"}
         )

    res_path = app.config.get('RESOURCES_PATH')
    api = swagger.docs(Api(app),
                       apiVersion=app.config.get('API_VERSION'),
                       basePath=app.config.get('WS_APP_BASE_LINK'),
                       api_spec_url=app.config.get('API_DOC'),
                       resourcePath=res_path)
    api.add_resource(About, res_path)
    api.add_resource(MtblsStudy, res_path + "/study/<string:study_id>")
    api.add_resource(MtblsMAF, res_path + "/study/<string:study_id>/assay/<string:assay_id>/maf")
    api.add_resource(StudyPubList, res_path + "/study/list")
    api.add_resource(Study, res_path + "/study/<string:study_id>/isa_json")
    api.add_resource(StudyTitle, res_path + "/study/<string:study_id>/title")
    api.add_resource(StudyDescription, res_path + "/study/<string:study_id>/description")
    api.add_resource(StudyNew, res_path + "/study/new")
    api.add_resource(StudyProtocols, res_path + "/study/<string:study_id>/protocols")
    api.add_resource(StudyContacts, res_path + "/study/<string:study_id>/contacts")
    api.add_resource(StudyFactors, res_path + "/study/<string:study_id>/factors")
    api.add_resource(StudyDescriptors, res_path + "/study/<string:study_id>/descriptors")
    api.add_resource(StudyPublications, res_path + "/study/<string:study_id>/publications")
    api.add_resource(StudyMaterials, res_path + "/study/<string:study_id>/materials")

    api.add_resource(StudySources, res_path + "/study/<string:study_id>/sources")
    api.add_resource(StudySource, res_path + "/study/<string:study_id>/sources/<string:source_name>")
    api.add_resource(StudySamples, res_path + "/study/<string:study_id>/samples")
    api.add_resource(StudySample, res_path + "/study/<string:study_id>/samples/<string:sample_name>")



def main():
    initialize_app(app)

    logger.info("Starting server %s v%s", app.config.get('WS_APP_NAME'), app.config.get('WS_APP_VERSION'))
    # app.run(host="0.0.0.0", port=config.PORT, debug=config.DEBUG, ssl_context=context)
    app.run(host="0.0.0.0", port=app.config.get('PORT'), debug=app.config.get('DEBUG'))
    logger.info("Finished server %s v%s", app.config.get('WS_APP_NAME'), app.config.get('WS_APP_VERSION'))


if __name__ == "__main__":
    context = ('ssl/wsapp.crt', 'ssl/wsapp.key')  # SSL certificate and key files
    main()
