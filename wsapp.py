import logging.config
import config
from flask import Flask
from flask_restful import Api
from flask_cors import CORS
from app.ws.about import About
from app.ws.mtbls_maf import MtblsMAF
from app.ws.mtbls_study import MtblsStudy
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
         origins={config.CORS_HOSTS},
         methods={"GET, HEAD, POST, OPTIONS, PUT, DELETE"}
         )

    res_path = app.config.get('RESOURCES_PATH')
    api = swagger.docs(Api(app),
                       description='MtblsWS-Py : MetaboLights Python-based REST service',
                       apiVersion=app.config.get('API_VERSION'),
                       basePath=app.config.get('WS_APP_BASE_LINK'),
                       api_spec_url=app.config.get('API_DOC'),
                       resourcePath=res_path
                       )

    api.add_resource(About, res_path)

    api.add_resource(IsaJsonStudies, res_path + "/studies")
    api.add_resource(MtblsStudy, res_path + "/mtbls_studies/<string:study_id>")
    api.add_resource(IsaJsonStudy, res_path + "/studies/<string:study_id>")
    api.add_resource(StudyTitle, res_path + "/studies/<string:study_id>/title")
    api.add_resource(StudyDescription, res_path + "/studies/<string:study_id>/description")
    api.add_resource(StudyContacts, res_path + "/studies/<string:study_id>/contacts"
                     , res_path + "/studies/<string:study_id>/contacts/")
    api.add_resource(StudyProtocols, res_path + "/studies/<string:study_id>/protocols")
    api.add_resource(StudyFactors, res_path + "/studies/<string:study_id>/factors")
    api.add_resource(StudyDescriptors, res_path + "/studies/<string:study_id>/descriptors")
    api.add_resource(StudyPublications, res_path + "/studies/<string:study_id>/publications")
    # some methods not yet implemented
    api.add_resource(StudyProcesses, res_path + "/studies/<string:study_id>/processes")
    api.add_resource(StudySources, res_path + "/studies/<string:study_id>/sources")
    api.add_resource(StudySamples, res_path + "/studies/<string:study_id>/samples")
    api.add_resource(StudyOtherMaterials, res_path + "/studies/<string:study_id>/othermaterials")

    api.add_resource(MtblsMAF, res_path + "/study/<string:study_id>/assay/<string:assay_id>/maf")

    api.add_resource(StudyAssays, res_path + "/studies/<string:study_id>/assays")


def main():
    initialize_app(app)

    logger.info("Starting server %s v%s", app.config.get('WS_APP_NAME'), app.config.get('WS_APP_VERSION'))
    # app.run(host="0.0.0.0", port=config.PORT, debug=config.DEBUG, ssl_context=context)
    app.run(host="0.0.0.0", port=app.config.get('PORT'), debug=app.config.get('DEBUG'))
    logger.info("Finished server %s v%s", app.config.get('WS_APP_NAME'), app.config.get('WS_APP_VERSION'))


if __name__ == "__main__":
    context = ('ssl/wsapp.crt', 'ssl/wsapp.key')  # SSL certificate and key files
    main()
