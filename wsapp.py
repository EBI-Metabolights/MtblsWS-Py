import config
import logging.config
from flask import Flask
from flask_restful import Api
from flask_cors import CORS
from app.ws.about import About
from app.ws.mtbls_maf import *
from app.ws.mtblsStudy import *
from app.ws.isaStudy import *
from app.ws.isaInvestigation import IsaInvestigation
from app.ws.isaAssay import *
from app.ws.ontology import *
from app.ws.assay_table import *
from app.ws.sample_table import *
from app.ws.tableColumns import *

"""
MTBLS WS-Py

MetaboLights Python-based REST Web Service
"""

application = Flask(__name__, instance_relative_config=True)

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('wslog')


def configure_app(flask_app):
    flask_app.config.from_object(config)
    flask_app.config.from_pyfile('config.py', silent=True)


def initialize_app(flask_app):
    configure_app(flask_app)

    CORS(application, resources={application.config.get('CORS_RESOURCES_PATH')},
         origins={application.config.get('CORS_HOSTS')},
         methods={"GET, HEAD, POST, OPTIONS, PUT, DELETE"}
         )

    res_path = application.config.get('RESOURCES_PATH')
    api = swagger.docs(Api(application),
                       description='MtblsWS-Py : MetaboLights Python-based REST service',
                       apiVersion=application.config.get('API_VERSION'),
                       basePath=application.config.get('WS_APP_BASE_LINK'),
                       api_spec_url=application.config.get('API_DOC'),
                       resourcePath=res_path
                       )

    api.add_resource(About, res_path)

    # MTBLS studies
    api.add_resource(MtblsStudies, res_path + "/studies")
    api.add_resource(StudyFiles, res_path + "/studies/<string:study_id>/isa-tab/study_files")
    api.add_resource(IsaTabInvestigationFile, res_path + "/studies/<string:study_id>/isa-tab/investigation")
    api.add_resource(IsaTabSampleFile, res_path + "/studies/<string:study_id>/isa-tab/sample")
    api.add_resource(IsaTabAssayFile, res_path + "/studies/<string:study_id>/isa-tab/assay")
    api.add_resource(AllocateAccession, res_path + "/studies/create_study")
    api.add_resource(CreateUploadFolder, res_path + "/studies/<string:study_id>/create_upload_folder")
    api.add_resource(CopyFilesFolders, res_path + "/studies/<string:study_id>/copy_from_upload_folder")
    api.add_resource(saveAuditFiles, res_path + "/studies/<string:study_id>/create_audit_files")


    # ISA Investigation
    api.add_resource(IsaInvestigation, res_path + "/studies/<string:study_id>")
    api.add_resource(StudyTitle, res_path + "/studies/<string:study_id>/title")
    api.add_resource(StudyDescription, res_path + "/studies/<string:study_id>/description")
    api.add_resource(StudyContacts, res_path + "/studies/<string:study_id>/contacts"
                     , res_path + "/studies/<string:study_id>/contacts/")
    api.add_resource(StudyProtocols, res_path + "/studies/<string:study_id>/protocols")
    api.add_resource(StudyFactors, res_path + "/studies/<string:study_id>/factors")
    api.add_resource(StudyDescriptors, res_path + "/studies/<string:study_id>/descriptors")
    api.add_resource(StudyPublications, res_path + "/studies/<string:study_id>/publications")
    api.add_resource(StudyReleaseDateAndStatus, res_path + "/studies/<string:study_id>/releaseDateAndStatus")

    #Metabolite Annotation File (MAF)
    api.add_resource(MtblsMAFSearch, res_path + "/maf/search/<string:search_type>")
    api.add_resource(ReadMetaboliteAnnotationFile, res_path + "/studies/<string:study_id>/maf/<string:annotation_file_name>")
    api.add_resource(MetaboliteAnnotationFile, res_path + "/studies/<string:study_id>/maf/validated/<string:annotation_file_name>")
    # Study
    api.add_resource(StudySources, res_path + "/studies/<string:study_id>/sources")
    api.add_resource(StudySamples, res_path + "/studies/<string:study_id>/samples")
    api.add_resource(EditSampleFile, res_path + "/studies/<string:study_id>/samples/<string:sample_file_name>")
    api.add_resource(StudyOtherMaterials, res_path + "/studies/<string:study_id>/otherMaterials")
    api.add_resource(StudyProcesses, res_path + "/studies/<string:study_id>/processSequence")

    # Assay
    api.add_resource(StudyAssay, res_path + "/studies/<string:study_id>/assays")
    api.add_resource(AssaySamples, res_path + "/studies/<string:study_id>/assays/samples")
    api.add_resource(AssayOtherMaterials, res_path + "/studies/<string:study_id>/assays/otherMaterials")
    api.add_resource(AssayDataFiles, res_path + "/studies/<string:study_id>/assays/dataFiles")
    api.add_resource(AssayProcesses, res_path + "/studies/<string:study_id>/assays/processSequence")
    api.add_resource(AssayTable, res_path + "/studies/<string:study_id>/assay/tableCell")
    api.add_resource(EditAssayFile, res_path + "/studies/<string:study_id>/assay/<string:assay_file_name>")

    # Manipulating table columns
    api.add_resource(SimpleColumns, res_path + "/studies/<string:study_id>/addColumn/<string:file_name>")
    api.add_resource(ComplexColumns, res_path + "/studies/<string:study_id>/addColumns/<string:file_name>")
    api.add_resource(ColumnsRows, res_path + "/studies/<string:study_id>/updateCell/<string:file_name>")

    # Add ontology resources
    api.add_resource(Ontology, res_path + "/studies/ontology")


def main():
    print("Initialising application")
    initialize_app(application)
    logger.info("Starting server %s v%s", application.config.get('WS_APP_NAME'), application.config.get('WS_APP_VERSION'))
    # application.run(host="0.0.0.0", port=config.PORT, debug=config.DEBUG, ssl_context=context)
    print("Starting application")
    application.run(host="0.0.0.0", port=application.config.get('PORT'), debug=application.config.get('DEBUG'))
    logger.info("Finished server %s v%s", application.config.get('WS_APP_NAME'), application.config.get('WS_APP_VERSION'))


print ("before main stanza")
if __name__ == "__main__":
    print("Setting ssl context for Flask server")
    context = ('ssl/wsapp.crt', 'ssl/wsapp.key')  # SSL certificate and key files
    main()
else:
    print("Setting ssl context for Gunicorn server")
    context = ('ssl/wsapp.crt', 'ssl/wsapp.key')  # SSL certificate and key files
    main()
