#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-May-23
#  Modified by:   kenneth
#
#  Copyright 2019 EMBL - European Bioinformatics Institute
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

import logging.config

from flask import Flask
from flask_cors import CORS
from flask_restful import Api

import config
from app.ws.MapStudies import *
from app.ws.about import About
from app.ws.assay_protocol import *
from app.ws.assay_table import *
from app.ws.biostudies import *
from app.ws.chebi_workflow import SplitMaf, ChEBIPipeLine, ChEBIPipeLineLoad
from app.ws.cluster_jobs import LsfUtils
from app.ws.compare_files import CompareTsvFiles
from app.ws.cronjob import *
from app.ws.enzyme_portal_helper import EnzymePortalHelper
from app.ws.google_calendar import GoogleCalendar
from app.ws.isaAssay import *
from app.ws.isaInvestigation import IsaInvestigation
from app.ws.isaStudy import *
from app.ws.jira_update import Jira
from app.ws.metaspace_pipeline import MetaspacePipeLine
from app.ws.mtblsStudy import *
from app.ws.mtbls_maf import *
from app.ws.mzML2ISA import *
from app.ws.ontology import *
from app.ws.organism import Organism
from app.ws.partner_utils import Metabolon
from app.ws.reports import reports
from app.ws.sample_table import *
from app.ws.send_files import SendFiles
from app.ws.spectra import ExtractMSSpectra
from app.ws.stats import StudyStats
from app.ws.study_actions import StudyStatus
from app.ws.study_files import StudyFiles, StudyFilesTree, SampleStudyFiles, UnzipFiles, CopyFilesFolders
from app.ws.table_editor import *
from app.ws.user_management import UserManagement
from app.ws.validation import Validation, OverrideValidation, UpdateValidationFile
from app.ws.pathway import keggid

"""
MTBLS WS-Py

MetaboLights Python-based REST Web Service
"""

application = Flask(__name__, instance_relative_config=True)
hostname = os.uname().nodename
logging.config.fileConfig('logging_' + hostname + '.conf')
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
                       description='MetaboLights RESTful WebService',
                       apiVersion=application.config.get('API_VERSION'),
                       basePath=application.config.get('WS_APP_BASE_LINK'),
                       api_spec_url=application.config.get('API_DOC'),
                       resourcePath=res_path
                       )

    api.add_resource(About, res_path)
    api.add_resource(MtblsMAFSearch, res_path + "/search/<string:query_type>")

    # MTBLS studies
    api.add_resource(MtblsStudies, res_path + "/studies")
    api.add_resource(MtblsStudiesWithMethods, res_path + "/studies/technology")
    api.add_resource(MyMtblsStudiesDetailed, res_path + "/studies/user")
    api.add_resource(MyMtblsStudies, res_path + "/studies/user/lite")
    api.add_resource(StudyFiles, res_path + "/studies/<string:study_id>/files")
    api.add_resource(StudyFilesTree, res_path + "/studies/<string:study_id>/files/tree")
    api.add_resource(SampleStudyFiles, res_path + "/studies/<string:study_id>/files/samples")
    api.add_resource(SendFiles,
                     res_path + "/studies/<string:study_id>/download",
                     res_path + "/studies/<string:study_id>/download/<string:obfuscation_code>")
    api.add_resource(UnzipFiles, res_path + "/studies/<string:study_id>/files/unzip")
    api.add_resource(IsaTabInvestigationFile, res_path + "/studies/<string:study_id>/investigation")
    api.add_resource(IsaTabSampleFile, res_path + "/studies/<string:study_id>/sample")
    api.add_resource(IsaTabAssayFile, res_path + "/studies/<string:study_id>/assay")
    api.add_resource(StudyAssay, res_path + "/studies/<string:study_id>/assays")
    api.add_resource(StudyAssayDelete, res_path + "/studies/<string:study_id>/assays/<string:assay_file_name>")
    api.add_resource(CreateAccession, res_path + "/studies/create")
    api.add_resource(CloneAccession, res_path + "/studies/clone")
    api.add_resource(DeleteStudy, res_path + "/studies/<string:study_id>/delete")
    api.add_resource(CreateUploadFolder, res_path + "/studies/<string:study_id>/upload")
    api.add_resource(StudyStatus, res_path + "/studies/<string:study_id>/status")
    api.add_resource(CopyFilesFolders, res_path + "/studies/<string:study_id>/sync")
    api.add_resource(AuditFiles, res_path + "/studies/<string:study_id>/audit")
    api.add_resource(StudyMetaInfo, res_path + "/studies/<string:study_id>/meta-info")

    # ISA Investigation
    api.add_resource(IsaInvestigation, res_path + "/studies/<string:study_id>")
    api.add_resource(StudyTitle, res_path + "/studies/<string:study_id>/title")
    api.add_resource(StudyReleaseDate, res_path + "/studies/<string:study_id>/release-date")
    api.add_resource(StudyDescription, res_path + "/studies/<string:study_id>/description")
    api.add_resource(StudyContacts, res_path + "/studies/<string:study_id>/contacts")
    api.add_resource(StudySubmitters, res_path + "/studies/<string:study_id>/submitters")
    api.add_resource(StudyProtocols, res_path + "/studies/<string:study_id>/protocols")
    api.add_resource(GetProtocolForAssays, res_path + "/studies/<string:study_id>/protocols/meta")
    api.add_resource(StudyFactors, res_path + "/studies/<string:study_id>/factors")
    api.add_resource(StudyDescriptors, res_path + "/studies/<string:study_id>/descriptors")
    api.add_resource(StudyPublications, res_path + "/studies/<string:study_id>/publications")
    api.add_resource(Organism, res_path + "/studies/<string:study_id>/organisms")

    # Metabolite Annotation File (MAF)
    api.add_resource(MetaboliteAnnotationFile, res_path + "/studies/<string:study_id>/maf/validate")

    # Study
    # api.add_resource(StudySources, res_path + "/studies/<string:study_id>/sources")
    # api.add_resource(StudySamples, res_path + "/studies/<string:study_id>/samples")
    # api.add_resource(EditSampleFile, res_path + "/studies/<string:study_id>/samples/<string:sample_file_name>")
    # api.add_resource(StudyOtherMaterials, res_path + "/studies/<string:study_id>/otherMaterials")
    # api.add_resource(StudyProcesses, res_path + "/studies/<string:study_id>/processSequence")

    # Assay
    # api.add_resource(AssaySamples, res_path + "/studies/<string:study_id>/assays/samples")
    # api.add_resource(AssayOtherMaterials, res_path + "/studies/<string:study_id>/assays/otherMaterials")
    # api.add_resource(AssayDataFiles, res_path + "/studies/<string:study_id>/assays/dataFiles")
    # api.add_resource(AssayProcesses, res_path + "/studies/<string:study_id>/assays/processSequence")
    # api.add_resource(AssayTable, res_path + "/studies/<string:study_id>/assay/tableCell")
    # api.add_resource(EditAssayFile, res_path + "/studies/<string:study_id>/assay/<string:assay_file_name>")

    # Manipulating TSV tables
    api.add_resource(SimpleColumns, res_path + "/studies/<string:study_id>/column/<string:file_name>")
    api.add_resource(ComplexColumns, res_path + "/studies/<string:study_id>/columns/<string:file_name>")
    api.add_resource(ColumnsRows, res_path + "/studies/<string:study_id>/cells/<string:file_name>")
    api.add_resource(AddRows, res_path + "/studies/<string:study_id>/rows/<string:file_name>")
    api.add_resource(GetTsvFile, res_path + "/studies/<string:study_id>/<string:file_name>")
    api.add_resource(CompareTsvFiles, res_path + "/studies/<string:study_id>/compare-files")

    api.add_resource(BioStudies, res_path + "/studies/<string:study_id>/biostudies")
    api.add_resource(BioStudiesFromMTBLS, res_path + "/studies/biostudies")
    api.add_resource(Validation, res_path + "/studies/<string:study_id>/validate-study")
    api.add_resource(NewValidation, res_path + "/studies/<string:study_id>/validation")
    # Direct API consumers/Partners
    api.add_resource(Metabolon, res_path + "/partners/metabolon/<string:study_id>/confirm")
    api.add_resource(MetaspacePipeLine, res_path + "/partners/metaspace/<string:study_id>/import")

    # EBI utils
    api.add_resource(MapStudies, res_path + "/ebi-internal/zooma")
    api.add_resource(Ontology, res_path + "/ebi-internal/ontology")  # Add ontology resources
    api.add_resource(Placeholder, res_path + "/ebi-internal/placeholder")  # Add placeholder
    api.add_resource(Cellosaurus, res_path + "/ebi-internal/cellosaurus")  # Cellosaurus
    api.add_resource(Convert2ISAtab, res_path + "/ebi-internal/<string:study_id>/mzml2isatab")
    api.add_resource(ValidateMzML, res_path + "/ebi-internal/<string:study_id>/validate-mzml")
    api.add_resource(UserManagement, res_path + "/ebi-internal/users")
    api.add_resource(ExtractMSSpectra, res_path + "/ebi-internal/<string:study_id>/extract-peak-list")
    api.add_resource(ReindexStudy, res_path + "/ebi-internal/<string:study_id>/reindex")
    api.add_resource(Jira, res_path + "/ebi-internal/create_tickets")

    # api.add_resource(GoogleDocs, res_path + "/ebi-internal/curation_log")
    api.add_resource(EnzymePortalHelper, res_path + "/ebi-internal/check_if_metabolite/<string:chebi_id>")
    api.add_resource(OverrideValidation, res_path + "/ebi-internal/<string:study_id>/validate-study/override")
    api.add_resource(UpdateValidationFile, res_path + "/ebi-internal/<string:study_id>/validate-study/update-file")
    api.add_resource(SplitMaf, res_path + "/ebi-internal/<string:study_id>/split-maf")
    api.add_resource(ChEBIPipeLine, res_path + "/ebi-internal/<string:study_id>/chebi-pipeline")
    api.add_resource(ChEBIPipeLineLoad, res_path + "/ebi-internal/chebi-load")
    api.add_resource(LsfUtils, res_path + "/ebi-internal/cluster-jobs")
    api.add_resource(StudyStats, res_path + "/ebi-internal/study-stats")
    api.add_resource(GoogleCalendar, res_path + "/ebi-internal/google-calendar-update")

    api.add_resource(cronjob, res_path + "/ebi-internal/cronjob")
    api.add_resource(keggid,res_path+"/ebi-internal/keggid")

    # https://www.ebi.ac.uk:443/metabolights/ws/v2
    api.add_resource(reports, res_path + "/v2/reports")

    # ToDo, complete this: api.add_resource(CheckCompounds, res_path + "/ebi-internal/compound-names")


def main():
    print("Initialising application")
    initialize_app(application)
    logger.info("Starting server %s v%s", application.config.get('WS_APP_NAME'),
                application.config.get('WS_APP_VERSION'))
    # application.run(host="0.0.0.0", port=config.PORT, debug=config.DEBUG, ssl_context=context)
    print("Starting application")
    application.run(host="0.0.0.0", port=application.config.get('PORT'), debug=application.config.get('DEBUG'),
                    threaded=True)
    logger.info("Finished server %s v%s", application.config.get('WS_APP_NAME'),
                application.config.get('WS_APP_VERSION'))


print("before main")
if __name__ == "__main__":
    print("Setting ssl context for Flask server")
    context = ('ssl/wsapp.crt', 'ssl/wsapp.key')  # SSL certificate and key files
    main()
else:
    print("Setting ssl context for Gunicorn server")
    context = ('ssl/wsapp.crt', 'ssl/wsapp.key')  # SSL certificate and key files
    main()
