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

from flask_cors import CORS
from flask_mail import Mail
from flask_restful import Api

import config
from app.ws.MapStudies import *
from app.ws.about import About
from app.ws.assay_protocol import *
from app.ws.biostudies import *
from app.ws.chebi.search.chebi_search_manager import ChebiSearchManager
from app.ws.chebi.search.curated_metabolite_table import CuratedMetaboliteTable
from app.ws.chebi.settings import get_chebi_ws_settings
from app.ws.chebi.wsproxy import ChebiWsProxy
from app.ws.chebi_workflow import SplitMaf, ChEBIPipeLine, ChEBIPipeLineLoad
from app.ws.chebi_ws import ChebiLiteEntity, ChebiEntity
from app.ws.cluster_jobs import LsfUtils, LsfUtilsStatus
from app.ws.compare_files import CompareTsvFiles
from app.ws.cronjob import *
from app.ws.curation_log import *
from app.ws.db.dbmanager import DBManager
from app.ws.db.settings import get_directory_settings
from app.ws.elasticsearch.elastic_service import ElasticsearchService
from app.ws.elasticsearch.settings import get_elasticsearch_settings
from app.ws.email.email_service import EmailService
from app.ws.email.settings import get_email_service_settings
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
from app.ws.pathway import fellaPathway
from app.ws.pathway import keggid
from app.ws.reports import CrossReferencePublicationInformation
from app.ws.reports import reports, StudyAssayTypeReports
from app.ws.sample_table import *
from app.ws.send_files import SendFiles
from app.ws.spectra import ExtractMSSpectra
from app.ws.stats import StudyStats
from app.ws.study_actions import StudyStatus, ToggleAccess, ToggleAccessGet
from app.ws.study_files import StudyFiles, StudyFilesTree, SampleStudyFiles, UnzipFiles, CopyFilesFolders, SyncFolder, \
    FileList, StudyFilesReuse, DeleteAsperaFiles
from app.ws.table_editor import *
from app.ws.user_management import UserManagement
from app.ws.validation import Validation, OverrideValidation, UpdateValidationFile, NewValidation


def configure_app(flask_app):
    flask_app.config.from_object('config')
    flask_app.config.from_pyfile('config.py', silent=True)

    # These code completes WsClient initialization using flask app context
    if not WsClient.search_manager:
        chebi_settings = get_chebi_ws_settings(flask_app)
        chebi_proxy = ChebiWsProxy(settings=chebi_settings)
        curation_table_file_path = flask_app.config.get("CURATED_METABOLITE_LIST_FILE_LOCATION")
        curation_table = CuratedMetaboliteTable.get_instance(curation_table_file_path)
        chebi_search_manager = ChebiSearchManager(ws_proxy=chebi_proxy, curated_metabolite_table=curation_table)
        WsClient.search_manager = chebi_search_manager

    if not WsClient.email_service:
        email_settings = get_email_service_settings(flask_app)
        flask_mail = Mail(flask_app)
        email_service = EmailService(settings=email_settings, mail=flask_mail)
        WsClient.email_service = email_service

    if not WsClient.elasticsearch_service:
        db_manager = DBManager.get_instance(flask_app)
        directory_settings = get_directory_settings(flask_app)
        elasticsearch_settings = get_elasticsearch_settings(flask_app)
        elasticsearch_service = ElasticsearchService(settings=elasticsearch_settings,
                                                     db_manager=db_manager, directory_settings=directory_settings)
        WsClient.elasticsearch_service = elasticsearch_service


def initialize_app(flask_app):
    configure_app(flask_app)

    CORS(flask_app, resources={flask_app.config.get("CORS_RESOURCES_PATH")},
         origins={flask_app.config.get('CORS_HOSTS')},
         methods={"GET, HEAD, POST, OPTIONS, PUT, DELETE"}
         )

    res_path = flask_app.config.get('RESOURCES_PATH')
    api = swagger.docs(Api(flask_app),
                       description='MetaboLights RESTful WebService',
                       apiVersion=flask_app.config.get('API_VERSION'),
                       basePath=flask_app.config.get('WS_APP_BASE_LINK'),
                       api_spec_url=flask_app.config.get('API_DOC'),
                       resourcePath=res_path
                       )

    api.add_resource(About, res_path)
    api.add_resource(MtblsMAFSearch, res_path + "/search/<string:query_type>")

    # MTBLS studies
    api.add_resource(MtblsStudies, res_path + "/studies")
    api.add_resource(MtblsPrivateStudies, res_path + "/studies/private")
    api.add_resource(MtblsStudiesWithMethods, res_path + "/studies/technology")
    api.add_resource(MyMtblsStudiesDetailed, res_path + "/studies/user")
    api.add_resource(MyMtblsStudies, res_path + "/studies/user/lite")
    api.add_resource(StudyFiles, res_path + "/studies/<string:study_id>/files")
    api.add_resource(DeleteAsperaFiles, res_path + "/studies/<string:study_id>/aspera-files")
    api.add_resource(StudyFilesReuse, res_path + "/studies/<string:study_id>/files-fetch")
    api.add_resource(FileList, res_path + "/studies/<string:study_id>/fileslist")
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
    api.add_resource(ToggleAccess, res_path + "/studies/<string:study_id>/access/toggle")
    api.add_resource(ToggleAccessGet, res_path + "/studies/<string:study_id>/access")
    api.add_resource(CopyFilesFolders, res_path + "/studies/<string:study_id>/sync")
    api.add_resource(SyncFolder, res_path + "/studies/<string:study_id>/dir_sync")
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
    api.add_resource(CombineMetaboliteAnnotationFiles, res_path + "/ebi-internal/mariana/maf/combine")

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
    api.add_resource(LsfUtilsStatus, res_path + "/ebi-internal/cluster-jobs-status")
    api.add_resource(StudyStats, res_path + "/ebi-internal/study-stats")
    api.add_resource(GoogleCalendar, res_path + "/ebi-internal/google-calendar-update")

    api.add_resource(cronjob, res_path + "/ebi-internal/cronjob")
    api.add_resource(keggid, res_path + "/ebi-internal/keggid")
    api.add_resource(fellaPathway, res_path + "/ebi-internal/fella-pathway")

    # https://www.ebi.ac.uk:443/metabolights/ws/v2
    api.add_resource(reports, res_path + "/v2/reports")
    api.add_resource(CrossReferencePublicationInformation, res_path + "/v2/europe-pmc-report")
    api.add_resource(StudyAssayTypeReports, res_path + "/v2/study-assay-type-reports")
    api.add_resource(curation_log, res_path + "/v2/curation_log")

    api.add_resource(ChebiLiteEntity, res_path + "/chebi/chebi-ids/<string:compound_name>")
    api.add_resource(ChebiEntity, res_path + "/chebi/entities/<string:chebi_id>")

    # ToDo, complete this: api.add_resource(CheckCompounds, res_path + "/ebi-internal/compound-names")
