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

import socket
from flask_cors import CORS
from flask_mail import Mail
from flask_restful import Api
from flask_restful_swagger import swagger
from app.config import get_settings
from app.utils import ValueMaskUtility

from app.ws.about import About, AboutServer
from app.ws.assay_protocol import GetProtocolForAssays
from app.ws.auth.accounts import UserAccounts
from app.ws.auth.authentication import (AuthLogin, AuthLoginWithToken,
                                        AuthUser, AuthUserStudyPermissions, AuthUserStudyPermissions2, AuthValidation, OneTimeTokenCreation, OneTimeTokenValidation)
from app.ws.biostudies import BioStudies, BioStudiesFromMTBLS
from app.ws.chebi.search.chebi_search_manager import ChebiSearchManager
from app.ws.chebi.search.curated_metabolite_table import CuratedMetaboliteTable
from app.ws.chebi.wsproxy import get_chebi_ws_proxy
from app.ws.chebi_workflow import ChEBIPipeLine, ChEBIPipeLineLoad, SplitMaf
from app.ws.chebi_ws import ChebiEntity, ChebiImageProxy, ChebiLiteEntity
from app.ws.cluster_jobs import LsfUtils
from app.ws.compare_files import CompareTsvFiles
from app.ws.compress import CompressRawDataFolders
from app.ws.cronjob import cronjob
from app.ws.curation_log import curation_log
from app.ws.db.dbmanager import DBManager
from app.ws.elasticsearch.elastic_service import ElasticsearchService
from app.ws.elasticsearch.search import ElasticSearchQuery
from app.ws.email.email_service import EmailService
from app.ws.enzyme_portal_helper import EnzymePortalHelper
from app.ws.folders.data_folders import DataFolders
from app.ws.ftp.ftp_operations import (FtpFolderPermission,
                                       FtpFolderPermissionModification,
                                       FtpFolderSyncStatus, PrivateFtpFolder,
                                       PrivateFtpFolderPath, SyncCalculation,
                                       SyncFromFtpFolder, SyncFromStudyFolder, SyncPublicStudyToFTP)
from app.ws.ftp_filemanager_testing import FTPRemoteFileManager
from app.ws.google_calendar import GoogleCalendar
from app.ws.internal import BannerMessage
from app.ws.isaAssay import StudyAssay, StudyAssayDelete
from app.ws.isaInvestigation import IsaInvestigation
from app.ws.isaStudy import (StudyContacts, StudyDescription, StudyDescriptors,
                             StudyFactors, StudyMetaInfo, StudyProtocols,
                             StudyPublications, StudyReleaseDate,
                             StudySubmitters, StudyTitle)
from app.ws.jira_update import Jira
from app.ws.metabolight_parameters import MetabolightsParameters
from app.ws.metabolight_statistics import MetabolightsStatistics
from app.ws.metaspace_pipeline import MetaspacePipeLine
from app.ws.mtbls_maf import (CombineMetaboliteAnnotationFiles,
                              MetaboliteAnnotationFile, MtblsMAFSearch)
from app.ws.mtblsCompound import (MtblsCompoundFile, MtblsCompoundIndex, MtblsCompoundIndexAll,
                                  MtblsCompoundIndexSync, MtblsCompoundSpectraFile, MtblsCompounds,
                                  MtblsCompoundsDetails)
from app.ws.mtblsStudy import (AuditFiles, CloneAccession, CreateAccession,
                               CreateUploadFolder, DeleteStudy,
                               IsaTabAssayFile, IsaTabInvestigationFile,
                               IsaTabSampleFile, MtblsPrivateStudies,
                               MtblsPublicStudiesIndexAll, MtblsStudies,
                               MtblsStudiesIndexAll, MtblsStudiesIndexSync,
                               MtblsStudiesWithMethods, MtblsStudyFolders,
                               MtblsStudyValidationStatus, MyMtblsStudies,
                               MyMtblsStudiesDetailed, PublicStudyDetail,
                               ReindexStudy, RetryReindexStudies, StudyFolderSynchronization,
                               UnindexedStudy)
from app.ws.mtblsWSclient import WsClient
from app.ws.mzML2ISA import Convert2ISAtab, ValidateMzML
from app.ws.ontology import Cellosaurus, Ontology, Placeholder
from app.ws.organism import Organism
from app.ws.partner_utils import Metabolon
from app.ws.pathway import fellaPathway, keggid
from app.ws.reports import (CrossReferencePublicationInformation,
                            StudyAssayTypeReports, reports)
from app.ws.send_files import SendFiles, SendFilesPrivate
from app.ws.settings.utils import get_study_settings
from app.ws.species import SpeciesTree
from app.ws.spectra import ExtractMSSpectra, ZipSpectraFiles
from app.ws.stats import StudyStats
from app.ws.study_actions import StudyStatus, ToggleAccess, ToggleAccessGet
from app.ws.study_files import (CopyFilesFolders, DeleteAsperaFiles, FileList,
                                SampleStudyFiles, StudyFiles, StudyFilesReuse,
                                StudyFilesTree, StudyRawAndDerivedDataFiles, SyncFolder,
                                UnzipFiles)
from app.ws.system import SystemTestEmail
from app.ws.table_editor import (AddRows, ColumnsRows, ComplexColumns,
                                 GetAssayMaf, GetTsvFile, SimpleColumns)
# from app.ws.tasks.study_file_encoding import FileEncodingChecker
from app.ws.tasks.create_json_files import (PublicStudyJsonExporter,
                                            StudyJsonExporter)
from app.ws.tasks.twitter import PublicStudyTweet
from app.ws.user_management import UserManagement
from app.ws.v1.studies import V1StudyDetail
from app.ws.validation import (NewValidation, OverrideValidation, StudyValidationTask,
                               ValidationFile, ValidationProcess,
                               ValidationComment, ValidationReport)


def configure_app(flask_app):
    
    settings = get_settings()
    flask_app.config.from_object(settings.flask)
    # These code completes WsClient initialization using flask app context
    if not WsClient.search_manager:
        chebi_proxy = get_chebi_ws_proxy()
        curation_table_file_path = get_settings().chebi.pipeline.curated_metabolite_list_file_location
        curation_table = CuratedMetaboliteTable.get_instance(curation_table_file_path)
        chebi_search_manager = ChebiSearchManager(ws_proxy=chebi_proxy, curated_metabolite_table=curation_table)
        WsClient.search_manager = chebi_search_manager

    if not WsClient.email_service:
        email_settings = settings.email
        flask_mail = Mail(flask_app)
        email_service = EmailService(settings=email_settings, mail=flask_mail)
        WsClient.email_service = email_service

    if not WsClient.elasticsearch_service:
        db_manager = DBManager.get_instance()
        study_settings = get_study_settings()
        elasticsearch_settings = settings.elasticsearch
        elasticsearch_service = ElasticsearchService(settings=elasticsearch_settings,
                                                     db_manager=db_manager, study_settings=study_settings)
        WsClient.elasticsearch_service = elasticsearch_service

    ########################################################################################################################
    #  Print important parameters to show on startup
    ########################################################################################################################
    study_settings = settings.study
    
    print("Configuration parameters...")
    print("................................................................................................................")
    print(f"DB HOST:\t\t\t{settings.database.connection.host}")
    print(f"STUDY_METADATA_ROOT_PATH:\t{study_settings.mounted_paths.study_metadata_files_root_path}")
    print(f"STUDY_AUDIT_FILES_ROOT_PATH:\t{study_settings.mounted_paths.study_audit_files_root_path}")
    print(f"STUDY_INTERNAL_FILES_ROOT_PATH:\t{study_settings.mounted_paths.study_internal_files_root_path}")
    print(f"STUDY_READONLY_FILES_ROOT_PATH:\t{study_settings.mounted_paths.study_readonly_files_root_path}")
    print(f"ELASTICSEARCH_HOST:\t\t{settings.elasticsearch.connection.host}")
    print(f"LSF_HOST:\t\t\t{settings.hpc_cluster.datamover.connection.host}")
    print(f"REPORTS_ROOT_PATH:\t\t{settings.study.mounted_paths.reports_root_path}")
    print(f"COMPOUND_FILES_ROOT_PATH:\t{study_settings.mounted_paths.compounds_root_path}")
    print(f"MAIL_SERVER:\t\t\t{settings.email.email_service.connection.host}:{settings.email.email_service.connection.port}")
    print(f"SERVER HOST NAME:\t\tActual: {socket.gethostname()}. WS_HOST_NAME:: {settings.server.service.mtbls_ws_host}")
    print(f"SERVER PORT:\t\t\t{settings.server.service.rest_api_port}")
    print("................................................................................................................")

    import yaml
    if settings.flask.DEBUG:
        import copy
        masked_copy = copy.deepcopy(settings.dict())
        mask_settings(masked_copy)
        masked_settings_text = yaml.dump(masked_copy)
        print(masked_settings_text)

def mask_settings(data: dict):
    for k, v in data.items():
        if isinstance(v, dict):
            mask_settings(v)
        else:
            masked_val = ValueMaskUtility.mask_value(k, v)
            data[k] = masked_val
            
def initialize_app(flask_app):
    configure_app(flask_app)

    CORS(flask_app, resources={get_settings().server.service.cors_resources_path: 
                               {"origins": get_settings().server.service.cors_hosts, 
                                "methods": {"GET, HEAD, POST, OPTIONS, PUT, DELETE"}}})

    res_path = get_settings().server.service.resources_path 
    api = swagger.docs(Api(flask_app),
                       description='MetaboLights RESTful WebService',
                       apiVersion=get_settings().server.description.metabolights_api_version,
                       basePath=get_settings().server.service.app_host_url,
                       api_spec_url=get_settings().server.service.api_doc,
                       resourcePath=res_path
                       )

    api.add_resource(About, res_path)
    api.add_resource(AboutServer, res_path + "/ebi-internal/server-info")
    api.add_resource(AuthLogin, res_path + "/auth/login")
    api.add_resource(AuthLoginWithToken, res_path + "/auth/login-with-token")
    api.add_resource(AuthValidation, res_path + "/auth/validate-token")
    api.add_resource(AuthUser, res_path + "/auth/user")
    api.add_resource(AuthUserStudyPermissions, res_path + "/auth/permissions/accession-number/<string:study_id>")
    api.add_resource(AuthUserStudyPermissions2, res_path + "/auth/permissions/obfuscationcode/<string:obfuscation_code>")
    api.add_resource(OneTimeTokenCreation, res_path + "/auth/create-onetime-token")
    api.add_resource(OneTimeTokenValidation, res_path + "/auth/login-with-onetime-token")
    api.add_resource(UserAccounts, res_path + "/auth/accounts")
    

    api.add_resource(MtblsMAFSearch, res_path + "/search/<string:query_type>")

    # MTBLS studies
    api.add_resource(V1StudyDetail, res_path + "/v1/study/<string:study_id>")
    # api.add_resource(V1StudyDetail, res_path + "/v1/security/studies/obfuscationcode/<string:obfuscationcode>/view")
    
    api.add_resource(MtblsStudies, res_path + "/studies")
    api.add_resource(MtblsPrivateStudies, res_path + "/studies/private")
    api.add_resource(MtblsStudiesWithMethods, res_path + "/studies/technology")
    api.add_resource(MyMtblsStudiesDetailed, res_path + "/studies/user")
    api.add_resource(MyMtblsStudies, res_path + "/studies/user/lite")
    api.add_resource(PublicStudyJsonExporter, res_path + "/studies/public/export-as-json")
    api.add_resource(StudyJsonExporter, res_path + "/studies/export-all-as-json")
    api.add_resource(PublicStudyDetail, res_path + "/studies/public/study/<string:study_id>")
    api.add_resource(GetAssayMaf, res_path + "/studies/public/study/<string:study_id>/assay/<int:sheet_number>/maf")
    api.add_resource(StudyRawAndDerivedDataFiles, res_path + "/studies/<string:study_id>/data-files")
    api.add_resource(StudyFiles, res_path + "/studies/<string:study_id>/files")
    api.add_resource(DeleteAsperaFiles, res_path + "/studies/<string:study_id>/aspera-files")
    api.add_resource(StudyFilesReuse, res_path + "/studies/<string:study_id>/files-fetch")

    api.add_resource(FileList, res_path + "/studies/<string:study_id>/fileslist")
    api.add_resource(StudyFilesTree, res_path + "/studies/<string:study_id>/files/tree")
    api.add_resource(SampleStudyFiles, res_path + "/studies/<string:study_id>/files/samples")
    api.add_resource(SendFiles, res_path + "/studies/<string:study_id>/download")
    api.add_resource(SendFilesPrivate, res_path + "/studies/<string:study_id>/download/<string:obfuscation_code>")
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

    api.add_resource(PrivateFtpFolder, res_path + "/studies/<string:study_id>/ftp")
    api.add_resource(PrivateFtpFolderPath, res_path + "/studies/<string:study_id>/ftp/path")
    api.add_resource(FtpFolderPermission, res_path + "/studies/<string:study_id>/ftp/permission")
    api.add_resource(FtpFolderPermissionModification, res_path + "/studies/<string:study_id>/ftp/permission/toggle")
    api.add_resource(SyncCalculation, res_path + "/studies/<string:study_id>/ftp/sync-calculation")
    api.add_resource(SyncFromFtpFolder, res_path + "/studies/<string:study_id>/ftp/sync")
    api.add_resource(FtpFolderSyncStatus, res_path + "/studies/<string:study_id>/ftp/sync-status")
    api.add_resource(SyncFromStudyFolder, res_path + "/studies/<string:study_id>/ftp/sync-from-study-folder")


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

    api.add_resource(MtblsCompounds, res_path + "/compounds/list")
    
    
    api.add_resource(MtblsCompoundsDetails, res_path + "/compounds/<string:accession>")
    
    api.add_resource(MtblsCompoundFile, res_path + "/compounds/<string:accession>/file")
    api.add_resource(MtblsCompoundSpectraFile, res_path + "/compounds/<string:accession>/<string:spectra_id>/file")
    
    api.add_resource(MtblsCompoundIndex, res_path + "/compounds/<string:accession>/es-index")
    api.add_resource(MtblsCompoundIndexAll, res_path + "/compounds/es-indexes/reindex-all")
    api.add_resource(MtblsCompoundIndexSync, res_path + "/compounds/es-indexes/sync-all")

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
    api.add_resource(StudyValidationTask, res_path + "/studies/<string:study_id>/validation-task")
    api.add_resource(ValidationReport, res_path + "/studies/<string:study_id>/validation-report")
    api.add_resource(StudyFolderSynchronization, res_path + "/studies/<string:study_id>/study-folders/rsync-task")
    
    api.add_resource(ValidationFile, res_path + "/studies/<string:study_id>/validate-study")
    api.add_resource(NewValidation, res_path + "/studies/<string:study_id>/validation")
    api.add_resource(MtblsStudyValidationStatus,
                     res_path + "/studies/<string:study_id>/validation-status/<string:validation_status>")
    # Direct API consumers/Partners
    api.add_resource(Metabolon, res_path + "/partners/metabolon/<string:study_id>/confirm")
    api.add_resource(MetaspacePipeLine, res_path + "/partners/metaspace/<string:study_id>/import")

    # EBI utils
    api.add_resource(Ontology, res_path + "/ebi-internal/ontology")  # Add ontology resources
    api.add_resource(Placeholder, res_path + "/ebi-internal/placeholder")  # Add placeholder
    api.add_resource(Cellosaurus, res_path + "/ebi-internal/cellosaurus")  # Cellosaurus
    api.add_resource(Convert2ISAtab, res_path + "/ebi-internal/<string:study_id>/mzml2isatab")
    api.add_resource(ValidateMzML, res_path + "/ebi-internal/<string:study_id>/validate-mzml")
    api.add_resource(UserManagement, res_path + "/ebi-internal/users")
    api.add_resource(ExtractMSSpectra, res_path + "/ebi-internal/<string:study_id>/extract-peak-list")
    api.add_resource(ReindexStudy, res_path + "/ebi-internal/<string:study_id>/es-index")
    api.add_resource(RetryReindexStudies, res_path + "/ebi-internal/studies/es-indexes/failed-indexes/retry")

    api.add_resource(UnindexedStudy, res_path + "/ebi-internal/studies/es-indexes/failed-indexes")
    api.add_resource(MtblsStudiesIndexSync, res_path + "/ebi-internal/studies/es-indexes/sync-all")
    api.add_resource(MtblsStudiesIndexAll, res_path + "/ebi-internal/studies/es-indexes/reindex-all")
    api.add_resource(MtblsPublicStudiesIndexAll, res_path + "/ebi-internal/public-studies/es-indexes/reindex-all")
    # api.add_resource(FileEncodingChecker, res_path + "/ebi-internal/studies/encoding-check")
    api.add_resource(Jira, res_path + "/ebi-internal/create_tickets")

    api.add_resource(EnzymePortalHelper, res_path + "/ebi-internal/check_if_metabolite/<string:chebi_id>")
    api.add_resource(OverrideValidation, res_path + "/ebi-internal/<string:study_id>/validate-study/override")
    api.add_resource(ValidationProcess, res_path + "/ebi-internal/<string:study_id>/validate-study/update-file")
    api.add_resource(ValidationComment, res_path + "/ebi-internal/<string:study_id>/validate-study/comment")
    api.add_resource(SplitMaf, res_path + "/ebi-internal/<string:study_id>/split-maf")
    api.add_resource(ChEBIPipeLine, res_path + "/ebi-internal/<string:study_id>/chebi-pipeline")
    api.add_resource(ChEBIPipeLineLoad, res_path + "/ebi-internal/chebi-load")
    api.add_resource(LsfUtils, res_path + "/ebi-internal/cluster-jobs")
    api.add_resource(StudyStats, res_path + "/ebi-internal/study-stats")
    api.add_resource(GoogleCalendar, res_path + "/ebi-internal/google-calendar-update")
    api.add_resource(MetabolightsParameters, res_path + "/ebi-internal/system/parameters")
    api.add_resource(MetabolightsStatistics, res_path + "/ebi-internal/system/statistics")
    api.add_resource(SystemTestEmail, res_path + "/ebi-internal/system/test-email")

    api.add_resource(cronjob, res_path + "/ebi-internal/cronjob")
    api.add_resource(FTPRemoteFileManager, res_path + "/ebi-internal/ftp-filemanager-testing")
    api.add_resource(keggid, res_path + "/ebi-internal/keggid")
    api.add_resource(fellaPathway, res_path + "/ebi-internal/fella-pathway")
    api.add_resource(PublicStudyTweet, res_path + "/ebi-internal/public-study-tweet")


    # https://www.ebi.ac.uk:443/metabolights/ws/v2
    api.add_resource(reports, res_path + "/v2/reports")
    api.add_resource(CrossReferencePublicationInformation, res_path + "/v2/europe-pmc-report")
    api.add_resource(StudyAssayTypeReports, res_path + "/v2/study-assay-type-reports")
    api.add_resource(ZipSpectraFiles, res_path + "/v2/zip-spectra-files")
    api.add_resource(curation_log, res_path + "/v2/curation_log")

    api.add_resource(ChebiLiteEntity, res_path + "/chebi/chebi-ids/<string:compound_name>")
    api.add_resource(ChebiEntity, res_path + "/chebi/entities/<string:chebi_id>")

    api.add_resource(MtblsStudyFolders, res_path + "/ebi-internal/<string:study_id>/study-folders/maintain")
    api.add_resource(CompressRawDataFolders, res_path + "/ebi-internal/<string:study_id>/study-folders/compress-raw-data-folders")
    
    # ToDo, complete this: api.add_resource(CheckCompounds, res_path + "/ebi-internal/compound-names")
    
    
    
    api.add_resource(SpeciesTree, res_path + "/species/tree")
    
    api.add_resource(ElasticSearchQuery, res_path + "/es-index/search")
    
    api.add_resource(BannerMessage, res_path + "/ebi-internal/banner")
    
    api.add_resource(DataFolders, res_path + "/ebi-internal/data-folders")
    
    api.add_resource(ChebiImageProxy, res_path + "/proxy/images/chebi/<chebiIdentifier>")
     
    
