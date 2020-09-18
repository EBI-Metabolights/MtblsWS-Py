#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Mar-12
#  Modified by:   kenneth
#
#  Copyright 2020 EMBL - European Bioinformatics Institute
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

import os

PORT = 5000
WS_APP_BASE_LINK = "https://www.ebi.ac.uk/metabolights"
DEBUG = False
PROJECT_PATH = os.path.realpath(os.path.dirname(__file__))
STATIC_FOLDER = os.path.join(PROJECT_PATH, "static")
TEMPLATE_FOLDER = os.path.join(PROJECT_PATH, "templates")

# Increment when the WS app changes. Follow the Semantic Versioning schema:
#   MAJOR version when backwards incompatible changes are introduced
#   MINOR version when new functionality is added in a backwards-compatible manner
#   PATCH version when bugs are fixed (but still backwards-compatible)
WS_APP_VERSION = "1.7.4"
API_VERSION = WS_APP_VERSION
ISA_API_VERSION = "0.11.0"
METASPACE_API_VERSION = "1.7.2"
MZML2ISA_VERSION = "1.0.3"

GA_TRACKING_ID = ""

WS_APP_NAME = "MtblsWS-Py"
WS_APP_DESCRIPTION = "MetaboLights RESTful WebService"
RESOURCES_PATH = "/metabolights/ws"
CORS_RESOURCES_PATH = RESOURCES_PATH + "/*"
API_DOC = RESOURCES_PATH + "/api/spec"
MTBLS_WS_RESOURCES_PATH = "/metabolights/webservice"
UPDATE_PATH_SUFFIX = "audit"

MTBLS_FILE_BASE = "<some local filesystem>/"
MTBLS_FTP_ROOT = MTBLS_FILE_BASE + "<some local filesystem>/"
REPORTING_PATH= "<report folder name>/"
STUDY_PATH = MTBLS_FILE_BASE + "/prod/<final file system>"
FILE_SYSTEM_PATH = '<some local filesystem>/'
MTBLS_ZOOMA_FILE = "<local file>"
MTBLS_ONTOLOGY_FILE = "<local file>"
BIOPORTAL_TOKEN = '<your bioportal token>'
METABOLIGHTS_TOKEN = '<your administrative metabolights token>'
MZML_XSD_SCHEMA = ["<local file>", "<script location>"]

MTBLS_PRIVATE_FTP_ROOT = ""

#GOOGLE SHEETS
GOOLGE_ZOOMA_SHEET = "<Google sheet url>"
MTBLS_STATISITC = "<Google sheet url>"
MTBLS_CURATION_LOG ="<Google sheet url>"
MTBLS_CURATION_LOG_TEST = "<Google sheet url>"

GOOGLE_SHEET_TOKEN = "./instance/google_sheet_api_credentials.json"
GOOGLE_CALENDAR_TOKEN = "./instance/google_calendar_api_credentials.json"
GOOGLE_CALENDAR_ID = ""

PARTNER_TEMPLATE_METABOLON = 'TEMPLATES/METABOLON'
DEFAULT_TEMPLATE = 'TEMPLATES/DUMMY'

CORS_HOSTS = "http://localhost:8000",\
             "http://localhost:4200",\
             "http://localhost:8080",\
             "http://localhost.ebi.ac.uk:8080",\
             "http://wwwdev.ebi.ac.uk",\
             "http://wp-np3-15:8080",\
             "http://wp-np3-15.ebi.ac.uk:8080"

DELETED_SAMPLES_PREFIX_TAG = "__TO_BE_DELETED__"

# These variabled will be overridden in  instance/config.py
# MTBLS_WS_HOST = "https://www.ebi.ac.uk"
# MTBLS_WS_PORT = ""
# MTBLS_FTP_ROOT = "<Folder to private ftp root>"

DB_PARAMS = {
    'database': 'db-name', 'user': 'user-name', 'password': 'user-password', 'host': 'hostname', 'port': 1234
}

SSH_PARAMS = {
    'host': 'ebi-cli',
    'user': 'user_name',
    'password': 'user_password'
}

JIRA_PARAMS = {
    'username': 'jira_username',
    'password': 'jira_password'
}

# Connection Pool parameters
CONN_POOL_MIN = 1
CONN_POOL_MAX = 20

# Timeout in secounds when listing a large folder for files
FILE_LIST_TIMEOUT = 90


#chebi
REMOVED_HS_MOL_COUNT = 500

# Validations
VALIDATION_FILES_LIMIT = 10000
VALIDATIONS_FILE = "https://www.ebi.ac.uk/metabolights/editor/assets/configs/config20180618/validations.json"

FOLDER_EXCLUSION_LIST = ['audit', '.d', '.raw', 'metaspace', 'chebi', 'old', 'backup', 'chebi_pipeline_annotations',
                         '/audit', '/metaspace', '/chebi', '/old', '/backup', '/chebi_pipeline_annotations']

EMPTY_EXCLUSION_LIST = ['tempbase', 'metexplore_mapping.json', 'synchelper', '_chroms.inf', 'prosol_history', 'title',
                        'msprofile.bin', 'tcc_1.xml', 'msactualdefs.xml', 'msmasscal.bin', 'tcc_1.xml', 'format.temp']

IGNORE_FILE_LIST = ['msprofile', '_func', '_chroms', '_header', 'defaultmasscal', 'checksum.xml', 'info.xml',
                    'binpump', 'tdaspec', 'isopump', 'acqmethod', 'msperiodicactuals', 'tofdataindex',
                    'devices.xml', '_inlet', '_extern', 'synchelper', 'title', 'msts.xml', 'metexplore_mapping',
                    'tempbase', 'prosol_history', 'validation_files', 'pulseprogram', '_history', 'tcc_1',
                    'msactualdefs', 'msmasscal', 'fq1list', 'pdata', 'uxnmr', 'shimvalues', 'specpar', 'output',
                    'format.temp', 'scon2', 'stanprogram', 'precom', 'settings', 'outd', 'gpnam', 'base_info',
                    'clevels']


RAW_FILES_LIST = ['.d', '.raw', '.idb', '.cdf', '.wiff', '.scan', '.dat', '.cmp', '.cdf.cmp',
                  '.lcd', '.abf', '.jpf', '.xps', '.mgf']

DERIVED_FILES_LIST = ['.mzml', '.nmrml', '.mzxml', '.xml', '.mzdata', '.cef', '.cnx', '.peakml', '.xy', '.smp',
                      '.scan']

COMPRESSED_FILES_LIST = ['.zip', 'zipx', '.gz', '.cdf.gz', '.tar', '.7z', '.z', '.g7z', '.arj', '.rar',
                         '.bz2', '.arj', '.z', '.war', '.raw.rar']

INTERNAL_MAPPING_LIST = ['metexplore_mapping', 'chebi_pipeline_annotations', 'validation_report', 'validation_files']

# Other files
OBO_FILE = "/net/isilon8/ftp_public/databases/chebi/ontology/chebi_lite.obo"
CHEBI_URL = "https://www.ebi.ac.uk/webservices/chebi/2.0/webservice?wsdl"
CHEBI_URL_WAIT = 300
CHEBI_PIPELINE_URL = WS_APP_BASE_LINK + "/ws/ebi-internal/"
CLASSYFIRE_ULR = "http://classyfire.wishartlab.com"
CLASSYFIRE_MAPPING = "/net/isilon8/ftp_public/databases/metabolights/submissionTool/ClassyFire_Mapping.tsv"
OPSIN_URL = "https://opsin.ch.cam.ac.uk/opsin/"
CHEMSPIDER_URL = "http://parts.chemspider.com/JSON.ashx?op="
CHEBI_UPLOAD_SCRIPT = ""
MTBLS_SUBMITTER_EMAIL = ""

# METASPACE
METASPACE_DATABASE = "HMDB-v4"
METASPACE_FDR = 0.1
METASPACE_APP_NAME = "MMIT"
METASPACE_APP_DESCRIPTION = "METASPACE-MetaboLights Interface Tools"
AWS_CREDENTIALS = "./instance/aws_credentials.cfg"

LSF_COMMAND_PATH = '<path to LSF command, bsub/bkill/bjobs>'
LSF_COMMAND_EMAIL = '<email to use for EBI LSF jobs>'
