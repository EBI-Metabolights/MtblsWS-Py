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
WS_APP_VERSION = "1.01.05"
API_VERSION = WS_APP_VERSION
ISA_API_VERSION = "0.10.3"
METASPACE_APP_VERSION = "0.7.1"

WS_APP_NAME = "MtblsWS-Py"
WS_APP_DESCRIPTION = "MetaboLights RESTful WebService"
RESOURCES_PATH = "/metabolights/ws"
CORS_RESOURCES_PATH = RESOURCES_PATH + "/*"
API_DOC = RESOURCES_PATH + "/api/spec"
MTBLS_WS_RESOURCES_PATH = "/metabolights/webservice"
UPDATE_PATH_SUFFIX = "audit"

MTBLS_FILE_BASE = "<some local filesystem>/"
MTBLS_FTP_ROOT = MTBLS_FILE_BASE + "<some local filesystem>/"
STUDY_PATH = MTBLS_FILE_BASE + "/prod/<final file system>"
MTBLS_ZOOMA_FILE = "<local file>"
BIOPORTAL_TOKEN = '<your bioportal token>'
MZML_XSD_SCHEMA = ["<local file>", "<script location>"]

PARTNER_TEMPLATE_METABOLON = 'MTBLS study with the template files'

CORS_HOSTS = "http://localhost:8000",\
             "http://localhost:4200",\
             "http://localhost:8080",\
             "http://localhost.ebi.ac.uk:8080",\
             "http://wwwdev.ebi.ac.uk",\
             "http://ves-ebi-8d:8080",\
             "http://ves-ebi-8d.ebi.ac.uk:8080"

DELETED_SAMPLES_PREFIX_TAG = "__TO_BE_DELETED__"

# These variabled will be overridden in  instance/config.py
# MTBLS_WS_HOST = "https://www.ebi.ac.uk"
# MTBLS_WS_PORT = ""
# MTBLS_FTP_ROOT = "<Folder to private ftp root>"

DB_PARAMS = {
    'database': 'db-name', 'user': 'user-name', 'password': 'user-password', 'host': 'hostname', 'port': 1234
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

VALIDATIONS_FILE = "https://www.ebi.ac.uk/metabolights/editor/assets/configs/config20180618/validations.json"
OBO_FILE = "/net/isilon8/ftp_public/databases/chebi/ontology/chebi_lite.obo"
CHEBI_URL = "https://www.ebi.ac.uk/webservices/chebi/2.0/webservice?wsdl"
CHEBI_URL_WAIT = 300
CLASSYFIRE_ULR = "http://classyfire.wishartlab.com"
CLASSYFIRE_MAPPING = "/net/isilon8/ftp_public/databases/metabolights/submissionTool/ClassyFire_Mapping.tsv"
OPSIN_URL = "https://opsin.ch.cam.ac.uk/opsin/"
CHEMSPIDER_URL = "http://parts.chemspider.com/JSON.ashx?op="
CHEBI_UPLOAD_SCRIPT = ""

# METASPACE
METASPACE_DATABASE = "HMDB-v4"
METASPACE_FDR = 0.1
METASPACE_APP_NAME = "MMIT"
METASPACE_APP_DESCRIPTION = "METASPACE-MetaboLights Interface Tools"
AWS_CREDENTIALS = './instance/aws_credentials.cfg'
