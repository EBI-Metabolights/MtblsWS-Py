import logging
import os
import socket
from app import file_utils as utils


def get_path_from_environment(name, default_value):
    if name in os.environ and os.environ[name]:
        value = os.environ[name]
    else:
        value = default_value
        message = f"Environment {name} is not set, trying default path:  '{default_value}'"
        logger.error(message)
        print(message)

    if not os.path.exists(value):
        message = f"{name} path '{value}' does not exist. Set this environment variable"
        logger.error(message)
        print(message)
        exit(1)
    else:
        message = f"{name} is set as '{value}'"
        logger.info(message)
        print(message)
    return value


PROJECT_PATH = os.path.realpath(os.path.dirname(__file__))
STATIC_FOLDER = os.path.join(PROJECT_PATH, "static")
TEMPLATE_FOLDER = os.path.join(PROJECT_PATH, "templates")
RESOURCES_FOLDER = os.path.join(PROJECT_PATH, "resources")

logger = logging.getLogger('wslog')
default_configs_folder = os.path.join(PROJECT_PATH, "configs")
default_secrets_folder = os.path.join(PROJECT_PATH, ".secrets")
CONFIGS_DIR = get_path_from_environment("CONFIGS_DIR", default_configs_folder)
SECRETS_DIR = get_path_from_environment("SECRETS_DIR", default_secrets_folder)


########################################################################################################################
########################################################################################################################
########################################################################################################################
#
#                                           SERVER SETTINGS SECTION
#
#                       This section is ordered by key names in CONFIGS_DIR/server_settings.json file
#
########################################################################################################################
########################################################################################################################
########################################################################################################################

server_settings = utils.load_json_config_file("server_settings.json", configs_dir=CONFIGS_DIR)

########################################################################################################################
#   ENVIRONMENT
#
#   Load from the following file: CONFIGS_DIR/server_settings.json with key "ENV"
#
#   Configuration examples:
#
#   "ENV": "Development"
#   "ENV": "Test"
#   "ENV": "Production"
########################################################################################################################
ENV = server_settings["ENV"]

########################################################################################################################
#   SERVICE
#
#   Load from the following file: CONFIGS_DIR/server_settings.json with key "SERVICE"
########################################################################################################################
server_service = server_settings["SERVICE"]

PORT = server_service["PORT"]
APP_BASE_LINK = server_service["APP_BASE_LINK"]
WS_APP_BASE_LINK = server_service["WS_APP_BASE_LINK"]
MTBLS_WS_HOST = server_service["MTBLS_WS_HOST"]
MTBLS_WS_PORT = server_service["MTBLS_WS_PORT"]
RESOURCES_PATH = server_service["RESOURCES_PATH"]
CORS_HOSTS = server_service["CORS_HOSTS"]
CORS_RESOURCES_PATH = server_service["CORS_RESOURCES_PATH"]
API_DOC = server_service["API_DOC"]

########################################################################################################################
#   DESCRIPTION
#
#   Load from the following file: CONFIGS_DIR/server_settings.json with key "DESCRIPTION"
########################################################################################################################
server_description = server_settings["DESCRIPTION"]

WS_APP_NAME = server_description["WS_APP_NAME"]
WS_APP_DESCRIPTION = server_description["WS_APP_DESCRIPTION"]
WS_APP_VERSION = server_description["WS_APP_VERSION"]
API_VERSION = server_description["API_VERSION"]
ISA_API_VERSION = server_description["ISA_API_VERSION"]
METASPACE_API_VERSION = server_description["METASPACE_API_VERSION"]
MZML2ISA_VERSION = server_description["MZML2ISA_VERSION"]


########################################################################################################################
#   DEBUG
#
#   Load from the following file: CONFIGS_DIR/server_settings.json with key "DEBUG"
#
#   Configuration example 
#
#   "DEBUG": {
#     "DEBUG": true,
#     "DEBUG_LOG_HEADERS": true,
#     "DEBUG_LOG_BODY": false,
#     "DEBUG_LOG_JSON": false
#   }
########################################################################################################################
server_debug = server_settings["DEBUG"]

DEBUG = server_debug["DEBUG"]
DEBUG_LOG_HEADERS = server_debug["DEBUG_LOG_HEADERS"]
DEBUG_LOG_BODY = server_debug["DEBUG_LOG_BODY"]
DEBUG_LOG_JSON = server_debug["DEBUG_LOG_JSON"]

########################################################################################################################
#                                    END OF SERVER SETTINGS SECTION
########################################################################################################################

########################################################################################################################
########################################################################################################################
########################################################################################################################
#
#                                  CREDENTIALS AND SECRETS SECTION
#
#                          This section is ordered by file names in SECRETS_DIR
#
########################################################################################################################
#   APPLICATION SECRETS
#
#   Load from the following file: SECRETS_DIR/application_secrets.json
#
#    Configuration Example:
#
#    {
#       "APPLICATION_SECRET_KEY": "invalid-secret-key",
#       "BIOPORTAL_TOKEN": "uuid api token of bioportal service user",
#       "METABOLIGHTS_TOKEN": "uuid api token of bioportal metabolights service user",
#       "MTBLS_SUBMITTER_EMAIL": "email address of placeholder service user"
#     }
########################################################################################################################
application_secrets = utils.load_json_credentials_file("application_secrets.json", secrets_dir=SECRETS_DIR)

APPLICATION_SECRET_KEY = application_secrets["APPLICATION_SECRET_KEY"]
BIOPORTAL_TOKEN = application_secrets["BIOPORTAL_TOKEN"]
METABOLIGHTS_TOKEN = application_secrets["METABOLIGHTS_TOKEN"]
MTBLS_SUBMITTER_EMAIL = application_secrets["MTBLS_SUBMITTER_EMAIL"]

########################################################################################################################
########################################################################################################################
########################################################################################################################
#   AWS CREDENTIALS
#
#   Path of credential file. Default is SECRETS_DIR/jira_credentials.json
########################################################################################################################
AWS_CREDENTIALS = os.path.join(SECRETS_DIR, "aws_credentials.cfg")

########################################################################################################################
########################################################################################################################
########################################################################################################################
#   TWITTER CREDENTIALS
#
#   Path of credential file. Default is SECRETS_DIR/twitter_credentials.json
########################################################################################################################

TWITTER_CREDENTIALS = utils.load_json_credentials_file("twitter_credentials.json", secrets_dir=SECRETS_DIR)

########################################################################################################################
#   DATABASE CREDENTIALS
#
#   Load from the following file: SECRETS_DIR/database_credentials.json
#
#   Configuration example
#
#   {
#     "database": "database-name",
#     "user": "db-user",
#     "password": "db-user-password",
#     "host": "localhost",
#      "port": 5432
#   }
########################################################################################################################
DB_PARAMS = utils.load_json_credentials_file("database_credentials.json", secrets_dir=SECRETS_DIR)

########################################################################################################################
#   ELASTICSEARCH CREDENTIALS
#
#   Load from the following file: SECRETS_DIR/elasticsearch_credentials.json
#
#   Configuration example:
#
#   {
#     "ELASTICSEARCH_HOST": "localhost",
#     "ELASTICSEARCH_PORT": 9200,
#     "ELASTICSEARCH_USE_TLS": false,
#     "ELASTICSEARCH_USER_NAME": "",
#     "ELASTICSEARCH_USER_PASSWORD": ""
#   }
########################################################################################################################
elasticsearch_credentials = utils.load_json_credentials_file("elasticsearch_credentials.json", secrets_dir=SECRETS_DIR)

ELASTICSEARCH_HOST = elasticsearch_credentials["ELASTICSEARCH_HOST"]
ELASTICSEARCH_PORT = elasticsearch_credentials["ELASTICSEARCH_PORT"]
ELASTICSEARCH_USE_TLS = elasticsearch_credentials["ELASTICSEARCH_USE_TLS"]
ELASTICSEARCH_USER_NAME = elasticsearch_credentials["ELASTICSEARCH_USER_NAME"]
ELASTICSEARCH_USER_PASSWORD = elasticsearch_credentials["ELASTICSEARCH_USER_PASSWORD"]

########################################################################################################################
#   EMAIL SERVICE CREDENTIALS
#
#   Load from the following file: SECRETS_DIR/mail_service_credentials.json
#
#   Configuration Example:
#
#   {
#      "MAIL_SERVER": "localhost",
#      "MAIL_PORT": 25,
#      "MAIL_USE_TLS": false,
#      "MAIL_USE_SSL": false,
#      "MAIL_USERNAME": null,
#      "MAIL_PASSWORD": null
#   }
########################################################################################################################
email_service_credentials = utils.load_json_credentials_file("email_service_credentials.json", secrets_dir=SECRETS_DIR)

MAIL_SERVER = email_service_credentials["MAIL_SERVER"]
MAIL_PORT = email_service_credentials["MAIL_PORT"]
MAIL_USE_TLS = email_service_credentials["MAIL_USE_TLS"]
MAIL_USE_SSL = email_service_credentials["MAIL_USE_SSL"]
MAIL_USERNAME = email_service_credentials["MAIL_USERNAME"]
MAIL_PASSWORD = email_service_credentials["MAIL_PASSWORD"]

########################################################################################################################
#   FTP SERVER CREDENTIALS
#
#   Load from the following file: SECRETS_DIR/ftp_server_credentials.json
#
#   Configuration example
#
#   {
#     "PRIVATE_FTP_SERVER": "ftp-private-dir.ebi.ac.uk",
#     "PRIVATE_FTP_SERVER_USER": "metabolights-user-name",
#     "PRIVATE_FTP_SERVER_PASSWORD": "metabolights-user-password"
#   }
########################################################################################################################
ftp_server_credentials = utils.load_json_credentials_file("ftp_server_credentials.json", secrets_dir=SECRETS_DIR)

PRIVATE_FTP_SERVER = ftp_server_credentials["PRIVATE_FTP_SERVER"]
PRIVATE_FTP_SERVER_USER = ftp_server_credentials["PRIVATE_FTP_SERVER_USER"]
PRIVATE_FTP_SERVER_PASSWORD = ftp_server_credentials["PRIVATE_FTP_SERVER_PASSWORD"]

########################################################################################################################
#   GOOGLE CALENDAR API CREDENTIALS
#
#   Load file: CONFIGS_DIR/google_calendar_api_credentials.json
########################################################################################################################
GOOGLE_CALENDAR_TOKEN = os.path.join(SECRETS_DIR, "google_calendar_api_credentials.json")

########################################################################################################################
#   GOOGLE SERVICE SECRETS
#
#   Load from the following file: SECRETS_DIR/google_service_secrets.json
########################################################################################################################
google_service_secrets = utils.load_json_credentials_file("google_service_secrets.json", secrets_dir=SECRETS_DIR)

GOOGLE_CALENDAR_ID = google_service_secrets["GOOGLE_CALENDAR_ID"]
MARIANA_DRIVE_ID = google_service_secrets["MARIANA_DRIVE_ID"]
GA_TRACKING_ID = google_service_secrets["GA_TRACKING_ID"]

########################################################################################################################
#   GOOGLE SHEETS API CREDENTIALS
#
#   Load file: CONFIGS_DIR/google_sheet_api_credentials.json
########################################################################################################################
GOOGLE_SHEET_TOKEN = os.path.join(SECRETS_DIR, "google_sheet_api_credentials.json")

########################################################################################################################
#   JIRA CREDENTIALS
#
#   Load from the following file: SECRETS_DIR/jira_credentials.json
########################################################################################################################
JIRA_PARAMS = utils.load_json_credentials_file("jira_credentials.json", secrets_dir=SECRETS_DIR)

########################################################################################################################
#   SSH CREDENTIALS
#
#   Load from the following file: SECRETS_DIR/ssh_credentials.json
########################################################################################################################
SSH_PARAMS = utils.load_json_credentials_file("ssh_credentials.json", secrets_dir=SECRETS_DIR)

########################################################################################################################
#   UNUSED CREDENTIALS
#
#   This file and parameters will be removed
#   Load from the following file: SECRETS_DIR/unused_credentials.json
########################################################################################################################
unused_credentials = utils.load_json_credentials_file("unused_credentials.json", secrets_dir=SECRETS_DIR)

metaspace_unused_credentials = unused_credentials["UNUSED_METASPACE"]
METASPACE_ACCESS_KEY_ID = metaspace_unused_credentials["METASPACE_ACCESS_KEY_ID"]
METASPACE_SECRET_ACCESS_KEY = metaspace_unused_credentials["METASPACE_SECRET_ACCESS_KEY"]
METASPACE_BUCKET = metaspace_unused_credentials["METASPACE_BUCKET"]

########################################################################################################################
#                                    END OF SECRETS AND CREDENTIALS SECTION
########################################################################################################################


########################################################################################################################
########################################################################################################################
########################################################################################################################
#
#                                        SERVICE SETTINGS SECTION
#
#                       This section is ordered by key names in CONFIGS_DIR/service_settings.json file
#
########################################################################################################################
########################################################################################################################
########################################################################################################################

service_settings = utils.load_json_config_file("service_settings.json", configs_dir=CONFIGS_DIR)

########################################################################################################################
#   AUTHENTICATION SERVICE SETTINGS
#
#   Load from the following file: CONFIGS_DIR/service_settings.json with key "AUTH_SERVICE_SETTINGS"
#
#    Configuration Example:
#
#    "AUTH_SERVICE_SETTINGS": {
#     "ACCESS_TOKEN_HASH_ALGORITHM": "HS256",
#     "ACCESS_TOKEN_EXPIRES_DELTA": 300,
#     "ACCESS_TOKEN_ALLOWED_AUDIENCE": "Metabolights Editor",
#     "ACCESS_TOKEN_ISSUER_NAME": "Metabolights PythonWS"
#   }
########################################################################################################################
auth_service_settings = service_settings["AUTH_SERVICE_SETTINGS"]

ACCESS_TOKEN_HASH_ALGORITHM = auth_service_settings["ACCESS_TOKEN_HASH_ALGORITHM"]
ACCESS_TOKEN_EXPIRES_DELTA = auth_service_settings["ACCESS_TOKEN_EXPIRES_DELTA"]
ACCESS_TOKEN_ALLOWED_AUDIENCE = auth_service_settings["ACCESS_TOKEN_ALLOWED_AUDIENCE"]
ACCESS_TOKEN_ISSUER_NAME = auth_service_settings["ACCESS_TOKEN_ISSUER_NAME"]

########################################################################################################################
#   CLUSTER JOB SETTINGS
#
#   Load from the following file: CONFIGS_DIR/service_settings.json with key "CLUSTER_JOB"
#
#    Configuration Example:
#
#    "CLUSTER_JOB": {
#     "LSF_COMMAND_PATH": "SOFTWARE-PATH/bin/",
#     "LSF_COMMAND_EMAIL": "valid.email.address@ebi.ac.uk"
#   }
########################################################################################################################
cluster_job_settings = service_settings["CLUSTER_JOB"]

LSF_COMMAND_PATH = cluster_job_settings["LSF_COMMAND_PATH"]
LSF_COMMAND_EMAIL = cluster_job_settings["LSF_COMMAND_EMAIL"]
LSF_COMMAND_BJOBS = cluster_job_settings["LSF_COMMAND_BJOBS"]
LSF_COMMAND_BSUB = cluster_job_settings["LSF_COMMAND_BSUB"]
LSF_COMMAND_BKILL = cluster_job_settings["LSF_COMMAND_BKILL"]

########################################################################################################################
#   DATABASE SETTINGS
#
#   Load from the following file: CONFIGS_DIR/service_settings.json with key "DB_SETTINGS"
#
#   Configuration example:
#
#   "DB_SETTINGS": {
#     "CONN_POOL_MIN": 10,
#     "CONN_POOL_MAX": 30
#   }
########################################################################################################################
db_settings = service_settings["DB_SETTINGS"]

CONN_POOL_MIN = db_settings["CONN_POOL_MIN"]
CONN_POOL_MAX = db_settings["CONN_POOL_MAX"]

########################################################################################################################
#   EMAIL SERVICE CONFIGURATION
#
#   Load from the following file: CONFIGS_DIR/service_settings.json with key "EMAIL_SERVICE_SETTINGS"
#
#   Configuration Example:
#
#   "EMAIL_SERVICE_SETTINGS": {
#     "EMAIL_NO_REPLY_ADDRESS": "invalid-mail1@ebi.ac.uk",
#     "CURATION_EMAIL_ADDRESS": "invalid-mail2@ebi.ac.uk",
#     "METABOLIGHTS_HOST_URL": "https://www.ebi.ac.uk/metabolights",
#     "FTP_UPLOAD_HELP_DOC_URL": "https://docs.google.com/document/d/invalid-ftp-url"
#   }
########################################################################################################################
mail_service_settings = service_settings["EMAIL_SERVICE_SETTINGS"]

EMAIL_NO_REPLY_ADDRESS = mail_service_settings["EMAIL_NO_REPLY_ADDRESS"]
CURATION_EMAIL_ADDRESS = mail_service_settings["CURATION_EMAIL_ADDRESS"]
METABOLIGHTS_HOST_URL = mail_service_settings["METABOLIGHTS_HOST_URL"]
FTP_UPLOAD_HELP_DOC_URL = mail_service_settings["FTP_UPLOAD_HELP_DOC_URL"]

########################################################################################################################
#   FTP SERVER SETTINGS
#
#   Load from the following file: CONFIGS_DIR/service_settings.json with key "FTP_SERVER_SETTINGS"
#   FTP_PRIVATE_MOUNT_TYPE options: MOUNTED|CODON|OBJECT_STORAGE
#   PRIVATE_FTP_RELATIVE_STUDIES_ROOT_PATH relative path after FTP user mtblight home path
#   Configuration example:
#
#   "FTP_SERVER_SETTINGS": {
#     "FTP_PRIVATE_MOUNT_TYPE": "MOUNTED",
#     "MOUNTED_FTP_PRIVATE_ROOT_PATH": "/ftp/private/root/path",
#     "PRIVATE_FTP_RELATIVE_STUDIES_ROOT_PATH": "/metabolights/prod/studies",
#   }
########################################################################################################################
ftp_server_settings = service_settings["FTP_SERVER_SETTINGS"]

FTP_PRIVATE_MOUNT_TYPE = ftp_server_settings["FTP_PRIVATE_MOUNT_TYPE"]
MOUNTED_FTP_PRIVATE_ROOT_PATH = ftp_server_settings["MOUNTED_FTP_PRIVATE_ROOT_PATH"]
PRIVATE_FTP_RELATIVE_STUDIES_ROOT_PATH = ftp_server_settings["PRIVATE_FTP_RELATIVE_STUDIES_ROOT_PATH"]

########################################################################################################################
#   GOOGLE SERVICE SETTINGS
#
#   Load from the following file: CONFIGS_DIR/service_settings.json with key "GOOGLE_SERVICE_SETTINGS"
########################################################################################################################
google_service_settings = service_settings["GOOGLE_SERVICE_SETTINGS"]

GOOLGE_ZOOMA_SHEET = google_service_settings["GOOLGE_ZOOMA_SHEET"]
MTBLS_STATISITC = google_service_settings["MTBLS_STATISITC"]
LC_MS_STATISITC = google_service_settings["LC_MS_STATISITC"]
MTBLS_CURATION_LOG = google_service_settings["MTBLS_CURATION_LOG"]
MTBLS_CURATION_LOG_TEST = google_service_settings["MTBLS_CURATION_LOG_TEST"]
EUROPE_PMC_REPORT = google_service_settings["EUROPE_PMC_REPORT"]

########################################################################################################################
#   METASPACE SETTINGS
#
#   Load from the following file: CONFIGS_DIR/service_settings.json with key "METASPACE"
#
#   Configuration example:
#
#   "METASPACE": {
#     "METASPACE_DATABASE": "DATABASE-NAME",
#     "METASPACE_FDR": 0.1
#   }
########################################################################################################################
metaspace_settings = service_settings["METASPACE"]

METASPACE_DATABASE = metaspace_settings["METASPACE_DATABASE"]
METASPACE_FDR = metaspace_settings["METASPACE_FDR"]

########################################################################################################################
#                                    END OF SERVICE SETTINGS SECTION
########################################################################################################################


########################################################################################################################
########################################################################################################################
########################################################################################################################
#
#                                             CHEBI SETTINGS SECTION
#
#                       This section is ordered by key names in CONFIGS_DIR/chebi_settings.json file
#
########################################################################################################################
chebi_settings = utils.load_json_config_file("chebi_settings.json", configs_dir=CONFIGS_DIR)

########################################################################################################################
#   CHEBI SERVICE
#
#   Load from the following file: CONFIGS_DIR/chebi_settings.json with key "CHEBI_SERVICE_SETTINGS"
########################################################################################################################
chebi_service_settings = chebi_settings["CHEBI_SERVICE_SETTINGS"]

CURATED_METABOLITE_LIST_FILE_LOCATION = chebi_service_settings["CURATED_METABOLITE_LIST_FILE_LOCATION"]
CHEBI_WS_WSDL = chebi_service_settings["CHEBI_WS_WSDL"]
CHEBI_WS_WSDL_SERVICE = chebi_service_settings["CHEBI_WS_WSDL_SERVICE"]
CHEBI_WS_WSDL_SERVICE_PORT = chebi_service_settings["CHEBI_WS_WSDL_SERVICE_PORT"]
CHEBI_WS_STRICT = chebi_service_settings["CHEBI_WS_STRICT"]
CHEBI_WS_XML_HUGE_TREE = chebi_service_settings["CHEBI_WS_XML_HUGE_TREE"]
CHEBI_WS_WSDL_SERVICE_BINDING_LOG_LEVEL = chebi_service_settings["CHEBI_WS_WSDL_SERVICE_BINDING_LOG_LEVEL"]

########################################################################################################################
#   CHEBI PIPELINE
#
#   Load from the following file: CONFIGS_DIR/chebi_settings.json with key "CEHBI_PIPELINE"
########################################################################################################################
chebi_pipeline_settings = chebi_settings["CEHBI_PIPELINE"]

CHEBI_UPLOAD_SCRIPT = chebi_pipeline_settings["CHEBI_UPLOAD_SCRIPT"]
CHEBI_PIPELINE_URL = chebi_pipeline_settings["CHEBI_PIPELINE_URL"]
CHEBI_PIPELINE_ANNOTATION_FOLDER = chebi_pipeline_settings["CHEBI_PIPELINE_ANNOTATION_FOLDER"]
OBO_FILE = chebi_pipeline_settings["OBO_FILE"]
CHEBI_URL = chebi_pipeline_settings["CHEBI_URL"]
CHEBI_URL_WAIT = chebi_pipeline_settings["CHEBI_URL_WAIT"]
REMOVED_HS_MOL_COUNT = chebi_pipeline_settings["REMOVED_HS_MOL_COUNT"]
CLASSYFIRE_ULR = chebi_pipeline_settings["CLASSYFIRE_ULR"]
CLASSYFIRE_MAPPING = chebi_pipeline_settings["CLASSYFIRE_MAPPING"]
OPSIN_URL = chebi_pipeline_settings["OPSIN_URL"]
CHEMSPIDER_URL = chebi_pipeline_settings["CHEMSPIDER_URL"]
CHEM_PLUS_URL = chebi_pipeline_settings["CHEM_PLUS_URL"]
UNICHEM_URL = chebi_pipeline_settings["UNICHEM_URL"]
DIME_URL = chebi_pipeline_settings["DIME_URL"]

########################################################################################################################
#                                    END OF CHEBI SETTINGS SECTION
########################################################################################################################


########################################################################################################################
########################################################################################################################
########################################################################################################################
#
#                                             STUDY FILE SETTINGS SECTION
#
#                     This section is ordered by key names in CONFIGS_DIR/study_file_settings.json file
#
########################################################################################################################
study_file_settings = utils.load_json_config_file("study_file_settings.json", configs_dir=CONFIGS_DIR)

########################################################################################################################
#   STUDY FILES
#
#   Load from the following file: CONFIGS_DIR/study_file_settings.json with key "STUDY_FILES"
########################################################################################################################
study_files_settings = study_file_settings["STUDY_FILES"]

STUDY_PATH = study_files_settings["STUDY_PATH"]
DEFAULT_TEMPLATE = study_files_settings["DEFAULT_TEMPLATE"]
PARTNER_TEMPLATE_METABOLON = study_files_settings["PARTNER_TEMPLATE_METABOLON"]
UPDATE_PATH_SUFFIX = study_files_settings["UPDATE_PATH_SUFFIX"]
FILES_LIST_JSON = study_files_settings["FILES_LIST_JSON"]
FILE_LIST_TIMEOUT = study_files_settings["FILE_LIST_TIMEOUT"]
study_files_settings = study_file_settings["STUDY_FILES"]
REPORTING_ROOT_PATH = study_files_settings["REPORTING_ROOT_PATH"]
MARIANA_PATH = study_files_settings["MARIANA_PATH"]
REPORTING_PATH = study_files_settings["REPORTING_PATH"]

########################################################################################################################
#   ONTOLOGY
#
#   Load from the following file: CONFIGS_DIR/study_file_settings.json with key "ONTOLOGY"
########################################################################################################################
ontology_settings = study_file_settings["ONTOLOGY"]

MTBLS_ONTOLOGY_FILE = ontology_settings["MTBLS_ONTOLOGY_FILE"]
MTBLS_ZOOMA_FILE = ontology_settings["MTBLS_ZOOMA_FILE"]

########################################################################################################################
#   VALIDATION
#
#   Load from the following file: CONFIGS_DIR/study_file_settings.json with key "VALIDATION"
########################################################################################################################
validation_settings = study_file_settings["VALIDATION"]

MZML_XSD_SCHEMA = validation_settings["MZML_XSD_SCHEMA"]
VALIDATIONS_FILE = validation_settings["VALIDATIONS_FILE"]
VALIDATION_SCRIPT = validation_settings["VALIDATION_SCRIPT"]
VALIDATION_FILES_LIMIT = validation_settings["VALIDATION_FILES_LIMIT"]

########################################################################################################################
#   FILE FILTERS
#
#   Load from the following file: CONFIGS_DIR/study_file_settings.json with key "FILE_FILTERS"
########################################################################################################################
file_filter_settings = study_file_settings["FILE_FILTERS"]

DELETED_SAMPLES_PREFIX_TAG = file_filter_settings["DELETED_SAMPLES_PREFIX_TAG"]
FOLDER_EXCLUSION_LIST = file_filter_settings["FOLDER_EXCLUSION_LIST"]
EMPTY_EXCLUSION_LIST = file_filter_settings["EMPTY_EXCLUSION_LIST"]
IGNORE_FILE_LIST = file_filter_settings["IGNORE_FILE_LIST"]
RAW_FILES_LIST = file_filter_settings["RAW_FILES_LIST"]
DERIVED_FILES_LIST = file_filter_settings["DERIVED_FILES_LIST"]
COMPRESSED_FILES_LIST = file_filter_settings["COMPRESSED_FILES_LIST"]
INTERNAL_MAPPING_LIST = file_filter_settings["INTERNAL_MAPPING_LIST"]
RSYNC_EXCLUDE_LIST = file_filter_settings["RSYNC_EXCLUDE_LIST"]
DERIVED_DATA_FOLDER_LST = file_filter_settings["DERIVED_DATA_FOLDER_LST"]

########################################################################################################################
#                                    END OF STUDY FILE SETTINGS SECTION
########################################################################################################################


########################################################################################################################
########################################################################################################################
########################################################################################################################
#
#                                             UNUSED SETTINGS SECTION
#
#                     This section is for backward compatibility and will be removed next release.
#                     This section is ordered by key names in CONFIGS_DIR/unused_settings.json file
#
########################################################################################################################
unused_settings = utils.load_json_config_file("unused_settings.json", configs_dir=CONFIGS_DIR)

MS_ASSAY_TEMPLATE = unused_settings["UNUSED_FILES"]["MS_ASSAY_TEMPLATE"]
NMR_ASSAY_TEMPLATE = unused_settings["UNUSED_FILES"]["NMR_ASSAY_TEMPLATE"]

MTBLS_FILE_BASE = unused_settings["UNUSED_PATHS"]["MTBLS_FILE_BASE"]

FILE_SYSTEM_PATH = unused_settings["UNUSED_INSTANCE_SETTINGS"]["FILE_SYSTEM_PATH"]
MTBLS_WS_RESOURCES_PATH = unused_settings["UNUSED_INSTANCE_SETTINGS"]["MTBLS_WS_RESOURCES_PATH"]

VALIDATION_RUN_MSG = unused_settings["UNUSED_VALIDATION_SETTINGS"]["VALIDATION_RUN_MSG"]
ASSAY_VALIDATION_FILE = unused_settings["UNUSED_VALIDATION_SETTINGS"]["ASSAY_VALIDATION_FILE"]
FILES_VALIDATION_FILE = unused_settings["UNUSED_VALIDATION_SETTINGS"]["FILES_VALIDATION_FILE"]
METADATA_VALIDATION_FILE = unused_settings["UNUSED_VALIDATION_SETTINGS"]["METADATA_VALIDATION_FILE"]
COMPLETE_VALIDATION_FILE = unused_settings["UNUSED_VALIDATION_SETTINGS"]["COMPLETE_VALIDATION_FILE"]


METASPACE = unused_settings["METASPACE"]["METASPACE"]
METASPACE_APP_NAME = unused_settings["METASPACE"]["METASPACE_APP_NAME"]
METASPACE_APP_DESCRIPTION = unused_settings["METASPACE"]["METASPACE_APP_DESCRIPTION"]

########################################################################################################################
#                                    END OF STUDY FILE SETTINGS SECTION
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
#
#                                           CLUSTER JOB SETTINGS SECTION
#
#                       This section is ordered by key names in CONFIGS_DIR/cluster_settings.json file
#
########################################################################################################################
########################################################################################################################
########################################################################################################################

cluster_settings = utils.load_json_config_file("cluster_settings.json", configs_dir=CONFIGS_DIR)

########################################################################################################################
#   CLUSTER
#
#   Load from the following file: CONFIGS_DIR/cluster_settings.json with key "CLUSTER"
########################################################################################################################
cluster = cluster_settings["CLUSTER"]

LSF_HOST = cluster["LSF_HOST"]
LSF_HOST_USER = cluster["LSF_HOST_USER"]
LSF_DATAMOVER_USER = cluster["LSF_DATAMOVER_USER"]
LSF_HOST_SSH_CMD = cluster["LSF_HOST_SSH_CMD"]
LSF_BSUB_DEFAULT_Q = cluster["LSF_BSUB_DEFAULT_Q"]
LSF_DATAMOVER_Q = cluster["LSF_DATAMOVER_Q"]
LSF_BSUB_STANDARD_Q = cluster["LSF_BSUB_STANDARD_Q"]
LSF_BSUB_LONG_PROCESS_Q = cluster["LSF_BSUB_LONG_PROCESS_Q"]
LSF_DATAMOVER_STUDY_PATH = cluster["LSF_DATAMOVER_STUDY_PATH"]
LSF_DATAMOVER_FTP_PRIVATE_HOME = cluster["LSF_DATAMOVER_FTP_PRIVATE_HOME"]

########################################################################################################################
#   JOB
#
#   Load from the following file: CONFIGS_DIR/server_settings.json with key "JOB"
########################################################################################################################
JOB = cluster_settings["JOB"]

JOB_SUBMIT_COMMAND = JOB["JOB_SUBMIT_COMMAND"]
JOB_RUNNING_COMMAND = JOB["JOB_RUNNING_COMMAND"]
JOB_KILL_COMMAND = JOB["JOB_KILL_COMMAND"]
JOB_TRACK_EMAIL = JOB["JOB_TRACK_EMAIL"]
JOB_TRACK_LOG_LOCATION = JOB["JOB_TRACK_LOG_LOCATION"]
JOB_STATUS_READ_TIMEOUT = JOB["JOB_STATUS_READ_TIMEOUT"]
########################################################################################################################

########################################################################################################################
#  Print important parameters to show on startup
########################################################################################################################
print("Configuration parameters are loaded...")
print("................................................................................................................")
print(f"DB_PARAMS['host']:\t{DB_PARAMS['host']}")
print(f"STUDY_PATH:\t\t{STUDY_PATH}")
print(f"ELASTICSEARCH_HOST:\t{ELASTICSEARCH_HOST}")
print(f"LSF_HOST:\t\t{LSF_HOST}")
print(f"FTP_PRIVATE_MOUNT_TYPE:\t{FTP_PRIVATE_MOUNT_TYPE}")
print(f"REPORTING_ROOT_PATH:\t{REPORTING_ROOT_PATH}")
print(f"MAIL_SERVER:\t\t{MAIL_SERVER}:{MAIL_PORT}")
print(f"SERVER HOST NAME:\t{socket.gethostname()}")
print(f"SERVER PORT:\t\t{PORT}")
print("................................................................................................................")


########################################################################################################################
########################################################################################################################
########################################################################################################################
#                                           END OF CONFIGURATION FILE
########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
