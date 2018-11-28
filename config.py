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
WS_APP_VERSION = "0.18.3"
API_VERSION = "0.13.0"

WS_APP_NAME = "MtblsWS-Py"
WS_APP_DESCRIPTION = "MetaboLights Python-based REST WebService"
RESOURCES_PATH = "/metabolights/swagger/ws"
CORS_RESOURCES_PATH = RESOURCES_PATH + "/*"
API_DOC = RESOURCES_PATH + "/api/spec"
MTBLS_WS_RESOURCES_PATH = "/metabolights/webservice"
UPDATE_PATH_SUFFIX = "audit"

MS_ASSAY_TEMPLATE = "file name"
NMR_ASSAY_TEMPLATE = "file name"

MTBLS_FILE_BASE = "<some local filesystem>/"
MTBLS_FTP_ROOT = MTBLS_FILE_BASE + "<some local filesystem>/"
STUDY_PATH = MTBLS_FILE_BASE + "/prod/<final file system>"
MTBLS_ZOOMA_FILE = "<local file>"
MZML_XSD_SCHEMA = ["<local file>","<script location>"]

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

PROTOCOL_PARAMS = [
    ['NMR', 'Extraction', 'Extraction Method'],
    ['MS',  'Extraction', 'Derivatization;Post Extraction'],
    ['NMR', 'NMR sample', 'NMR tube type;Temperature;Sample pH;Solvent'],
    ['NMR', 'NMR spectroscopy', 'Magnetic field strength;Pulse sequence name;Instrument;NMR Probe;Number of transients'],
    ['NMR', 'NMR assay', ''],
    ['MS',  'Data transformation', ''],
    ['NMR', 'Data transformation', ''],
    ['MS',  'Chromatography', 'Column type;Column model;Chromatography Instrument'],
    ['MS',  'Mass spectrometry', 'Scan polarity;Mass analyzer;Ion source;Instrument;Scan m/z range'],
    ['MS', 'Metabolite identification', ''],
    ['NMR', 'Metabolite identification', '']
]

DB_PARAMS = {
    'database': 'db-name', 'user': 'user-name', 'password': 'user-password', 'host': 'hostname', 'port': 1234
}