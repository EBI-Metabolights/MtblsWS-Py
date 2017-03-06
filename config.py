import os


PORT = 5000
APP_BASE_LINK = "https://www.ebi.ac.uk/metabolights"
DEBUG = False
PROJECT_PATH = os.path.realpath(os.path.dirname(__file__))

APP_NAME = "MtblsWS-Py"
APP_DESCRIPTION = "MetaboLights Python-based REST WebService"
APP_VERSION = "0.1.0"
RESOURCES_PATH = "/mtbls/ws"
API_DOC = RESOURCES_PATH + "/api/spec"
TEST_DATA_PATH = "testdata"

STATIC_FOLDER = os.path.join(PROJECT_PATH, "static")
TEMPLATE_FOLDER = os.path.join(PROJECT_PATH, "templates")

# Calling the Java-based MTBLS WS
MTBLS_WS_HOST = "http://ves-ebi-8d"
MTBLS_WS_PORT = ":8080"
MTBLS_WS_RESOURCES_PATH = "/metabolights/webservice"

UPDATE_PATH_SUFFIX = "/audit"

