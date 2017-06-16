import os

PORT = 5000
WS_APP_BASE_LINK = "https://www.ebi.ac.uk/metabolights"
DEBUG = False
PROJECT_PATH = os.path.realpath(os.path.dirname(__file__))

WS_APP_NAME = "MtblsWS-Py"
WS_APP_DESCRIPTION = "MetaboLights Python-based REST WebService"
# Increment when the WS app changes. Follow the Semantic Versioning schema:
#   MAJOR version when backwards incompatible changes are introduced
#   MINOR version when new functionality is added in a backwards-compatible manner
#   PATCH version when bugs are fixed (but still backwards-compatible)
WS_APP_VERSION = "0.4.1"
RESOURCES_PATH = "/mtbls/ws"

TEST_DATA_PATH = "testdata"

# Increment only when the API changes
API_VERSION = "0.4.1"
API_DOC = RESOURCES_PATH + "/api/spec"

STATIC_FOLDER = os.path.join(PROJECT_PATH, "static")
TEMPLATE_FOLDER = os.path.join(PROJECT_PATH, "templates")

# Calling the Java-based MTBLS WS
MTBLS_WS_HOST = "http://ves-ebi-90"
MTBLS_WS_PORT = ":8080"
MTBLS_WS_RESOURCES_PATH = "/metabolights/webservice"

UPDATE_PATH_SUFFIX = "/audit"
