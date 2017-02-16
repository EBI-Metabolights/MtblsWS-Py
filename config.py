import os

ENV = os.environ.get("ENVIRONMENT", "dev")
# ENV = os.environ.get("ENVIRONMENT", "prod")

PROJECT_PATH = os.path.realpath(os.path.dirname(__file__))

STATIC_FOLDER = os.path.join(PROJECT_PATH, "static")
TEMPLATE_FOLDER = os.path.join(PROJECT_PATH, "templates")

APP_NAME = "MetaboLights Python-based REST WebService"
APP_VERSION = "v1"
RESOURCES_PATH = "/mtbls/ws/" + APP_VERSION
API_DOC = RESOURCES_PATH + "/api/spec"
TEST_DATA_PATH = "testdata"

MTBLS_WS_HOST = "http://ves-ebi-8d"
MTBLS_WS_PORT = ":8080"
MTBLS_WS_RESOURCES_PATH = "/metabolights/webservice"

if ENV == "dev":
    STUDIES_INPUT_PATH = "/Users/jrmacias/Projects/Deploy-local/vnas-metabolights/metabolights/test/studies/"
    STUDIES_OUTPUT_PATH = "/Users/jrmacias/Projects/Deploy-local/vnas-metabolights/metabolights/test/userspace/"
    PORT = 5000
    APP_BASE_LINK = "http://localhost:" + str(PORT)
    DEBUG = True
else:
    STUDIES_INPUT_PATH = "/net/vnas-metabolights/metabolights/test/studies/"
    STUDIES_OUTPUT_PATH = "/net/vnas-metabolights/metabolights/test/userspace/"
    PORT = 5000
    APP_BASE_LINK = "https://www.ebi.ac.uk/metabolights"
    DEBUG = False
