import os

PORT = 5000
WS_APP_BASE_LINK = "http://www.ebi.ac.uk/metabolights"
DEBUG = False
PROJECT_PATH = os.path.realpath(os.path.dirname(__file__))

WS_APP_NAME = "MtblsWS-Py"
WS_APP_DESCRIPTION = "MetaboLights Python-based REST WebService"
# Increment when the WS app changes. Follow the Semantic Versioning schema:
#   MAJOR version when backwards incompatible changes are introduced
#   MINOR version when new functionality is added in a backwards-compatible manner
#   PATCH version when bugs are fixed (but still backwards-compatible)
WS_APP_VERSION = "0.9.4"
RESOURCES_PATH = "/mtbls/ws"

# Increment only when the API changes
API_VERSION = "0.9.1"
API_DOC = RESOURCES_PATH + "/api/spec"

STATIC_FOLDER = os.path.join(PROJECT_PATH, "static")
TEMPLATE_FOLDER = os.path.join(PROJECT_PATH, "templates")

MTBLS_WS_RESOURCES_PATH = "/metabolights/webservice"

UPDATE_PATH_SUFFIX = "audit"

CORS_HOSTS = "http://localhost:8000",\
             "http://localhost:4200",\
             "http://localhost:8080",\
             "http://localhost.ebi.ac.uk:8080",\
             "http://wwwdev.ebi.ac.uk",\
             "http://ves-ebi-8d:8080",\
             "http://ves-ebi-8d.ebi.ac.uk:8080"
