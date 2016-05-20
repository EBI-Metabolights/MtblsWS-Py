import os

ENV = os.environ.get('ENVIRONMENT', 'dev')

PROJECT_PATH = os.path.realpath(os.path.dirname(__file__))

STATIC_FOLDER = os.path.join(PROJECT_PATH, 'static')
TEMPLATE_FOLDER = os.path.join(PROJECT_PATH, 'templates')

APP_NAME = 'MetaboLights Python-based REST WebService'
APP_VERSION = 'v1'
RESOURCES_PATH = '/mtbls/ws/' + APP_VERSION
API_DOC = RESOURCES_PATH + '/api/spec'

if ENV == 'dev':
    PORT = 5000
    APP_BASE_LINK = 'http://localhost:' + str(PORT)
    DEBUG = True
else:
    APP_BASE_LINK = 'https://www.ebi.ac.uk/metabolights'
    DEBUG = False
