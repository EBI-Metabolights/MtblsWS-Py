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

import logging.config
import pathlib

from flask import Flask

from app.ws.assay_protocol import *
from app.ws.assay_table import *
from app.ws.biostudies import *
from app.ws.cronjob import *
from app.ws.curation_log import *
from app.ws.isaAssay import *
from app.ws.isaStudy import *
from app.ws.mtblsStudy import *
from app.ws.mtbls_maf import *
from app.ws.mzML2ISA import *
from app.ws.ontology import *
from app.ws.sample_table import *
from app.ws.table_editor import *
from app.wsapp_config import initialize_app

"""
MTBLS WS-Py

MetaboLights Python-based REST Web Service
"""
application = Flask(__name__, instance_relative_config=True)
hostname = os.uname().nodename
logging_config_file_name = 'logging_' + hostname + '.conf'
current_dir = pathlib.Path(__file__).parent.resolve()
file_path = os.path.join(current_dir, logging_config_file_name)

if not os.path.exists(file_path):
    print(f"{file_path} is not found. It is being created from default.")
    shutil.copy('logging.conf', logging_config_file_name)
    default_log_dir = os.path.join(current_dir, "logs")
    if not os.path.exists(default_log_dir):
        os.makedirs(default_log_dir, exist_ok=True)

logging.config.fileConfig('logging_' + hostname + '.conf')
logger = logging.getLogger('wslog')


def main():
    print("Initialising application")
    initialize_app(application)
    logger.info("Starting server %s v%s", application.config.get('WS_APP_NAME'),
                application.config.get('WS_APP_VERSION'))
    # application.run(host="0.0.0.0", port=config.PORT, debug=config.DEBUG, ssl_context=context)
    print("Starting application on port %s" % str(application.config.get('PORT')))
    application.run(host="0.0.0.0", port=application.config.get('PORT'), debug=application.config.get('DEBUG'),
                    threaded=True)
    logger.info("Finished server %s v%s", application.config.get('WS_APP_NAME'),
                application.config.get('WS_APP_VERSION'))


print("before main")
if __name__ == "__main__":
    print("Setting ssl context for Flask server")
    context = ('ssl/wsapp.crt', 'ssl/wsapp.key')  # SSL certificate and key files
    main()
else:
    print("Setting ssl context for Gunicorn server")
    context = ('ssl/wsapp.crt', 'ssl/wsapp.key')  # SSL certificate and key files
    main()
