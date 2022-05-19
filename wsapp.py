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
import os
import shutil

from flask import Flask

from app.wsapp_config import initialize_app

"""
MTBLS WS-Py

MetaboLights Python-based REST Web Service
"""
hostname = os.uname().nodename
current_dir = os.path.dirname(os.path.abspath(__file__))
instance_dir = os.path.join(current_dir, "instance")
if not os.path.exists(instance_dir):
    os.makedirs(instance_dir, exist_ok=True)

if "INSTANCE_DIR" in os.environ and os.environ["INSTANCE_DIR"]:
    instance_dir = os.environ["INSTANCE_DIR"]
    logging_config_file_name = 'logging_docker.conf'
else:
    logging_config_file_name = 'logging_' + hostname + '.conf'

print(f"Running on: {hostname}")
print(f"Instance directory is: {instance_dir}")
application = Flask(__name__, instance_relative_config=True, instance_path=instance_dir)

application.config.setdefault("PYTHON_WS_APPLICATION_PATH", current_dir)
logger = logging.getLogger('wslog')


def setup_logging():
    logger_config_file_path = os.path.join(current_dir, logging_config_file_name)
    if not os.path.exists(logger_config_file_path):
        print(f"{logger_config_file_path} is not found. It is being created from default.")
        shutil.copy('logging.conf', logging_config_file_name)

    default_log_dir = os.path.join(current_dir, "logs")
    if not os.path.exists(default_log_dir):
        os.makedirs(default_log_dir, exist_ok=True)

    logging.config.fileConfig(logging_config_file_name)
    print(f"Running on: {hostname}")


def main():
    setup_logging()
    print("Initialising application")
    initialize_app(application)
    logger.info("Starting server %s v%s", application.config.get('WS_APP_NAME'),
                application.config.get('WS_APP_VERSION'))
    # application.run(host="0.0.0.0", port=config.PORT, debug=config.DEBUG, ssl_context=context)
    print("Starting application on port %s" % str(application.config.get('PORT')))
    application.run(host="0.0.0.0", port=application.config.get('PORT'), debug=application.config.get('DEBUG'),
                    threaded=True, use_reloader=False)
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
