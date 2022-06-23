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
from jinja2 import Environment, select_autoescape, FileSystemLoader

from app.wsapp_config import initialize_app

"""
MTBLS WS-Py

MetaboLights Python-based REST Web Service
"""
hostname = os.uname().nodename
current_dir = os.path.dirname(os.path.abspath(__file__))

application = Flask(__name__)

application.config.setdefault("PYTHON_WS_APPLICATION_PATH", current_dir)
logger = logging.getLogger('wslog')


def setup_logging():
    default_log_dir = os.path.join(current_dir, "logs")
    if not os.path.exists(default_log_dir):
        os.makedirs(default_log_dir, exist_ok=True)

    logging_config_file_path = None
    if "LOG_FILE_CONFIG" in os.environ and os.environ["LOG_FILE_CONFIG"]:
        logging_config_file_path = os.environ["LOG_FILE_CONFIG"]

    if logging_config_file_path and os.path.exists(logging_config_file_path):
        print(f"Using logging config file {logging_config_file_path}")
    else:
        default_logging_config_file_path = os.path.join(current_dir, f"logging_{hostname}.conf")
        logging_config_file_path = default_logging_config_file_path
        if os.path.exists(default_logging_config_file_path):
            print(f"Using default logging config file {default_logging_config_file_path}")
        else:
            print(f"Creating default logging config file {default_logging_config_file_path}")

            env = Environment(
                loader=FileSystemLoader('resources/'),
                autoescape=select_autoescape(['html', 'xml'])
            )
            template = env.get_template('template_logging.conf')
            content = {"hostname": hostname}
            log_file_content = template.render(content)
            with open(default_logging_config_file_path, "w") as file:
                file.writelines(log_file_content)

    logging.config.fileConfig(logging_config_file_path)
    print(f"Running on server: '{hostname}' using logging config {logging_config_file_path}")


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
