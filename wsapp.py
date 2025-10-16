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
import re
from typing import List
# import re


from flask import Flask, request
from flask_restful import abort
from jinja2 import Environment, select_autoescape, FileSystemLoader
from app.config import get_settings
from app.config.model.server import EndpointDescription, EndpointMethodOption

from app.study_folder_utils import convert_relative_to_real_path
from app.wsapp_config import initialize_app
from app.ws.redis.redis import get_redis_server
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
    # if not os.path.exists(default_log_dir):
    #     os.makedirs(default_log_dir, exist_ok=True)

    logging_config_file_path = get_settings().server.log.log_config_file_path

    if logging_config_file_path and os.path.exists(logging_config_file_path):
        print(f"Using logging config file {logging_config_file_path}")
    else:
        default_logging_config_file_path = os.path.join(get_settings().server.log.log_path, f"logging_{hostname}.conf")
        if not os.path.exists(get_settings().server.log.log_path):
            os.makedirs(get_settings().server.log.log_path, exist_ok=True)
        logging_config_file_path = default_logging_config_file_path

        print(f"Creating default logging config file {default_logging_config_file_path}")

        env = Environment(
            loader=FileSystemLoader(convert_relative_to_real_path('resources/')),
            autoescape=select_autoescape(['html', 'xml'])
        )
        template = env.get_template('template_logging.conf')
        content = {"hostname": hostname}
        log_file_content = template.render(content)
        with open(default_logging_config_file_path, "w") as file:
            file.writelines(log_file_content)

    logging.config.fileConfig(logging_config_file_path)
    print(f"Running on server: '{hostname}' using logging config {logging_config_file_path}")

MANAGED_HTTP_METHODS = {"GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD"}
BYPASS_HTTP_METHODS = ("OPTIONS", "HEAD")
@application.before_request
def evaluate_request():
    settings = get_settings()
    allowed_host_domains = settings.server.service.allowed_host_domains
    protocol = request.scheme
    if "HTTP_X_FORWARDED_PROTO" in request.environ:
        protocol = request.environ["HTTP_X_FORWARDED_PROTO"]
        
    host_url =  f"{protocol}://{request.host}"
    allowed = [x for x in allowed_host_domains if re.fullmatch(x, host_url)]
    if not allowed:
        logger.warning(f"Request is not allowed from {host_url}")
        abort(403, message=f"Forbidden request from {host_url}.")    
    if request.method in BYPASS_HTTP_METHODS:
        return None
    
    disabled_endpoints: List[EndpointDescription] = settings.server.service.disabled_endpoints
    if disabled_endpoints:
        matched = check_request(request, disabled_endpoints)
        if matched:
            abort(503, message=f"This endpoint is disabled and unreachable.")
        
    
    if settings.server.service.maintenance_mode:
        enabled_endpoints = settings.server.service.enabled_endpoints_under_maintenance
        if enabled_endpoints:
            matched = check_request(request, enabled_endpoints)
            if not matched:
                abort(503, message=f"This endpoint is under maintenance now. Please try again later.")
        
    return None

def check_request(current_request, endpoints: List[EndpointDescription]):
    if current_request.method not in MANAGED_HTTP_METHODS:
        abort(400, message=f"{current_request.method} is unexpected request method.")
        
    context_path = get_settings().server.service.resources_path
    current_path = current_request.path.rstrip("/")
    
    for endpoint in endpoints:
        pattern = f"{context_path}{endpoint.path.rstrip('/')}"
        
        method: EndpointMethodOption = endpoint.method
        if isinstance(endpoint.method, EndpointMethodOption): 
            method: EndpointMethodOption = [endpoint.method]
        accepted_methods = set()
        for item in method:
            if item == EndpointMethodOption.ANY:
                accepted_methods = MANAGED_HTTP_METHODS
                break
            accepted_methods.add(item.value)
        if current_request.method not in accepted_methods:
            continue
        result = re.fullmatch(pattern, current_path)

        if result:
            return True
    return False

@application.after_request
def check_response(result):
    return result


# @application.before_request
# def check_study_maintenance_mode():
#     if request.method in BYPASS_HTTP_METHODS:
#         return None
#     settings = get_settings()

#     disabled_endpoints: List[
#         EndpointDescription
#     ] = settings.server.service.disabled_endpoints
#     if disabled_endpoints:
#         matched = check_request(request, disabled_endpoints)
#         if matched:
#             abort(503, message=f"This endpoint is disabled and unreachable.")

#     if settings.server.service.maintenance_mode:
#         enabled_endpoints = settings.server.service.enabled_endpoints_under_maintenance
#         if enabled_endpoints:
#             matched = check_request(request, enabled_endpoints)
#             if not matched:
#                 message = f"This endpoint is under maintenance. Please try again later."
#                 abort(503, message=message)
#     return None

setup_logging()
print("Initialising application")
initialize_app(application)
