#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Feb-28
#  Modified by:   kenneth
#
#  Copyright 2020 EMBL - European Bioinformatics Institute
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

import logging
import os
import sys

from flask import request
from flask_restful import Resource, fields, marshal_with
from flask_restful_swagger import swagger
from app.config import get_settings

from app.utils import metabolights_exception_handler
from app.ws.isaAssay import log_request

"""
MtblsWS-Py About

Basic description of the Web Service
"""

logger = logging.getLogger('wslog')


class About(Resource):
    """Basic description of the Web Service"""
    @swagger.operation(
        summary="About MetaboLights Web Service",
        notes="Basic description of the Web Service",
        nickname="about",
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            }
        ]
    )
    def get(self):
        server_settings = get_settings().server
        resources_path = server_settings.service.resources_path
        api_doc = f"{resources_path}{server_settings.service.api_doc}"
        
        api = {"version": server_settings.description.metabolights_api_version,
               "documentation": server_settings.service.app_host_url + api_doc + ".html",
               "specification": server_settings.service.app_host_url + api_doc + ".json",
               "isatoolsApi": server_settings.description.isa_api_version,
               "metaspaceApi": server_settings.description.metaspace_api_version,
               "mzml2isa": server_settings.description.mzml2isa_api_version
               }
        app = {"name": server_settings.description.ws_app_name,
                "version": server_settings.description.ws_app_version,
                "description": server_settings.description.ws_app_description,
                "url": server_settings.service.app_host_url + server_settings.service.resources_path 
                }
        about = {"about": {'app': app, 'api': api}}
        return about


class AboutServer(Resource):
    """Basic description of the Web Service host"""
    @swagger.operation(
        summary="Name of the Web Service host.",
        nickname="Web server host name",
        parameters=[
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    @metabolights_exception_handler
    def get(self):
        log_request(request)

        hostname = os.uname().nodename
        about = {'server_name': hostname, "python_version": sys.version.split()[0]}
        return about