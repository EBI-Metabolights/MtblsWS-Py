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

from flask import request
from flask_restful import Resource, fields, marshal_with
from flask_restful_swagger import swagger
from app.config import get_settings

from app.utils import metabolights_exception_handler
from app.ws.ejprd_beacon.beacon_request_params import RequestParams
from app.ws.ejprd_beacon.beacon_response_builder import BeaconResponseBuilder
from app.ws.isaAssay import log_request
from app.ws.mtblsWSclient import WsClient

"""
MtblsWS-Py About

Basic description of the Web Service
"""

logger = logging.getLogger('wslog')
wsc = WsClient()


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

        from flask import current_app as app
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
        logger.info('Running a GET info request.')
        log_request(request)

        hostname = os.uname().nodename
        about = {'server_name': hostname}
        return about


class AboutMtblsBeacon(Resource):

    @swagger.operation(
        summary="Information about the Metabolights Beacon (GA4GH)",
        nickname="Beacon information",
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
        """""This one is implied by the reference implementation to have information about the number of datasets.
        For this purpose I will just reuse the public /studies/ bit of code """""
        log_request(request)
        beacon_request = RequestParams().from_request(request)
        studies = wsc.get_public_studies()
        response = BeaconResponseBuilder.build_beacon_info_response(studies, beacon_request, lambda x,y,z: x)
        return response


class MtblsBeaconServiceInfo(Resource):

    @swagger.operation(
        summary="Service summary of the Metabolights Beacon (GA4GH)",
        nickname="Beacon service summary",
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
        config = get_settings()
        beacon_response = BeaconResponseBuilder.build_beacon_service_info_response(conf=config.beacon)
        return beacon_response


class MtblsBeaconConfiguration(Resource):

    @swagger.operation(
        summary="Schema and Metadata Configuration of the Metabolights Beacon (GA4GH)",
        nickname="Beacon config summary",
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
        config = get_settings()
        beacon_response = BeaconResponseBuilder.build_configuration_response(conf=config.beacon)
        return beacon_response


class MtblsBeaconEntryTypes(Resource):

    @swagger.operation(
        summary="Get the entry types (queryable models) available on the Metabolights Beacon (GA4GH)",
        nickname="Available Beacon Entry Types",
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
        beacon_response = BeaconResponseBuilder.build_entry_type_response()
        return beacon_response

class MtblsBeaconMap(Resource):

    @swagger.operation(
        summary="Get the BeaconMap of the Metabolights Beacon (GA4GH)",
        nickname="Get BeaconMap",
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
        config = get_settings()
        beacon_response = BeaconResponseBuilder.build_configuration_response(conf=config.beacon, map=True)
        return beacon_response
