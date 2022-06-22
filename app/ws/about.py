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

from flask import current_app as app
from flask import request
from flask_restful import Resource, fields, marshal_with
from flask_restful_swagger import swagger

from app.utils import metabolights_exception_handler
from app.ws.isaAssay import log_request
from app.ws.study.user_service import UserService

"""
MtblsWS-Py About

Basic description of the Web Service
"""

logger = logging.getLogger('wslog')

app_fields = {
    'WsName': fields.String,
    'WsVersion': fields.String,
    'WsDescription': fields.String,
    'WsURL': fields.String,
}

api_fields = {
    'ApiVersion': fields.String,
    'ApiDocumentation': fields.String,
    'ApiSpecification': fields.String,
    'IsatoolsApi': fields.String,
    'METASPACE-Api': fields.String,
    'mzml2isa': fields.String
}

about_fields = {
    'WsApp': fields.Nested(app_fields),
    'WsApi': fields.Nested(api_fields),
}


class About(Resource):
    """Basic description of the Web Service"""
    @swagger.operation(
        summary="About this Web Service",
        notes="Basic description of the Web Service",
        nickname="about",
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            }
        ]
    )
    @marshal_with(about_fields, envelope='AboutWS')
    def get(self):

        from flask import current_app as app

        """Get a basic description of the Web Service"""
        logger.info('Getting WS-about onto_information')
        api = {"ApiVersion": app.config.get('API_VERSION'),
               "ApiDocumentation": app.config.get('WS_APP_BASE_LINK') + app.config.get('API_DOC') + ".html",
               "ApiSpecification": app.config.get('WS_APP_BASE_LINK') + app.config.get('API_DOC') + ".json",
               "IsatoolsApi": app.config.get('ISA_API_VERSION'),
               "METASPACE-Api": app.config.get('METASPACE_API_VERSION'),
               "mzml2isa": app.config.get('MZML2ISA_VERSION')
               }
        appl = {"WsName": app.config.get('WS_APP_NAME'),
                "WsVersion": app.config.get('WS_APP_VERSION'),
                "WsDescription": app.config.get('WS_APP_DESCRIPTION'),
                "WsURL": app.config.get('WS_APP_BASE_LINK') + app.config.get('RESOURCES_PATH')
                }
        about = {'WsApp': appl, 'WsApi': api}
        return about


class AboutServer(Resource):
    """Basic description of the Web Service host"""
    @swagger.operation(
        summary="About this Web Service host",
        notes="Basic description of the Web Service service",
        nickname="about service",
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
        about = {'server_name': hostname}
        return about