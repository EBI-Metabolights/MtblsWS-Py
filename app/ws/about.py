#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Feb-26
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

import logging
from flask_restful import Resource, fields, marshal_with
from flask_restful_swagger import swagger

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
               "IsatoolsApi": app.config.get('ISA_API_VERSION')
               }
        appl = {"WsName": app.config.get('WS_APP_NAME'),
                "WsVersion": app.config.get('WS_APP_VERSION'),
                "WsDescription": app.config.get('WS_APP_DESCRIPTION'),
                "WsURL": app.config.get('WS_APP_BASE_LINK') + app.config.get('RESOURCES_PATH')
                }
        about = {'WsApp': appl, 'WsApi': api}
        return about
