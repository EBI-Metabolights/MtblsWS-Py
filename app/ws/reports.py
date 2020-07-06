#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Jul-6
#  Modified by:   Jiakang
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

from flask_restful import Resource
from flask_restful_swagger import swagger

from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.ontology_info import *

logger = logging.getLogger('wslog')
iac = IsaApiClient()
wsc = WsClient()


class reports(Resource):

    @swagger.operation(
        summary="Get Metabolights periodic report",
        notes="Get Metabolights periodic report",
        parameters=[
            {
                "name": "query",
                "description": "Report query",
                "required": True,
                "allowEmptyValue": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["daily_stats", "user_stats"]
            },

            {
                "name": "start",
                "description": "Period start date",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },

            {
                "name": "end",
                "description": "Period end date",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },

            {
                "name": "queryFields",
                "description": "Specifcy the fields to return, the default is all options: "
                               "{'studies_created','public','private','review','curation','user'}",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            }

        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax."
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
    def get(self):
        print('Hello world')

    # =========================== put =============================================

    @swagger.operation(
        summary="POST Metabolights periodic report",
        notes='POST Metabolights periodic report',

        parameters=[
            {
                "name": "query",
                "description": "Report query",
                "required": True,
                "allowEmptyValue": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["daily_stats", "user_stats"]
            },

            {
                "name": "start",
                "description": "Period start date",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },

            {
                "name": "end",
                "description": "Period end date",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },

            {
                "name": "queryFields",
                "description": "Specifcy the fields to return, the default is all options: "
                               "{'studies_created','public','private','review','curation','user'}",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax."
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
    def post(self):
        print('Hi world')
