#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Feb-25
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

import json

from flask import request
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger

from app.ws.cronjob import getGoogleSheet
from app.ws.db_connection import *
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import log_request

logger = logging.getLogger('wslog')
wsc = WsClient()


class curation_log(Resource):
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
                "enum": ["daily_stats", "user_stats", "global"]
            },

            {
                "name": "start",
                "description": "Period start date,YYYYMMDD",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },

            {
                "name": "end",
                "description": "Period end date,YYYYMMDD",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },

            {
                "name": "queryFields",
                "description": "Specify the fields to return, the default is all options: "
                               "'studies_created','public','private','review','curation','user'",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
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
    def put(self):
        pass

    @swagger.operation(
        summary="Get MetaboLights curation log",
        notes='Get MetaboLights curation log',

        parameters=[
            {
                "name": "studyID",
                "description": "MetaboLights study ID,comma separated",
                "required": False,
                "allowEmptyValue": True,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "field",
                "description": "column name(s). comma separated",
                "required": False,
                "allowEmptyValue": True,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "page",
                "description": "page numbers, 100 studies each page",
                "required": False,
                "allowEmptyValue": True,
                "paramType": "query",
                "dataType": "number",
            },
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
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
        log_request(request)
        parser = reqparse.RequestParser()

        # studyID
        parser.add_argument('studyID', help='studyID')
        studyID = None
        if request.args:
            args = parser.parse_args(req=request)
            studyID = args['studyID']
            if studyID:
                if ',' in studyID:
                    studyID = studyID.split(',')
                else:
                    studyID = [studyID]
                studyID = [x.upper() for x in studyID]

        # column
        parser.add_argument('field', help='column name(s)')
        field = None
        if request.args:
            args = parser.parse_args(req=request)
            field = args['field']
            if field:
                if ',' in field:
                    field = field.split(',')
                else:
                    field = [field]

        # page
        parser.add_argument('page', help='page number')
        page = None
        if request.args:
            args = parser.parse_args(req=request)
            page = args['page']
            if page != None:
                page = int(args['page'])

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            abort(401)

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions('MTBLS1', user_token)
        if not write_access:
            abort(403)

        # Load google sheet
        try:
            google_df = getGoogleSheet(app.config.get('MTBLS_CURATION_LOG'), 'Studies',
                                       app.config.get('GOOGLE_SHEET_TOKEN'))
            google_df = google_df.set_index('MTBLS ID')
        except Exception as e:
            logger.info('Fail to load google sheet:', e)
            abort(404)
            return []

        if studyID == None or (len(studyID) > 100 and page != None):
            studyID = list(google_df.index.values)[100 * (page - 1): (100 * (page - 1) + 100)]

        # entire sheet
        if studyID == None and field == None:
            result = google_df.to_json(orient="index")

        # entire column
        elif studyID == None and len(field) > 0:
            result = google_df[field].to_json(orient="columns")

        # entire row
        elif len(studyID) > 0 and field == None:
            result = google_df.loc[studyID, :].to_json(orient="index")

        # combination
        else:
            result = google_df.loc[studyID, field].to_json(orient="index")

        return json.loads(result)
