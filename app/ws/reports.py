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

from datetime import datetime

from flask import jsonify
from flask import request, abort
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger

from app.ws.db_connection import get_connection
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.ontology_info import *
from app.ws.utils import log_request

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
    def post(self):
        log_request(request)
        parser = reqparse.RequestParser()

        parser.add_argument('query', help='Report query')
        query = None
        if request.args:
            args = parser.parse_args(req=request)
            query = args['query']
            if query:
                query = query.strip()

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

        if query == 'daily_stats':
            # try:
            sql = open('./instance/study_report.sql', 'r').read()
            postgresql_pool, conn, cursor = get_connection()
            cursor.execute(sql)
            dates = cursor.fetchall()
            data = {}
            for dt in dates:
                dict_temp = {dt[0].strftime('%Y-%m-%d'):
                                 {'studies_created': dt[1],
                                  'public': dt[2],
                                  'review': dt[3],
                                  'curation': dt[4],
                                  'user': dt[5]
                                  }
                             }
                data = {**data, **dict_temp}
            res = {"created_at": "2020-07-07", "updated_at": datetime.today().strftime('%Y-%m-%d'), 'data': data}
            # j = json.dumps(res)
            return jsonify(res)


        if query == 'user_stats':
            # try:
            sql = open('./instance/user_report.sql', 'r').read()
            postgresql_pool, conn, cursor = get_connection()
            cursor.execute(sql)
            result = cursor.fetchall()
            data = {}
            user_count = 0
            active_user = 0
            for dt in result:
                dict_temp = {dt[0] :
                                 {'user_email': dt[1],
                                  'country_code' : dt[2],
                                  'total': dt[5],
                                  'submitted': dt[6],
                                  'review': dt[8],
                                  'curation': dt[7],
                                  'public': dt[9],
                                  'dormant': dt[10],
                                  'affiliation': dt[3],
                                  'user_status': dt[4],
                                  }
                             }
                data = {**data, **dict_temp}
                user_count += 1
                if dt[4] == 2 :
                    active_user += 1
            res = {"created_at": "2020-07-07", "updated_at": datetime.today().strftime('%Y-%m-%d'), 'user_count' : user_count, 'active_user': active_user, 'data': data}
            # j = json.dumps(res)
            return jsonify(res)
