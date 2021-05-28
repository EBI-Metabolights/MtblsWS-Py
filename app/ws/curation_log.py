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

from app.ws.cronjob import getGoogleSheet, getCellCoordinate, getWorksheet, update_cell
from app.ws.db_connection import *
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import log_request
from flask import jsonify

logger = logging.getLogger('wslog')
wsc = WsClient()


class curation_log(Resource):
    @swagger.operation(
        summary="Get Metabolights periodic report",
        notes='''Update curation log.
              <br>
              <pre><code>
{
  "MTBLS1": {
    "Study Type": "UPLC-QTOF-MS",
    "Species": "Arabidopsis thaliana"
  },
  "MTBLS2": {
    "Study Type": "LC-MS",
    "Species": "Homo Sapiens",
    "Study Publication Date": "2015-02-14"
  }
}</code></pre>''',
        parameters=[
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "data",
                "description": "update field in JSON format.",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
        log_request(request)
        parser = reqparse.RequestParser()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            abort(401)

        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions('MTBLS1', user_token)

        user_name = get_username_by_token(user_token)

        if not write_access:
            abort(403)

        # loading data
        data_dict = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
        except Exception as e:
            logger.info(e)
            abort(400)
        if not data_dict:
            abort(403)

        try:
            wks = getWorksheet(app.config.get('MTBLS_CURATION_LOG'), 'Studies',
                               app.config.get('GOOGLE_SHEET_TOKEN'))
        except Exception as e:
            logger.info('Fail to load worksheet.', e)
            print('Fail to load worksheet.', e)
            abort(400)
            return []

        output = {'success':[],'un_success':[]}


        editable_columns = ['Study Type', 'Species', 'Place Holder', 'Assigned to']
        for studyID, fields in data_dict.items():
            try:
                r = wks.find(studyID).row
                # r, _ = getCellCoordinate(app.config.get('MTBLS_CURATION_LOG'), 'Studies',
                #                          app.config.get('GOOGLE_SHEET_TOKEN'), studyID)
            except:
                logger.info('Can find {studyID} in curation log'.format(studyID=studyID))
                print('Can find {studyID} in curation log'.format(studyID=studyID))
                continue

            for field, value in fields.items():
                if field in editable_columns:
                    c = wks.find(field).col
                    # _, c = getCellCoordinate(app.config.get('MTBLS_CURATION_LOG'), 'Studies',
                    #                          app.config.get('GOOGLE_SHEET_TOKEN'), field)
                    if update_cell(wks, r, c, value):
                        output['success'].append("{user_name} updated {studyID} - {field} to {value}".format(user_name=user_name,
                                                                                                  studyID=studyID,
                                                                                                  field=field,
                                                                                                  value=value))
                        logger.info(
                            "{user_name} updated {studyID} - {field} to {value}".format(user_name=user_name,
                                                                                        studyID=studyID, field=field,
                                                                                        value=value))
                        print(
                            "{user_name} updated {studyID} - {field} to {value}".format(user_name=user_name,
                                                                                        studyID=studyID, field=field,
                                                                                        value=value))
                else:
                    logger.info('Permission denied modify {studyID} {field}'.format(studyID=studyID, field=field))
                    print('Permission denied modify {studyID} {field}'.format(studyID=studyID, field=field))
                    output['un_success'].append('Permission denied modify {studyID} {field}'.format(studyID=studyID, field=field))
                    continue

        return jsonify(output)

    # ============================= GET ==========================================

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
