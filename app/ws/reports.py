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
from app.ws.study_files import get_all_files
from app.ws.utils import log_request, writeDataToFile, readDatafromFile, clean_json, get_techniques, get_studytype, \
    get_instruments_organism

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
    def get(self):
        global start_date, query_field
        global end_date
        log_request(request)
        parser = reqparse.RequestParser()

        parser.add_argument('query', help='Report query')
        query = None
        if request.args:
            args = parser.parse_args(req=request)
            query = args['query']
            if query:
                query = query.strip()

        parser.add_argument('start', help='start date')
        if request.args:
            args = parser.parse_args(req=request)
            start = args['start']
            if start:
                start_date = datetime.strptime(start, '%Y%m%d')
            else:
                start_date = datetime.strptime('20110809', '%Y%m%d')

        parser.add_argument('end', help='end date')
        if request.args:
            args = parser.parse_args(req=request)
            end = args['end']
            if end:
                end_date = datetime.strptime(end, '%Y%m%d')
            else:
                end_date = datetime.today()

        parser.add_argument('queryFields', help='queryFields')
        if request.args:
            args = parser.parse_args(req=request)
            queryFields = args['queryFields']
            if queryFields:
                query_field = tuple([x.strip() for x in queryFields.split(',')])
            else:
                query_field = None

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

        reporting_path = app.config.get('MTBLS_FTP_ROOT') + app.config.get('REPORTING_PATH') + 'global/'

        if query == 'daily_stats':
            file_name = 'daily_report.json'
            j_file = readDatafromFile(reporting_path + file_name)

            data_res = {}
            for date, report in j_file['data'].items():
                d = datetime.strptime(date, '%Y-%m-%d')
                if d >= start_date and d <= end_date:
                    if query_field != None:
                        slim_report = {k: report[k] for k in query_field}
                        data_res.update({date: slim_report})
                    else:
                        data_res.update({date: report})
                else:
                    continue
            j_file['data'] = data_res
            return jsonify(j_file)

        elif query == 'user_stats':
            file_name = 'user_report.json'
            j_file = readDatafromFile(reporting_path + file_name)
            return jsonify(j_file)
        else:
            file_name = ''
            abort(404)

    # =========================== POST =============================================

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
                "enum": ["daily_stats", "user_stats", "study_stats", "global"]
            },
            {
                "name": "studyid",
                "description": "None to update all studies",
                "required": False,
                "allowEmptyValue": True,
                "paramType": "query",
                "dataType": "string"
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

        # query field
        parser.add_argument('query', help='Report query')
        query = None
        if request.args:
            args = parser.parse_args(req=request)
            query = args['query']
            if query:
                query = query.strip()

        # study ID
        parser.add_argument('studyid', help='Study ID')
        studyid = None
        if request.args:
            args = parser.parse_args(req=request)
            studyid = args['studyid']
            if studyid:
                studyid = studyid.strip().upper()

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

        reporting_path = app.config.get('MTBLS_FTP_ROOT') + app.config.get('REPORTING_PATH') + 'global/'
        file_name = ''
        res = ''

        if query == 'daily_stats':
            try:
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
                file_name = 'daily_report.json'
            except Exception as e:
                logger.info(e)
                print(e)

        if query == 'user_stats':
            # try:
            file_name = 'study_report.json'
            study_data = readDatafromFile(reporting_path + file_name)
            sql = open('./instance/user_report.sql', 'r').read()
            postgresql_pool, conn, cursor = get_connection()
            cursor.execute(sql)
            result = cursor.fetchall()
            data = {}
            user_count = 0
            active_user = 0
            for dt in result:
                study_list = dt[6].split(",")
                studies = {}
                for x in study_list:
                    temp = study_data[x.strip()]
                    studies[x.strip()] = temp
                dict_temp = {str(dt[0]):
                                 {"user_email": str(dt[1]),
                                  "country_code": dt[2],
                                  "total": str(dt[5]),
                                  "submitted": str(dt[7]),
                                  "review": str(dt[9]),
                                  "curation": str(dt[8]),
                                  "public": str(dt[10]),
                                  "dormant": str(dt[11]),
                                  "affiliation": dt[3],
                                  "user_status": str(dt[4]),
                                  "studies": studies,
                                  }
                             }
                data = {**data, **dict_temp}
                user_count += 1
                if dt[4] == 2:
                    active_user += 1
                data['user_count'] = str(user_count)
                data['active_user'] = str(active_user)
            res = {"created_at": "2020-07-07", "updated_at": datetime.today().strftime('%Y-%m-%d'),
                   "data": data}

            file_name = 'user_report.json'

        if query == 'study_stats':
            postgresql_pool, conn, cursor = get_connection()
            cursor.execute(
                "select acc from studies")
            studies = cursor.fetchall()
            data = {}
            for st in studies:
                study_files, latest_update_time = get_all_files(
                    app.config.get('STUDY_PATH') + str(st[0]))
                dict_temp = {str(st[0]):
                                 {'latest_update_time': latest_update_time,
                                  'study_files': study_files
                                  }
                             }
                data = {**data, **dict_temp}
            file_name = 'study_report.json'
            res = data

        if query == 'global':
            file_name = 'global.json'
            j_data = readDatafromFile(reporting_path + file_name)

            # load global.json and update
            if studyid:
                studyid = studyid.upper()
                # load global.json and clean the date set
                j_data = clean_json(j_data, studyid)

                # techniques
                res1 = get_techniques(studyID=studyid)
                for tech, value in res1['techniques'].items():
                    if tech in j_data['data']['techniques']:
                        print(tech)
                        j_data['data']['techniques'][tech] += value  # res['techniques'][tech]
                    else:
                        j_data['data']['techniques'].update({tech: value})

                # study_type
                res2 = get_studytype(studyID=studyid)
                j_data['data']['study_type']['targeted'] += res2['study_type']['targeted']
                j_data['data']['study_type']['untargeted'] += res2['study_type']['untargeted']
                j_data['data']['study_type']['targeted_untargeted'] += res2['study_type']['targeted_untargeted']

                # instruments & organisms
                ins, org = get_instruments_organism(studyID=studyid)
                for i, value in ins['instruments'].items():
                    if i not in j_data['data']['instruments']:
                        j_data['data']['instruments'].update({i: value})
                    else:
                        for studies, v in ins['instruments'][i].items():
                            j_data['data']['instruments'][i].update({studies: v})

                # organisms
                for o, org_part in org['organisms'].items():
                    if o not in j_data['data']['organisms']:
                        j_data['data']['organisms'].update({o: org_part})
                    else:
                        for org_p, studies in org_part.items():
                            if org_p not in j_data['data']['organisms'][o]:
                                j_data['data']['organisms'][o].update({org_p: studies})
                            else:
                                j_data['data']['organisms'][o][org_p] += studies

            # generate new global file
            else:
                # techniques
                techs = get_techniques()
                j_data['data']['techniques'] = techs

                # study_type
                types = get_studytype()
                j_data['data']['study_type'] = types

                # instruments & organisms
                i, s = get_instruments_organism()
                j_data['data']['instruments'] = i
                j_data['data']['organisms'] = s

                j_data["updated_at"] = datetime.today().strftime('%Y-%m-%d')

                res = j_data

        # j_res = json.dumps(res,indent=4)
        writeDataToFile(reporting_path + file_name, res, True)
        return jsonify(res)