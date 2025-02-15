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
import logging
import os
import zipfile
from datetime import datetime

from flask import request, jsonify
from flask_restful import Resource, reqparse, abort
from flask_restful_swagger import swagger
from app.config import get_settings

from app.study_folder_utils import convert_relative_to_real_path
from app.ws.db_connection import get_connection, get_study
from app.ws.isaApiClient import IsaApiClient
from app.ws.misc_utilities.request_parsers import RequestParsers
from app.ws.mtblsWSclient import WsClient
from app.ws.report_builders.analytical_method_builder import AnalyticalMethodBuilder
from app.ws.report_builders.europe_pmc_builder import EuropePmcReportBuilder
from app.ws.study.folder_utils import get_all_files
from app.ws.utils import log_request, writeDataToFile, readDatafromFile, clean_json, get_techniques, get_studytype, \
    get_instruments_organism

logger = logging.getLogger('wslog')
iac = IsaApiClient()
wsc = WsClient()

class StudyAssayTypeReports(Resource):

    @swagger.operation(
        summary="POST Metabolights study assay type report",
        notes='POST Metabolights report for a specific study type. This requires a globals.json file to have previously'
              ' been generated. To generate this globals.json file, hit the /v2/reports endpoint with query type global.'
              ' This resource does not return the report itself. It creates a new file in the reporting directory under the name '
              'of {study_type}.csv. Any previous reports of the same study type will be overwritten.',

        parameters=[

            {
                "name": "studytype",
                "description": "Which type of study IE NMR to generate the report for",
                "required": True,
                "allowEmptyValue": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "slim",
                "description": "Whether to generate a slim version of the file",
                "required": True,
                "paramType": "query",
                "dataType": "Boolean",
                "allowMultiple": False,
                "default": False
            },
            {
                "name": "verbose",
                "description": "Whether to give a verbose output of the performance of the builder",
                "required": True,
                "paramType": "query",
                "dataType": "Boolean",
                "allowMultiple": False,
                "default": False
            },
            {
                "name": "drive",
                "description": "Whether to save the output file to google drive.",
                "required": False,
                "paramType": "query",
                "dataType": "Boolean",
                "allowMultiple": False,
                "default": False
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

        parser = RequestParsers.study_type_report_parser()
        studytype = None

        args = parser.parse_args(req=request)
        studytype = args['studytype']
        slim = args['slim']
        verbose = args['verbose']
        drive = args['drive']

        if studytype:
            studytype = studytype.strip()
        else:
            abort(400)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            abort(401)

        wsc = WsClient()

        is_curator, __, __, __, study_location, __, __, __ = wsc.get_permissions('MTBLS1', user_token)
        if is_curator is False:
            abort(413)

        reporting_path = os.path.join(get_settings().study.mounted_paths.reports_root_path, 
                                      get_settings().report.report_base_folder_name, 
                                      get_settings().report.report_global_folder_name)
        msg = AnalyticalMethodBuilder(
            original_study_location=study_location,
            studytype=studytype,
            slim=slim,
            reporting_path=reporting_path,
            verbose=verbose,
            g_drive=drive
        ).build()

        logger.info(msg)
        return jsonify({'message': msg})

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
                "enum": ["daily_stats", "user_stats", "global", 'study_status', "file_extension"]
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
                "description": "Specify the fields to return",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },

            {
                "name": "studyStatus",
                "description": "Specify the study status, the default is all options: 'Public', 'In Review', 'Private', 'Provisional', 'Placeholder', 'Dormant'",
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

        parser.add_argument('studyStatus', help='studyStatus')
        studyStatus = None
        if request.args:
            args = parser.parse_args(req=request)
            studyStatus = args['studyStatus']
            if studyStatus:
                studyStatus = tuple([x.strip() for x in studyStatus.split(',')])

        parser.add_argument('queryFields', help='queryFields')
        query_field = None
        if request.args:
            args = parser.parse_args(req=request)
            queryFields = args['queryFields']
            if queryFields:
                query_field = tuple([x.strip().lower() for x in queryFields.split(',')])

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
            if query in ['study_status', "global"]:
                studyStatus = ['public']
            else:
                abort(403)

        reporting_path = os.path.join(get_settings().study.mounted_paths.reports_root_path, 
                                      get_settings().report.report_base_folder_name, 
                                      get_settings().report.report_global_folder_name)
        if query == 'daily_stats':
            file_name = 'daily_report.json'
            j_file = readDatafromFile(os.path.join(reporting_path, file_name))

            data_res = {}
            for date, report in j_file['data'].items():
                d = datetime.strptime(date, '%Y-%m-%d')
                if d >= start_date and d <= end_date:
                    if query_field:
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
            j_file = readDatafromFile(os.path.join(reporting_path, file_name))
            return jsonify(j_file)

        elif query == 'global':
            file_name = 'global.json'
            j_file = readDatafromFile(os.path.join(reporting_path, file_name))
            return jsonify(j_file)

        elif query == 'file_extension':
            file_name = 'file_extension.json'
            j_file = readDatafromFile(os.path.join(reporting_path, file_name))
            return jsonify(j_file)

        elif query == 'study_status':
            file_name = 'study_report.json'
            j_file = readDatafromFile(os.path.join(reporting_path, file_name))
            data_res = {}

            for studyID, study_info in j_file['data'].items():
                d = datetime.strptime(study_info['submissiondate'], '%Y-%m-%d')
                status = study_info['status']

                if not studyStatus:
                    if d >= start_date and d <= end_date:
                        data_res.update({studyID: study_info})
                    else:
                        continue
                else:
                    if d >= start_date and d <= end_date and status.lower() in studyStatus:
                        data_res.update({studyID: study_info})
                    else:
                        continue

            j_file['data'] = data_res
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
                "enum": ["daily_stats", "user_stats", "study_stats", "file_extension", "global"]
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

        reporting_path = os.path.join(get_settings().study.mounted_paths.reports_root_path, 
                                      get_settings().report.report_base_folder_name, 
                                      get_settings().report.report_global_folder_name)
        file_name = ''
        res = ''

        if query == 'daily_stats':
            try:
                sql = open(convert_relative_to_real_path('resources/study_report.sql'), 'r').read()
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
            study_data = readDatafromFile(os.path.join(reporting_path,  file_name))
            sql = open(convert_relative_to_real_path('resources/user_report.sql'), 'r').read()
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
                    try:
                        temp = study_data['data'][x.strip()]
                        studies[x.strip()] = temp
                    except:
                        continue
                dict_temp = {str(dt[0]):
                                 {
                                  "user_email": str(dt[1]),
                                  "country_code": dt[2],
                                  "total": str(dt[5]),
                                  "provisional": str(dt[7]),
                                  "review": str(dt[9]),
                                  "private": str(dt[8]),
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
                # data['user_count'] = str(user_count)
                # data['active_user'] = str(active_user)
            res = {"created_at": "2020-07-07", "updated_at": datetime.today().strftime('%Y-%m-%d'),
                   "user_count": str(user_count), "active_user": str(active_user),
                   "data": data}

            file_name = 'user_report.json'

        if query == 'study_stats':
            postgresql_pool, conn, cursor = get_connection()
            cursor.execute(
                "select acc from studies")
            studies = cursor.fetchall()
            data = {}
            for st in studies:
                print(st[0])
                folder_path = os.path.join(get_settings().study.mounted_paths.study_metadata_files_root_path, str(st[0]))
                study_files, latest_update_time = get_all_files(folder_path)

                study_info = get_study(st[0])
                name = study_info.pop('submitter').split(',')
                country = study_info.pop('country').split(',')

                name_d = [{'name': x} for x in name]
                country_d = [{'country': x} for x in country]
                submitter = []
                for x in zip(name_d, country_d):
                    res = {}
                    for y in x:
                        res.update(y)
                    submitter.append(res)

                study_info['submitter'] = submitter
                study_info['latest_update_time'] = latest_update_time
                study_info['study_files'] = study_files

                dict_temp = {str(st[0]): study_info}
                data = {**data, **dict_temp}
            file_name = 'study_report.json'

            res = {'data': data}
            res["updated_at"] = datetime.today().strftime('%Y-%m-%d')

        if query == 'global':
            file_name = 'global.json'
            j_data = readDatafromFile(os.path.join(reporting_path,  file_name))

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
                j_data['data']['techniques'] = techs['techniques']

                # study_type
                types = get_studytype()
                j_data['data']['study_type'] = types['study_type']

                # instruments & organisms
                i, s = get_instruments_organism()
                j_data['data']['instruments'] = i['instruments']
                j_data['data']['organisms'] = s['organisms']

                j_data["updated_at"] = datetime.today().strftime('%Y-%m-%d')

            res = j_data

        if query == 'file_extension':
            file_name = 'file_extension.json'

            postgresql_pool, conn, cursor = get_connection()
            cursor.execute(
                "select acc from studies where status = 3;")
            studies = cursor.fetchall()
            file_ext = []

            for studyID in studies:
                print(studyID[0])
                logger.info("Extracting study extension details: " + studyID[0])
                wd = os.path.join(get_settings().study.mounted_paths.study_metadata_files_root_path, studyID[0])

                try:
                    file_ext.append(get_file_extensions(studyID[0], wd))
                except:
                    print("Error extracting study extension details: " + studyID[0])

            res = {"created_at": "2020-03-22", "updated_at": datetime.today().strftime('%Y-%m-%d'), 'data': file_ext}

        # j_res = json.dumps(res,indent=4)
        writeDataToFile(os.path.join(reporting_path, file_name), res, True)

        return jsonify({"POST " + file_name: True})


class CrossReferencePublicationInformation(Resource):

    @swagger.operation(
        summary="GET Metabolights cross referenced with europepmc report (Curator Only)",
        notes='GET report that checks the publication information provided to Metabolights by our submitters against '
              'EuropePMC, highlighting any discrepancies. Will fail if user token is not curator level.',

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
                "name": "google_drive",
                "description": "Save the report to google drive instead of the virtual machine?",
                "paramType": "query",
                "type": "Boolean",
                "required": False,
            }]
    )
    def get(self):
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            # user token is required
            abort(401)
        parser = RequestParsers.europepmc_report_parser()
        args = parser.parse_args(request)
        logger.info('ARGS ' + str(args))
        drive = False
        if 'google_drive' in args:
            if args['google_drive'] == 'true':
                drive = True

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions('MTBLS1', user_token)
        if not is_curator:
            abort(403)
        priv_list = wsc.get_non_public_studies()['content']

        msg = EuropePmcReportBuilder(priv_list, user_token, wsc, iac).build(drive)
        if msg.count('Problem') == 1:
            abort(500, message=msg)

        return 200, msg



def get_file_extensions(id, path):
    study_ext = {}
    study_ext['list'] = []
    study_ext['ext_count'] = {}
    for root, dirs, files in os.walk(path):
        for file in files:
            try:
                extension = os.path.splitext(file)[1]
                if extension:
                    if extension not in study_ext['list']:
                        study_ext['list'].append(extension)
                    if extension == '.zip':
                        edata = extractZip(path, file, study_ext['list'], study_ext['ext_count'])
                        study_ext['list'] = edata[0]
                        study_ext['ext_count'] = edata[1]
                    if extension in study_ext['ext_count']:
                        study_ext['ext_count'][extension] = study_ext['ext_count'][extension] + 1
                    else:
                        study_ext['ext_count'][extension] = 1
            except:
                logger.error("Error file details: " + file)

    extensions = study_ext['list']
    extensions_count = study_ext['ext_count']
    study_ext = {'id': id}
    study_ext['extensions'] = extensions
    study_ext['extensions_count'] = extensions_count
    return study_ext


def extractZip(filepath, file, list, count):
    zfile = zipfile.ZipFile(os.path.join(filepath, file))
    for finfo in zfile.infolist():
        extension = os.path.splitext(finfo.filename)[1]
        if extension:
            if extension not in list:
                list.append(extension)
            if extension in count:
                count[extension] = count[extension] + 1
            else:
                count[extension] = 1
    return [list, count]
