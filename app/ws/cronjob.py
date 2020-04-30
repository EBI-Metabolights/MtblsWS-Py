#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Jan-17
#  Modified by:   Jiakang
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

import re
import traceback

import gspread
import numpy as np
import psycopg2
import requests
from flask import jsonify
from flask import request, abort
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

from app.ws.mtblsWSclient import WsClient
from app.ws.ontology_info import *
from app.ws.utils import log_request

logger = logging.getLogger('wslog')
wsc = WsClient()


class cronjob(Resource):
    @swagger.operation(
        summary="Update Google sheets for MetaboLights study curation and statistics",
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
                "name": "source",
                "description": "update source",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["curation log-Database Query", "curation log-Database update", "MTBLS statistics"]
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
                "message": "Unauthorized. Access to the resource requires user authentication. "
                           "Please provide a study id and a valid user token"
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token"
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

        parser.add_argument('source', help='source to update')
        source = None
        if request.args:
            args = parser.parse_args(req=request)
            source = args['source']
            if source:
                source = source.strip()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            abort(401)

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions('MTBLS1', user_token)
        if not is_curator:
            abort(403)

        if source == 'curation log-Database Query':
            try:
                logger.info('Updating curation log-Database Query')
                curation_log_database_query()
                return jsonify({'success': True})
            except Exception as e:
                logger.info(e)
                print(e)
        elif source == 'curation log-Database update':
            try:
                logger.info('Updating curation log-Database update')
                curation_log_database_update()
                return jsonify({'success': True})
            except Exception as e:
                logger.info(e)
                print(e)
        elif source == 'MTBLS statistics':
            try:
                logger.info('Updating MTBLS statistics')
                MTBLS_statistics_update()
                return jsonify({'success': True})
            except Exception as e:
                logger.info(e)
                print(e)
        else:
            abort(400)


def curation_log_database_query():
    try:
        params = app.config.get('DB_PARAMS')

        with psycopg2.connect(**params) as conn:
            sql = open('./resources/updateDB.sql', 'r').read()
            data = pd.read_sql_query(sql, conn)

        data['percentage known'] = round(
            data['maf_known'].astype('float').div(data['maf_rows'].replace(0, np.nan)).fillna(0) * 100, 2)

        token = app.config.get('GOOGLE_SHEET_TOKEN')

        google_df = getGoogleSheet(app.config.get('MTBLS_CURATION_LOG'), 'Database Query',
                                   app.config.get('GOOGLE_SHEET_TOKEN'))

        data.columns = google_df.columns

        replaceGoogleSheet(data, app.config.get('MTBLS_CURATION_LOG'), 'Database Query',
                           app.config.get('GOOGLE_SHEET_TOKEN'))

    except Exception as e:
        print(e)
        logger.info(e)


def curation_log_database_update():
    def execute_query(query):
        try:
            params = app.config.get('DB_PARAMS')
            conn = psycopg2.connect(**params)
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
            conn.close()

        except psycopg2.Error as e:
            print("Unable to connect to the database")
            logger.info("Unable to connect to the database")
            print(e.pgcode)
            logger.info(e.pgcode)
            print(e.pgerror)
            logger.info(e.pgcode)

    google_df = getGoogleSheet(app.config.get('MTBLS_CURATION_LOG'), 'Database update',
                               app.config.get('GOOGLE_SHEET_TOKEN'))

    sql = ''.join(google_df['--Updates. Run this in the database on a regular basis'].tolist())
    execute_query(sql)
    print('Done')


def MTBLS_statistics_update():
    ## update untarget NMR
    untarget_NMR = extractUntargetStudy(['NMR'])
    res = untarget_NMR[['studyID']]
    replaceGoogleSheet(df=res, url=app.config.get('MTBLS_STATISITC'), worksheetName='untarget NMR',
                       token_path=app.config.get('GOOGLE_SHEET_TOKEN'))

    ## update untarget LC-MS
    untarget_LCMS = extractUntargetStudy(['LC'])
    res = untarget_LCMS[['studyID']]
    replaceGoogleSheet(df=res, url=app.config.get('MTBLS_STATISITC'), worksheetName='untarget LC-MS',
                       token_path=app.config.get('GOOGLE_SHEET_TOKEN'))

    ## update NMR and LC-MS
    studyID, studyType = getStudytype(['LC', 'NMR'], publicStudy=False)
    df = pd.DataFrame(columns=['studyID', 'dataType'])
    df.studyID, df.dataType = studyID, studyType
    replaceGoogleSheet(df=df, url=app.config.get('MTBLS_STATISITC'), worksheetName='both NMR and LCMS',
                       token_path=app.config.get('GOOGLE_SHEET_TOKEN'))


def extractUntargetStudy(studyType, public=True):
    def extractNum(s):
        num = re.findall("\d+", s)[0]
        return int(num)

    def getDescriptor(publicStudy=True, sIDs=None):
        res = []
        if sIDs == None:
            studyIDs = getStudyIDs(publicStudy=publicStudy)
        else:
            studyIDs = sIDs

        for studyID in studyIDs:
            url = 'https://www.ebi.ac.uk/metabolights/ws/studies/{study_id}/descriptors'.format(study_id=studyID)
            try:
                resp = requests.get(url, headers={'user_token': app.config.get('METABOLIGHTS_TOKEN')})
                data = resp.json()
                for descriptor in data['studyDesignDescriptors']:
                    temp_dict = {'studyID': studyID,
                                 'term': descriptor['annotationValue'],
                                 'matched_iri': descriptor['termAccession']}
                    res.append(temp_dict)
            except Exception as e:
                print(studyID, end='\t')
                print(e.args)

        df = pd.DataFrame(res)
        # df.to_csv('../tests/descriptor.tsv', sep='\t', index=False)
        return df

    studyIDs, _ = getStudytype(studyType, publicStudy=public)
    descripter = getDescriptor(sIDs=studyIDs)
    untarget = descripter.loc[descripter['term'].str.startswith(('untargeted', 'Untargeted', 'non-targeted'))]
    untarget_df = untarget.copy()
    untarget_df['num'] = untarget_df['studyID'].apply(extractNum)
    untarget_df = untarget_df.sort_values(by=['num'])
    untarget_df = untarget_df.drop('num', axis=1)

    return untarget_df


def getStudyIDs(publicStudy=False):
    def atoi(text):
        return int(text) if text.isdigit() else text

    def natural_keys(text):
        return [atoi(c) for c in re.split('(\d+)', text)]

    url = 'https://www.ebi.ac.uk/metabolights/webservice/study/list'
    request = urllib.request.Request(url)
    request.add_header('user_token', app.config.get('METABOLIGHTS_TOKEN'))
    response = urllib.request.urlopen(request)
    content = response.read().decode('utf-8')
    j_content = json.loads(content)

    studyIDs = j_content['content']
    studyIDs.sort(key=natural_keys)

    if publicStudy:
        studyStatus = getStudyStatus()
        s = studyStatus['MTBLS230']
        studyIDs = [studyID for studyID in studyIDs if studyStatus[studyID] in ['Public', 'In Review']]
    else:
        studyStatus = getStudyStatus()
        studyIDs = [studyID for studyID in studyIDs if
                    studyStatus[studyID] in ['Public', 'In Review', 'Submitted', 'In Curation']]
    return studyIDs


def getStudyStatus():
    query_user_access_rights = """
             select case 
             when (status = 0 and placeholder = '1') then 'Placeholder' 
             when (status = 0 and placeholder='') then 'Submitted' 
             when status = 1 then 'In Curation' 
             when status = 2 then 'In Review' 
             when status = 3 then 'Public' 
             else 'Dormant' end as status, 
                acc from studies;
            """
    token = app.config.get('METABOLIGHTS_TOKEN')

    def execute_query(query, user_token, study_id=None):
        try:
            params = app.config.get('DB_PARAMS')
            conn = psycopg2.connect(**params)
            cursor = conn.cursor()
            query = query.replace('\\', '')
            if study_id is None:
                cursor.execute(query, [user_token])
            else:
                query2 = query_user_access_rights.replace("#user_token#", user_token)
                query2 = query2.replace("#study_id#", study_id)
                cursor.execute(query2)
            data = cursor.fetchall()
            conn.close()

            return data

        except psycopg2.Error as e:
            print("Unable to connect to the database")
            print(e.pgcode)
            print(e.pgerror)
            print(traceback.format_exc())

    study_list = execute_query(query_user_access_rights, token)
    study_status = {}
    for study in study_list:
        study_status[study[1]] = study[0]

    return study_status


def getStudytype(sType, publicStudy=True):
    def get_connection():
        postgresql_pool = None
        conn = None
        cursor = None
        try:
            params = app.config.get('DB_PARAMS')
            conn_pool_min = app.config.get('CONN_POOL_MIN')
            conn_pool_max = app.config.get('CONN_POOL_MAX')
            postgresql_pool = psycopg2.pool.SimpleConnectionPool(conn_pool_min, conn_pool_max, **params)
            conn = postgresql_pool.getconn()
            cursor = conn.cursor()
        except Exception as e:
            print("Could not query the database " + str(e))
            if postgresql_pool:
                postgresql_pool.closeall
        return postgresql_pool, conn, cursor

    q2 = ' '
    if publicStudy:
        q2 = ' status in (2, 3) and '

    if type(sType) == str:
        q3 = "studytype = '{sType}'".format(sType=sType)

    # fuzzy search
    elif type(sType) == list:
        DB_query = []
        for q in sType:
            query = "studytype like '%{q}%'".format(q=q)
            DB_query.append(query)
        q3 = ' and '.join(DB_query)

    else:
        return None

    query = "SELECT acc,studytype FROM studies WHERE {q2} {q3};".format(q2=q2, q3=q3)
    # query = q1 + q2 + q3 + ';'
    print(query)
    postgresql_pool, conn, cursor = get_connection()
    cursor.execute(query)
    res = cursor.fetchall()
    studyID = [r[0] for r in res]
    studytype = [r[1] for r in res]
    return studyID, studytype


def setGoogleSheet(df, url, worksheetName, token_path):
    '''
    set whole dataframe to google sheet, if sheet existed create a new one
    :param df: dataframe want to save to google sheet
    :param url: url of google sheet
    :param worksheetName: worksheet name
    :return: Nan
    '''
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(token_path, scope)
    gc = gspread.authorize(credentials)
    try:
        wks = gc.open_by_url(url).worksheet(worksheetName)
        print(worksheetName + ' existed... create a new one')
        wks = gc.open_by_url(url).add_worksheet(title=worksheetName + '_1', rows=df.shape[0], cols=df.shape[1])
    except:
        wks = gc.open_by_url(url).add_worksheet(title=worksheetName, rows=df.shape[0], cols=df.shape[1])
    set_with_dataframe(wks, df)


def getGoogleSheet(url, worksheetName, token_path):
    '''
    get google sheet
    :param url: url of google sheet
    :param worksheetName: work sheet name
    :return: data frame
    '''
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(token_path, scope)
    gc = gspread.authorize(credentials)
    # wks = gc.open('Zooma terms').worksheet('temp')
    wks = gc.open_by_url(url).worksheet(worksheetName)
    content = wks.get_all_records()
    # max_rows = len(wks.get_all_values())
    df = pd.DataFrame(content)
    return df


def replaceGoogleSheet(df, url, worksheetName, token_path):
    '''
    replace the old google sheet with new data frame, old sheet will be clear
    :param df: dataframe
    :param url: url of google sheet
    :param worksheetName: work sheet name
    :return: Nan
    '''
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(token_path, scope)
    gc = gspread.authorize(credentials)
    wks = gc.open_by_url(url).worksheet(worksheetName)
    wks.clear()
    set_with_dataframe(wks, df)
