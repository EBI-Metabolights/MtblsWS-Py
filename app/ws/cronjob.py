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

import logging

import psycopg2

from app.ws.mtblsWSclient import WsClient

logger = logging.getLogger('wslog')
wsc = WsClient()

import gspread
import numpy as np
from flask import request, abort
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.ontology_info import *
from app.ws.utils import log_request

logger = logging.getLogger('wslog')
iac = IsaApiClient()
wsc = WsClient()


class cronjob(Resource):
    @swagger.operation(
        summary="Create (or update) Google Calendar entries for MetaboLights study curation (curator only)",
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
                "enum": ["curation log-Database Query", "curation log-Database update"]
            }

        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
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
                curation_log_database_query()
            except Exception as e:
                logger.info(e)
                print(e)
        elif source == 'curation log-Database update':
            try:
                curation_log_database_update()
            except Exception as e:
                logger.info(e)
                print(e)


def curation_log_database_query():
    params = app.config.get('DB_PARAMS')

    with psycopg2.connect(**params) as conn:
        sql = open('./instance/updateDB.sql', 'r').read()
        data = pd.read_sql_query(sql, conn)

    data['percentage known'] = round(
        data['maf_known'].astype('float').div(data['maf_rows'].replace(0, np.nan)).fillna(0) * 100, 2)

    token = app.config.get('GOOGLE_SHEET_TOKEN')

    google_df = getGoogleSheet(app.config.get('MTBLS_CURATION_LOG'), 'Database Query',
                               app.config.get('GOOGLE_SHEET_TOKEN'))

    data.columns = google_df.columns

    replaceGoogleSheet(data, app.config.get('MTBLS_CURATION_LOG'), 'Database Query',
                       app.config.get('GOOGLE_SHEET_TOKEN'))


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
