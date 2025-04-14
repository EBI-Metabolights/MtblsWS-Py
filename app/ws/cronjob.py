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


import datetime
import json
import logging
import os
import re
import urllib
from datetime import datetime

import gspread
import numpy as np
import pandas as pd
import psycopg2
import requests
from flask import jsonify, request
from flask_restful import Resource, reqparse, abort
from flask_restful_swagger import swagger
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from owlready2 import urllib
from app.config import get_settings
from app.config.utils import get_host_internal_url

from app.services.storage_service.acl import Acl
from app.services.storage_service.storage_service import StorageService
from app.study_folder_utils import convert_relative_to_real_path
from app.ws.db.dbmanager import DBManager
from app.ws.db.schemes import Study
from app.ws.db.types import StudyStatus
from app.ws.db_connection import get_study_info, get_study_by_type, get_public_studies
from app.ws.misc_utilities.dataframe_utils import DataFrameUtils
from app.ws.mtblsWSclient import WsClient
from app.ws.study.commons import create_ftp_folder
from app.ws.study.user_service import UserService
from app.ws.utils import log_request, writeDataToFile

logger = logging.getLogger('wslog')
wsc = WsClient()


class cronjob(Resource):
    @swagger.operation(
        summary="Update Google sheets for MetaboLights study curation and statistics",
        parameters=[
            {
                "name": "user-token",
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
                "enum": ["curation log-Database Query", "curation log-Database update", "MTBLS statistics",
                         "empty studies", "MARIANA study_classify", "ftp file permission", "test cronjob"]
            },
            {
                "name": "starting_index",
                "description": "Starting index in the study list",
                "required": False,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "int"
            },
            {
                "name": "ending_index",
                "description": "Ending index in the study list",
                "required": False,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "int"
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
        

        
        source = None
        if request.args:
            
            source = request.args.get('source')
            if source:
                source = source.strip()

        
        
        starting_index = None
        ending_index = None
        if request.args:
            
            starting_index = request.args.get('starting_index')
            ending_index = request.args.get('ending_index')

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
            logger.info('Updating curation log-Database Query')
            return curation_log_database_query()
        elif source == 'curation log-Database update':
            logger.info('Updating curation log-Database Update')
            return curation_log_database_update(starting_index, ending_index)
                
        elif source == 'MTBLS statistics':
            try:
                logger.info('Updating MTBLS statistics')
                MTBLS_statistics_update()
                return jsonify({'success': True})
            except Exception as e:
                logger.info(e)
                print(e)

        elif source == 'empty studies':
            try:
                logger.info('Get list of empty studies')
                blank_inv, no_inv = get_empty_studies()

                return jsonify({'Investigation files check':
                    {
                        'Empty investigation': {'counts': len(blank_inv), 'list': blank_inv},
                        'Missing investigation': {'counts': len(no_inv), 'list': no_inv}
                    }
                })
            except Exception as e:
                logger.info(e)
                print(e)
        elif source == 'MARIANA study_classify':
            data = {'data': {**untarget_NMR(), **untarget_LCMS(), **NMR_and_LCMS()}}
            time_stamp = {"created_at": "2020-07-20", "updated_at": datetime.today().strftime('%Y-%m-%d')}
            res = {**time_stamp, **data}
            file_name = 'study_classify.json'
            file_path = os.path.join(get_settings().study.mounted_paths.reports_root_path, 
                                     get_settings().report.mariana_report_folder_name)
            writeDataToFile(file_path + file_name, res, True)
            return jsonify(res)
        elif source == 'ftp file permission':
            submit, curation, review, public = file_permission()
            if len(submit) + len(curation) + len(review) == 0:
                return jsonify({'result': 'Nothing to change'})
            else:
                res = {"Change ftp folder access permission": {'Submission studies (770)': submit,
                                                               'In curation studies (750)': curation,
                                                               'In review studies (550)': review,
                                                               'Public studies (550)': public}}
                return jsonify(res)
        elif source == 'test cronjob':
            pass
        else:
            abort(400)


def curation_log_database_query():
    try:
        settings = get_settings()
        params = settings.database.connection.model_dump()
        with psycopg2.connect(**params) as conn:
            sql = open(convert_relative_to_real_path('resources/updateDB.sql'), 'r').read()
            data = pd.read_sql_query(sql, conn)

        percentage_known = round(
            data['maf_known'].astype('float').div(data['maf_rows'].replace(0, np.nan)).fillna(0) * 100, 2)

        data.insert(data.shape[1] - 1, column="percentage known", value=percentage_known)

        google_sheet_api = get_settings().google.connection.google_sheet_api
        google_sheet_api_dict = google_sheet_api.__dict__

        replaceGoogleSheet(data, get_settings().google.sheets.mtbls_curation_log, 'Database Query',
                           google_sheet_api_dict)
        return jsonify({'curationlog sheet update': "Success"})

    except Exception as e:
        print(e)
        logger.error(e)
        return jsonify({'curationlog sheet update': "Failed"})
        


def curation_log_database_update(starting_index, ending_index):
    try:
        def execute_query(query):
            try:
                settings = get_settings()
                params = settings.database.connection.model_dump()
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

        google_sheet_api = get_settings().google.connection.google_sheet_api
        google_sheet_api_dict = google_sheet_api.__dict__
        
        google_df = getGoogleSheet(get_settings().google.sheets.mtbls_curation_log, 'Database update',
                                google_sheet_api_dict)

        command_list = google_df['--Updates. Run this in the database on a regular basis'].tolist()
        # empty_study = "update studies set studytype ='', species ='', placeholder ='', curator =''"
        # command_list = [x for x in command_list if empty_study not in x]

        # Find the maximum number of Metlite ID
        try:
            settings = get_settings()
            params = settings.database.connection.model_dump()
            connection = psycopg2.connect(**params)
            cursor = connection.cursor()
            select_Query = "select max(lpad(replace(acc, 'MTBLS', ''), 4, '0')) as acc_short from studies order by acc_short"
            cursor.execute(select_Query)
            result = cursor.fetchone()
            max_acc_short = int(result[0])
            logger.info(f'max_acc_short - {max_acc_short}')
        except Exception as e:
            logger.error("Retrieving acc from DB failed " + str(e))
        res = []
        e = 0
        length_of_list = len(command_list)
        logger.info(f'Length of  command_list- {length_of_list}')
        # starting_index = 7300
        # ending_index = 7316
        logger.info(f'starting_index from request - {starting_index}')
        logger.info(f'ending_index from request - {ending_index}')

        if starting_index is None or starting_index.isnumeric() is False:
            start_index = 0
        else:
            start_index = int(starting_index)

        if ending_index is None or ending_index.isnumeric() is False:
            end_index = length_of_list-1
        else:
            end_index = int(ending_index)

        if start_index > end_index or start_index >= length_of_list:
            start_index = 0

        if end_index >= length_of_list:
            max_of_itr = length_of_list - start_index - 1
            end_index = length_of_list - 1
        else:
            max_of_itr = end_index - start_index

        logger.info(f'starting_index for processing - {start_index}')
        logger.info(f'ending_index for processing - {end_index}')
        logger.info(f'maximum of iteration count- {max_of_itr}')

        start_line = command_list[start_index]
        end_line = command_list[end_index]
        starting_study_acc = re.search("acc = '(.*?)'", start_line)[1]
        ending_study_acc = re.search("acc = '(.*?)'", end_line)[1]
        logger.info(f'starting_study_acc - {starting_study_acc}')
        logger.info(f'ending_study_acc - {ending_study_acc}')

        for x in range(length_of_list):
            get_index = x + start_index
            line = command_list[get_index]
            if line and line.strip() != '':
                organism = update_species(line)
                if organism != '':
                    res.append(organism)
            else:
                e = e + 1
            if x == max_of_itr:
                break

        res += ['commit;']
        sql = ''.join(res)
        execute_query(sql)
        logger.info("Query executed successfully!!")
        response = {"number_studies_updated": len(res), "empty_rows": e}
        return response
    except Exception as e:
            logger.error("Exception while updating Study Metadata to DB " + str(e))
            response = {"Study Metadata to DB update": "Failed"}
            return response

def get_empty_studies():
    empty_email = []
    no_email = []

    google_sheet_api = get_settings().google.connection.google_sheet_api
    google_sheet_api_dict = google_sheet_api.__dict__
    g_sheet = getGoogleSheet(get_settings().google.sheets.mtbls_curation_log, 'Empty investigation files',
                             google_sheet_api_dict)
    ignore_studies = g_sheet['studyID'].tolist()
    ignore_submitter = ['MetaboLights', 'Metaspace', 'Metabolon', 'Venkata Chandrasekhar', 'User Cochrane']
    
    studyInfos = get_study_info(get_settings().auth.service_account.api_token)
    keys = ['studyID', 'username', 'status', 'placeholder']

    for studyInfo in studyInfos:
        print(studyInfo[0])
        if studyInfo[0] in ignore_studies or \
                any(ext in studyInfo[1] for ext in ignore_submitter) or \
                studyInfo[2] == 'Dormant':
            continue

        source = '/ws/studies/{study_id}?investigation_only=true'.format(study_id=studyInfo[0])
        ws_url = get_host_internal_url() + source

        try:
            resp = requests.get(ws_url, headers={'user_token': get_settings().auth.service_account.api_token})

            # empty investigation and studyID > MTBLS1700
            if resp.status_code == 200:
                data = resp.json()
                if data["isaInvestigation"]['studies'][0][
                    'title'] == 'Please update the study title' and empty_study_filter(studyInfo[0]):
                    # json_response
                    empty_email.append(studyInfo[0])

                    # logger
                    logger.info('Empty i_Investigation.txt in {studyID}'.format(studyID=studyInfo[0]))
                    print('Empty i_Investigation.txt in {studyID}'.format(studyID=studyInfo[0]))
                    continue

            # non - investigation - All studies
            else:
                # json_response
                no_email.append(studyInfo[0])

                # logger
                logger.info('Fail to load i_Investigation.txt from {studyID}'.format(studyID=studyInfo[0]))
                print('Fail to load i_Investigation.txt from {studyID}'.format(studyID=studyInfo[0]))
                continue

        except Exception as e:
            print(e.args)
            logger.info(e.args)

    # ws response & email
    empty_email.sort(key=natural_keys)
    no_email.sort(key=natural_keys)
    
    return empty_email, no_email


def file_permission(force: bool = False):
    raise NotImplementedError('file_permission is not implemented')
    # submit = []
    # curation = []
    # review = []
    # public = []
    # ftp_private_storage = StorageService.get_ftp_private_storage()
    # files = ftp_private_storage.remote.list_folder('/')
    # files = []
    # study_ids = [x.name.split('-')[0].upper() for x in files if x.name.upper().startswith('MTBLS')]
    # token = get_settings().auth.service_account.api_token
    # UserService.get_instance().validate_user_has_curator_role(token)
    # study_obfuscation_code_map = {}
    # study_status_map = {}
    #
    # with DBManager.get_instance().session_maker() as db_session:
    #     result = db_session.query(Study.acc, Study.obfuscationcode, Study.status).all()
    #     for item in result:
    #         study_obfuscation_code_map[item.acc] = item.obfuscationcode
    #         study_status_map[item.acc] = StudyStatus(item.status)
    #
    # for study_id in study_ids:
    #     try:
    #         if study_id not in study_obfuscation_code_map:
    #             logger.warning(f'Study {study_id} folder exist but is not defined in database')
    #             continue
    #         obfuscation_code = study_obfuscation_code_map[study_id]
    #         ftp_path = study_id.lower() + '-' + obfuscation_code
    #         if not ftp_private_storage.remote.does_folder_exist(ftp_path):
    #             create_ftp_folder(study_id, obfuscation_code, token, None, send_email=False)
    #         db_study_status = study_status_map[study_id]
    #         permission = ftp_private_storage.remote.get_folder_permission(ftp_path)
    #
    #         if db_study_status == StudyStatus.INCURATION and (permission != Acl.AUTHORIZED_READ or force):
    #             ftp_private_storage.remote.update_folder_permission(ftp_path, Acl.AUTHORIZED_READ)
    #             curation.append(study_id)
    #         elif db_study_status == StudyStatus.SUBMITTED and (permission != Acl.AUTHORIZED_READ_WRITE or force):
    #             ftp_private_storage.remote.update_folder_permission(ftp_path, Acl.AUTHORIZED_READ_WRITE)
    #             submit.append(study_id)
    #         elif db_study_status == StudyStatus.INREVIEW and (permission != Acl.READ_ONLY or force):
    #             ftp_private_storage.remote.update_folder_permission(ftp_path, Acl.READ_ONLY)
    #             review.append(study_id)
    #         elif db_study_status == StudyStatus.PUBLIC and (permission != Acl.READ_ONLY or force):
    #             ftp_private_storage.remote.update_folder_permission(ftp_path, Acl.READ_ONLY)
    #             public.append(study_id)
    #
    #     except Exception as e:
    #         logger.info(e)
    #         print(e)
    #         continue
    #
    # return submit, curation, review, public


def untarget_NMR():
    untarget_NMR = extractUntargetStudy(['NMR'])
    res = untarget_NMR['studyID'].tolist()
    return {'untarget_NMR': res}


def untarget_LCMS():
    untarget_LCMS = extractUntargetStudy(['LC'])
    res = untarget_LCMS['studyID'].tolist()
    return {'untarget_LCMS': res}


def NMR_and_LCMS():
    studyID, studyType = get_study_by_type(['LC', 'NMR'], publicStudy=False)
    df = pd.DataFrame(columns=['studyID', 'dataType'])
    df.studyID, df.dataType = studyID, studyType
    res = df.to_dict('records')
    return {"NMR_LCMS_studys": res}


def NMR_studies():
    pass


def GC_MS_studies():
    pass


def LC_MS_studies():
    pass


def MTBLS_statistics_update():
    ## update untarget NMR
    print('-' * 20 + 'UPDATE untarget NMR' + '-' * 20)
    logger.info('UPDATE untarget NMR')
    untarget_NMR = extractUntargetStudy(['NMR'])
    res = untarget_NMR[['studyID']]
    replaceGoogleSheet(df=res, url=get_settings().google.sheets.mtbls_statistics, worksheetName='untarget NMR',
                       googlesheet_key_dict=get_settings().google.connection.google_sheet_api)

    ## update untarget LC-MS
    print('-' * 20 + 'UPDATE untarget LC-MS' + '-' * 20)
    logger.info('UPDATE untarget LC-MS')
    untarget_LCMS = extractUntargetStudy(['LC'])
    res = untarget_LCMS[['studyID']]
    replaceGoogleSheet(df=res, url=get_settings().google.sheets.mtbls_statistics, worksheetName='untarget LC-MS',
                       googlesheet_key_dict=get_settings().google.connection.google_sheet_api)

    ## update NMR and LC-MS
    print('-' * 20 + 'UPDATE NMR and LC-MS' + '-' * 20)
    logger.info('UPDATE NMR and LC-MS')
    studyID, studyType = get_study_by_type(['LC', 'NMR'], publicStudy=False)
    df = pd.DataFrame(columns=['studyID', 'dataType'])
    df.studyID, df.dataType = studyID, studyType
    replaceGoogleSheet(df=df, url=get_settings().google.sheets.mtbls_statistics, worksheetName='both NMR and LCMS',
                       googlesheet_key_dict=get_settings().google.connection.google_sheet_api)

    ## update NMR sample / assay sheet
    print('-' * 20 + 'UPDATE NMR info' + '-' * 20)
    logger.info('UPDATE NMR info')
    df = getNMRinfo()
    replaceGoogleSheet(df=df, url=get_settings().google.sheets.mtbls_statistics, worksheetName='NMR',
                       googlesheet_key_dict=get_settings().google.connection.google_sheet_api)

    ## update MS sample / assay sheet

    ## update LC-MS sample / assay sheet
    print('-' * 20 + 'UPDATE LC-MS info' + '-' * 20)
    logger.info('UPDATE LC-MS info')
    df = getLCMSinfo()
    replaceGoogleSheet(df=df, url=get_settings().google.sheets.lc_ms_statistics, worksheetName='LCMS samples and assays',
                       googlesheet_key_dict=get_settings().google.connection.google_sheet_api)


def extractUntargetStudy(studyType=None, publicStudy=True):
    def extractNum(s):
        num = re.findall(r"\d+", s)[0]
        return int(num)

    # get all descriptor from studies
    def getDescriptor(sIDs=None):
        res = []

        if not sIDs:
            studyIDs = wsc.get_public_studies()
        else:
            studyIDs = sIDs

        for studyID in studyIDs:
            print(studyID)
            context_path = get_settings().server.service.resources_path
            source = '{context_path}/studies/{study_id}/descriptors'.format(context_path=context_path, study_id=studyID)
            ws_url = get_settings().server.service.mtbls_ws_host + ':' + str(get_settings().server.service.rest_api_port) + source
            try:
                resp = requests.get(ws_url, headers={'user_token': get_settings().auth.service_account.api_token})
                data = resp.json()
                for descriptor in data['studyDesignDescriptors']:
                    temp_dict = {'studyID': studyID,
                                 'term': descriptor['annotationValue'],
                                 'matched_iri': descriptor['termAccession']}
                    res.append(temp_dict)
            except Exception as e:
                logger.info('Fail to load descriptor from ' + studyID)
                logger.info(e.args)
                print('Fail to load descriptor from ' + studyID, end='\t')
                print(e.args)
        df = pd.DataFrame(res)
        return df

    if not studyType:
        studyIDs = get_public_studies()
    else:
        studyIDs, _ = get_study_by_type(studyType, publicStudy=publicStudy)

    descripter = getDescriptor(sIDs=studyIDs)
    untarget = descripter.loc[descripter['term'].str.startswith(('untargeted', 'Untargeted', 'non-targeted'))]
    untarget_df = untarget.copy()
    untarget_df['num'] = untarget_df['studyID'].apply(extractNum)
    untarget_df = untarget_df.sort_values(by=['num'])
    untarget_df = untarget_df.drop('num', axis=1)

    return untarget_df


def getNMRinfo():
    NMR_studies, _ = get_study_by_type(['NMR'], publicStudy=True)
    NMR_studies.sort(key=natural_keys)

    sample_df = pd.DataFrame(
        columns=["Study", "Characteristics.Organism.", "Characteristics.Organism.part.", "Protocol.REF", "Sample.Name"])

    assay_df = pd.DataFrame(columns=['Study', 'Sample.Name', 'Protocol.REF.0', 'Protocol.REF.1',
                                     'Parameter.Value.NMR.tube.type.', 'Parameter.Value.Solvent.',
                                     'Parameter.Value.Sample.pH.', 'Parameter.Value.Temperature.', 'Unit',
                                     'Label', 'Protocol.REF.2', 'Parameter.Value.Instrument.',
                                     'Parameter.Value.NMR.Probe.', 'Parameter.Value.Number.of.transients.',
                                     'Parameter.Value.Pulse.sequence.name.',
                                     'Acquisition.Parameter.Data.File', 'Protocol.REF.3', 'NMR.Assay.Name',
                                     'Free.Induction.Decay.Data.File', 'Protocol.REF.4',
                                     'Derived.Spectral.Data.File', 'Protocol.REF.5',
                                     'Data.Transformation.Name', 'Metabolite.Assignment.File'])

    for studyID in NMR_studies:
        print(studyID)
        # print('-' * 20 + studyID + '-' * 20)
        try:
            assay_file, investigation_file, sample_file, maf_file = getFileList(studyID)
        except:
            try:
                assay_file, sample_file = assay_sample_list(studyID)
                investigation_file = 'i_Investigation.txt'
            except Exception as e:
                print('Fail to load study ', studyID)
                print(e)
                logger.info(e.args)
                continue
        # ------------------------ SAMPLE FILE ----------------------------------------
        #
        sample_temp = get_sample_file(studyID, sample_file)
        sample_temp.insert(0, 'Study', studyID)
        sample_temp = DataFrameUtils.sample_cleanup(sample_temp)

        sample_df = pd.concat([sample_df, sample_temp], ignore_index=True)
        # print('get sample file from', studyID, end='\t')
        # print(sample_temp.shape)

        # ------------------------ ASSAY FILE -----------------------------------------
        for assay in assay_file:
            assay_temp = get_assay_file(studyID, assay)
            if 'Acquisition Parameter Data File' not in list(assay_temp.columns):
                continue
            else:
                assay_temp.insert(0, 'Study', studyID)
                assay_temp = DataFrameUtils.NMR_assay_cleanup(assay_temp)
                assay_df = pd.concat([assay_df, assay_temp], ignore_index=True)

            # print('get assay file from', studyID, end='\t')
            # print(assay_temp.shape)

    merge_frame = pd.merge(sample_df, assay_df, on=['Study', 'Sample.Name'])
    return merge_frame


# TODO
def getMSinfo():
    pass


def getLCMSinfo():
    failed_studies = []
    LCMS_studies, _ = get_study_by_type(['LC'], publicStudy=True)
    LCMS_studies.sort(key=natural_keys)

    sample_df = pd.DataFrame(
        columns=["Study", "Characteristics.Organism.", "Characteristics.Organism.part.", "Protocol.REF", "Sample.Name"])

    assay_df = pd.DataFrame(columns=['Study', 'Sample.Name', 'Protocol.REF.0', 'Parameter.Value.Post.Extraction.',
                                     'Parameter.Value.Derivatization.', 'Extract.Name', 'Protocol.REF.1',
                                     'Parameter.Value.Chromatography.Instrument.', 'Parameter.Value.Column.model.',
                                     'Parameter.Value.Column.type.', 'Labeled.Extract.Name', 'Label', 'Protocol.REF.2',
                                     'Parameter.Value.Scan.polarity.', 'Parameter.Value.Scan.m/z.range.',
                                     'Parameter.Value.Instrument.', 'Parameter.Value.Ion.source.',
                                     'Parameter.Value.Mass.analyzer.', 'MS.Assay.Name', 'Raw.Spectral.Data.File',
                                     'Protocol.REF.3', 'Normalization.Name', 'Derived.Spectral.Data.File',
                                     'Protocol.REF.4',
                                     'Data.Transformation.Name', 'Metabolite.Assignment.File'])

    for studyID in LCMS_studies:
        print(studyID)
        print('-' * 20 + studyID + '-' * 20)
        try:
            assay_file, investigation_file, sample_file, maf_file = getFileList(studyID)
        except:
            try:
                assay_file, sample_file = assay_sample_list(studyID)
                investigation_file = 'i_Investigation.txt'
            except Exception as e:
                print('Fail to load study ', studyID)
                logger.info(e.args)
                continue
        try:
            # ------------------------ ASSAY FILE -----------------------------------------
            for assay in assay_file:
                assay_temp = get_assay_file(studyID, assay)
                if 'Parameter Value[Scan polarity]' not in list(assay_temp.columns):
                    continue
                else:
                    assay_temp.insert(0, 'Study', studyID)
                    assay_temp = DataFrameUtils.LCMS_assay_cleanup(assay_temp)
                    assay_df = pd.concat([assay_df, assay_temp], ignore_index=True)

                    # print('get assay file from', studyID, end='\t')
                    # print(assay_temp.shape)

            # ------------------------ SAMPLE FILE ----------------------------------------
            sample_temp = get_sample_file(studyID, sample_file)
            sample_temp.insert(0, 'Study', studyID)
            sample_temp = DataFrameUtils.sample_cleanup(sample_temp)

            sample_df = pd.concat([sample_df, sample_temp], ignore_index=True)
            # print('get sample file from', studyID, end='\t')
            # print(sample_temp.shape)
        except Exception as e:
            failed_studies.append(studyID)
            print(e)
            pass

    sample_df = sample_df.drop_duplicates()
    assay_df = assay_df.drop_duplicates()

    merge_frame = pd.merge(sample_df, assay_df, on=['Study', 'Sample.Name'])
    return merge_frame


def getFileList2(studyID):
    context_path = get_settings().server.service.resources_path
    source = '{context_path}/studies/{study_id}/files?include_raw_data=false'.format(context_path=context_path, study_id=studyID)
    ws_url = get_settings().server.service.mtbls_ws_host + ':' + str(get_settings().server.service.rest_api_port) + source
    try:
        request = urllib.request.Request(ws_url)
        request.add_header('user_token', get_settings().auth.service_account.api_token)
        response = urllib.request.urlopen(request)
        content = response.read().decode('utf-8')
        j_content = json.loads(content)

        assay_file, sample_file, investigation_file, maf_file = [], '', '', []
        for files in j_content['study']:
            if files['status'] == 'active' and files['type'] == 'metadata_assay':
                assay_file.append(files['file'])
                continue
            if files['status'] == 'active' and files['type'] == 'metadata_investigation':
                investigation_file = files['file']
                continue
            if files['status'] == 'active' and files['type'] == 'metadata_sample':
                sample_file = files['file']
                continue
            if files['status'] == 'active' and files['type'] == 'metadata_maf':
                maf_file.append(files['file'])
                continue

        if assay_file == []: print('Fail to load assay file from ', studyID)
        if sample_file == '': print('Fail to load sample file from ', studyID)
        if investigation_file == '': print('Fail to load investigation file from ', studyID)
        if maf_file == []: print('Fail to load maf file from ', studyID)

        return assay_file, investigation_file, sample_file, maf_file

    except Exception as e:
        print(e)
        logger.info(e)


def getFileList(studyID):
    try:
        source = '/ws/studies/{study_id}/files?include_raw_data=false'.format(study_id=studyID)
        internal_url = get_host_internal_url() + source
        current_request = urllib.request.Request(internal_url)
        current_request.add_header('user_token', get_settings().auth.service_account.api_token)
        response = urllib.request.urlopen(current_request)
        content = response.read().decode('utf-8')
        j_content = json.loads(content)

        assay_file, sample_file, investigation_file, maf_file = [], '', '', []
        for files in j_content['study']:
            if files['status'] == 'active' and files['type'] == 'metadata_assay':
                assay_file.append(files['file'])
                continue
            if files['status'] == 'active' and files['type'] == 'metadata_investigation':
                investigation_file = files['file']
                continue
            if files['status'] == 'active' and files['type'] == 'metadata_sample':
                sample_file = files['file']
                continue
            if files['status'] == 'active' and files['type'] == 'metadata_maf':
                maf_file.append(files['file'])
                continue

        if assay_file == []: print('Fail to load assay file from ', studyID)
        if sample_file == '': print('Fail to load sample file from ', studyID)
        if investigation_file == '': print('Fail to load investigation file from ', studyID)
        if maf_file == []: print('Fail to load maf file from ', studyID)

        return assay_file, investigation_file, sample_file, maf_file
    except Exception as e:
        print(e)
        logger.info(e)


def get_sample_file(studyID, sample_file_name):
    '''
    get sample file

    :param studyID: study ID
    :param sample_file_name: active sample file name
    :return:  DataFrame
    '''
    import io
    try:
        context_path = get_settings().server.service.resources_path
        source = '{context_path}/studies/{study_id}/sample'.format(context_path=context_path, study_id=studyID)
        ws_url = get_settings().server.service.mtbls_ws_host + ':' + str(get_settings().server.service.rest_api_port) + source

        resp = requests.get(ws_url, headers={'user_token': get_settings().auth.service_account.api_token},
                            params={'sample_filename': sample_file_name})
        data = resp.text
        content = io.StringIO(data)
        df = pd.read_csv(content, sep='\t')
        return df
    except Exception as e:
        logger.info(e)
        print(e)


def get_assay_file(studyID, assay_file_name):
    '''
    get assay file

    :param studyID:  study ID
    :param assay_file_name: active assay file name
    :return:  DataFrame
    '''
    import io
    try:
        context_path = get_settings().server.service.resources_path
        source = '{context_path}/studies/{study_id}/assay'.format(context_path=context_path, study_id=studyID)
        ws_url = get_settings().server.service.mtbls_ws_host + ':' + str(get_settings().server.service.rest_api_port) + source

        resp = requests.get(ws_url, headers={'user_token': get_settings().auth.service_account.api_token},
                            params={'assay_filename': assay_file_name})
        data = resp.text
        content = io.StringIO(data)
        df = pd.read_csv(content, sep='\t')
        return df
    except Exception as e:
        logger.info(e)
        print(e)


def assay_sample_list(studyID):
    '''
    get list of sample and assay from investigation file
    :param studyID:
    :return:
    '''
    import io

    try:
        context_path = get_settings().server.service.resources_path
        source = '{context_path}/studies/{study_id}/investigation'.format(context_path=context_path, study_id=studyID)
        ws_url = get_settings().server.service.mtbls_ws_host + ':' + str(get_settings().server.service.rest_api_port) + source

        resp = requests.get(ws_url, headers={'user_token': get_settings().auth.service_account.api_token})
        buf = io.StringIO(resp.text)

        assay_list, sample_file = [], ''
        try:
            for line in buf.readlines():
                if line.startswith('Study Assay File Name'):
                    assay_list = list(re.findall(r'"([^"]*)"', line))
                if line.startswith('Study File Name'):
                    sample_file = re.findall(r'"([^"]*)"', line)[0]
                if len(assay_list) != 0 and sample_file != '':
                    return assay_list, sample_file
        except:
            print('Fail to read investigation file from ' + studyID)

    except Exception as e:
        logger.info(e)
        print(e)
        logger.info('Fail to load investigation file from ' + studyID)


def atoi(text):
    return int(text) if text.isdigit() else text


def natural_keys(text):
    return [atoi(c) for c in re.split(r'(\d+)', text)]


def empty_study_filter(studyID):
    num = int(studyID.lower().split('mtbls')[1])
    return num > 1700


def get_unique_organisms(studyID):
    '''
    get list of unique organism from study
    :param studyID: studyID
    :return: list of organisms
    '''
    try:
        context_path = get_settings().server.service.resources_path
        source = '{context_path}/studies/{study_id}/organisms'.format(context_path=context_path, study_id=studyID)
        url = get_settings().server.service.mtbls_ws_host + ':' + str(get_settings().server.service.rest_api_port) + source

        resp = requests.get(url, headers={'user_token': get_settings().auth.service_account.api_token})
        data = resp.json()
        org = []
        for organism in data['organisms']:
            org.append(organism['Characteristics[Organism]'])
        org = [x for x in org if len(x) > 0]
        return list(set(org))
    except Exception as e:
        logger.error(f'Exception while fetching organism {str(e)}')
        logger.error(f'Fail to load organism for study -  {studyID}')
        return []


def update_species(command_string):
    import re
    studyID = re.search("acc = '(.*?)'", command_string)[1]
    org_list = get_unique_organisms(studyID)
    if len(org_list) > 0:
        org = ';'.join(org_list)
        new_s = re.sub("species ='(.*?)'", "species ='{organism}'".format(organism=org), command_string)
        return new_s
    else:
        return command_string


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


def getGoogleSheet(url, worksheetName, googlesheet_key_dict):
    '''
    get google sheet
    :param url: url of google sheet
    :param worksheetName: work sheet name
    :return: data frame
    '''
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(keyfile_dict=googlesheet_key_dict, scopes=scope)
    gc = gspread.authorize(credentials)
    # wks = gc.open('Zooma terms').worksheet('temp')
    wks = gc.open_by_url(url).worksheet(worksheetName)
    content = wks.get_all_records()
    # max_rows = len(wks.get_all_values())
    df = pd.DataFrame(content)
    return df


def replaceGoogleSheet(df, url, worksheetName, googlesheet_key_dict):
    '''
    replace the old google sheet with new data frame, old sheet will be clear
    :param df: dataframe
    :param url: url of google sheet
    :param worksheetName: work sheet name
    :return: Nan
    '''
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(keyfile_dict=googlesheet_key_dict, scopes=scope)
    gc = gspread.authorize(credentials)
    wks = gc.open_by_url(url).worksheet(worksheetName)
    wks.clear()
    set_with_dataframe(wks, df)


def getCellCoordinate(url, worksheetName, token_path, term):
    '''
    find cell coordinate
    :param url: url of google sheet
    :param worksheetName: work sheet name
    :param term: searching term
    :return: cell row and cell column
    '''
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(token_path, scope)
    gc = gspread.authorize(credentials)
    # wks = gc.open('Zooma terms').worksheet('temp')
    wks = gc.open_by_url(url).worksheet(worksheetName)
    cell = wks.find(term)
    return cell.row, cell.col


def update_cell(ws, row, column, value):
    try:
        ws.update_cell(row, column, value)
        return True
    except Exception:
        print("Faill to update {value} at ({row}, {column})".format(value=value, row=row, column=column))
        logger.info("Faill to update {value} at ({row}, {column})".format(value=value, row=row, column=column))
        pass


def getWorksheet(url, worksheetName, token_path):
    '''
    get google sheet
    :param url: url of google sheet
    :param worksheetName: work sheet name
    :return: google wks
    '''
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(token_path, scope)
    gc = gspread.authorize(credentials)
    wks = gc.open_by_url(url).worksheet(worksheetName)
    return wks
