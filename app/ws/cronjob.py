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

import json
import urllib
from datetime import datetime

import gspread
import numpy as np
import pandas as pd
import requests
from flask import jsonify
from flask import request
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from owlready2 import urllib

from app.ws.db_connection import *
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import log_request, writeDataToFile

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
                "enum": ["curation log-Database Query", "curation log-Database update", "MTBLS statistics",
                         "empty studies", "MARIANA study_classify", "test cronjob"]
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
                return jsonify({'curation log update': True})
            except Exception as e:
                logger.info(e)
                print(e)
        elif source == 'curation log-Database update':
            try:
                logger.info('Updating curation log-Database update')
                curation_log_database_update()
                return jsonify({'Database update': True})
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
            file_path = app.config.get('MTBLS_FTP_ROOT') + app.config.get('MARIANA_PATH')
            writeDataToFile(file_path + file_name, res, True)
            return jsonify(res)
        elif source == 'test cronjob':
            pass
        else:
            abort(400)


def curation_log_database_query():
    try:
        params = app.config.get('DB_PARAMS')

        with psycopg2.connect(**params) as conn:
            sql = open('./resources/updateDB.sql', 'r').read()
            data = pd.read_sql_query(sql, conn)

        percentage_known = round(
            data['maf_known'].astype('float').div(data['maf_rows'].replace(0, np.nan)).fillna(0) * 100, 2)

        data.insert(data.shape[1] - 1, column="percentage known", value=percentage_known)

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

    command_list = google_df['--Updates. Run this in the database on a regular basis'].tolist()
    empty_study = "update studies set studytype ='', species ='', placeholder ='', curator =''"
    command_list = [x for x in command_list if empty_study not in x] + ['commit;']

    sql = ''.join(command_list)
    execute_query(sql)
    print('Done')


def get_empty_studies():
    empty_email = []
    no_email = []

    g_sheet = getGoogleSheet(app.config.get('MTBLS_CURATION_LOG'), 'Empty investigation files',
                             token_path=app.config.get('GOOGLE_SHEET_TOKEN'))
    ignore_studies = g_sheet['studyID'].tolist()
    ignore_submitter = ['MetaboLights', 'Metaspace', 'Metabolon', 'Venkata Chandrasekhar', 'User Cochrane']

    studyInfos = get_study_info(app.config.get('METABOLIGHTS_TOKEN'))
    keys = ['studyID', 'username', 'status', 'placeholder']

    for studyInfo in studyInfos:
        print(studyInfo[0])
        if studyInfo[0] in ignore_studies or \
                any(ext in studyInfo[1] for ext in ignore_submitter) or \
                studyInfo[2] == 'Dormant':
            continue

        source = '/metabolights/ws/studies/{study_id}?investigation_only=true'.format(study_id=studyInfo[0])
        ws_url = 'http://wp-p3s-15.ebi.ac.uk:5000' + source

        try:
            resp = requests.get(ws_url, headers={'user_token': app.config.get('METABOLIGHTS_TOKEN')})

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
    replaceGoogleSheet(df=res, url=app.config.get('MTBLS_STATISITC'), worksheetName='untarget NMR',
                       token_path=app.config.get('GOOGLE_SHEET_TOKEN'))

    ## update untarget LC-MS
    print('-' * 20 + 'UPDATE untarget LC-MS' + '-' * 20)
    logger.info('UPDATE untarget LC-MS')
    untarget_LCMS = extractUntargetStudy(['LC'])
    res = untarget_LCMS[['studyID']]
    replaceGoogleSheet(df=res, url=app.config.get('MTBLS_STATISITC'), worksheetName='untarget LC-MS',
                       token_path=app.config.get('GOOGLE_SHEET_TOKEN'))

    ## update NMR and LC-MS
    print('-' * 20 + 'UPDATE NMR and LC-MS' + '-' * 20)
    logger.info('UPDATE NMR and LC-MS')
    studyID, studyType = get_study_by_type(['LC', 'NMR'], publicStudy=False)
    df = pd.DataFrame(columns=['studyID', 'dataType'])
    df.studyID, df.dataType = studyID, studyType
    replaceGoogleSheet(df=df, url=app.config.get('MTBLS_STATISITC'), worksheetName='both NMR and LCMS',
                       token_path=app.config.get('GOOGLE_SHEET_TOKEN'))

    ## update NMR sample / assay sheet
    print('-' * 20 + 'UPDATE NMR info' + '-' * 20)
    logger.info('UPDATE NMR info')
    df = getNMRinfo()
    replaceGoogleSheet(df=df, url=app.config.get('MTBLS_STATISITC'), worksheetName='NMR',
                       token_path=app.config.get('GOOGLE_SHEET_TOKEN'))

    ## update MS sample / assay sheet

    ## update LC-MS sample / assay sheet
    print('-' * 20 + 'UPDATE LC-MS info' + '-' * 20)
    logger.info('UPDATE LC-MS info')
    df = getLCMSinfo()
    replaceGoogleSheet(df=df, url=app.config.get('LC_MS_STATISITC'), worksheetName='LCMS samples and assays',
                       token_path=app.config.get('GOOGLE_SHEET_TOKEN'))


def extractUntargetStudy(studyType=None, publicStudy=True):
    def extractNum(s):
        num = re.findall("\d+", s)[0]
        return int(num)

    # get all descriptor from studies
    def getDescriptor(sIDs=None):
        res = []

        if sIDs == None:
            studyIDs = wsc.get_public_studies()
        else:
            studyIDs = sIDs

        for studyID in studyIDs:
            print(studyID)
            source = '/metabolights/ws/studies/{study_id}/descriptors'.format(study_id=studyID)
            ws_url = app.config.get('MTBLS_WS_HOST') + ':' + str(app.config.get('PORT')) + source
            try:
                resp = requests.get(ws_url, headers={'user_token': app.config.get('METABOLIGHTS_TOKEN')})
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

    if studyType == None:
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
        # sample_temp2 = readSSHDataFrame(app.config.get('FILE_SYSTEM_PATH') + studyID + '/' + sample_file)
        sample_temp.insert(0, 'Study', studyID)
        sample_temp = sample_cleanup(sample_temp)

        sample_df = sample_df.append(sample_temp, ignore_index=True)
        # print('get sample file from', studyID, end='\t')
        # print(sample_temp.shape)

        # ------------------------ ASSAY FILE -----------------------------------------
        for assay in assay_file:
            # assay_temp = readSSHDataFrame(app.config.get('FILE_SYSTEM_PATH') + studyID + '/' + assay)
            assay_temp = get_assay_file(studyID, assay)
            if 'Acquisition Parameter Data File' not in list(assay_temp.columns):
                continue
            else:
                assay_temp.insert(0, 'Study', studyID)
                assay_temp = NMR_assay_cleanup(assay_temp)
                assay_df = assay_df.append(assay_temp, ignore_index=True)

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
                    assay_temp = LCMS_assay_cleanup(assay_temp)
                    assay_df = assay_df.append(assay_temp, ignore_index=True)

                    # print('get assay file from', studyID, end='\t')
                    # print(assay_temp.shape)

            # ------------------------ SAMPLE FILE ----------------------------------------
            sample_temp = get_sample_file(studyID, sample_file)
            sample_temp.insert(0, 'Study', studyID)
            sample_temp = sample_cleanup(sample_temp)

            sample_df = sample_df.append(sample_temp, ignore_index=True)
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
    source = '/metabolights/ws/studies/{study_id}/files?include_raw_data=false'.format(study_id=studyID)
    ws_url = app.config.get('MTBLS_WS_HOST') + ':' + str(app.config.get('PORT')) + source
    try:
        request = urllib.request.Request(ws_url)
        request.add_header('user_token', app.config.get('METABOLIGHTS_TOKEN'))
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
        source = '/metabolights/ws/studies/{study_id}/files?include_raw_data=false'.format(study_id=studyID)
        url = 'http://wp-p3s-15.ebi.ac.uk:5000' + source
        request = urllib.request.Request(url)
        request.add_header('user_token', app.config.get('METABOLIGHTS_TOKEN'))
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


def get_sample_file(studyID, sample_file_name):
    '''
    get sample file

    :param studyID: study ID
    :param sample_file_name: active sample file name
    :return:  DataFrame
    '''
    import io
    try:
        source = '/metabolights/ws/studies/{study_id}/sample'.format(study_id=studyID)
        ws_url = app.config.get('MTBLS_WS_HOST') + ':' + str(app.config.get('PORT')) + source

        resp = requests.get(ws_url, headers={'user_token': app.config.get('METABOLIGHTS_TOKEN')},
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
    :param sample_file_name: active assay file name
    :return:  DataFrame
    '''
    import io
    try:
        source = '/metabolights/ws/studies/{study_id}/assay'.format(study_id=studyID)
        ws_url = app.config.get('MTBLS_WS_HOST') + ':' + str(app.config.get('PORT')) + source

        resp = requests.get(ws_url, headers={'user_token': app.config.get('METABOLIGHTS_TOKEN')},
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
        source = '/metabolights/ws/studies/{study_id}/investigation'.format(study_id=studyID)
        ws_url = app.config.get('MTBLS_WS_HOST') + ':' + str(app.config.get('PORT')) + source

        resp = requests.get(ws_url, headers={'user_token': app.config.get('METABOLIGHTS_TOKEN')})
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
        logger.info('Fail to load investigation file from', studyID)


def NMR_assay_cleanup(df):
    '''
    Change / mapping NMR Study dataframe column name
    :param df:
    :return:
    '''
    keep = ['Study', 'Sample Name', 'Protocol REF', 'Protocol REF.1',
            'Parameter Value[NMR tube type]', 'Parameter Value[Solvent]',
            'Parameter Value[Sample pH]', 'Parameter Value[Temperature]', 'Unit',
            'Label', 'Protocol REF.2', 'Parameter Value[Instrument]',
            'Parameter Value[NMR Probe]', 'Parameter Value[Number of transients]',
            'Parameter Value[Pulse sequence name]',
            'Acquisition Parameter Data File', 'Protocol REF.3', 'NMR Assay Name',
            'Free Induction Decay Data File', 'Protocol REF.4',
            'Derived Spectral Data File', 'Protocol REF.5',
            'Data Transformation Name', 'Metabolite Assignment File']

    rename = {'Sample Name': 'Sample.Name', 'Protocol REF': 'Protocol.REF.0', 'Protocol REF.1': 'Protocol.REF.1',
              'Parameter Value[NMR tube type]': 'Parameter.Value.NMR.tube.type.',
              'Parameter Value[Solvent]': 'Parameter.Value.Solvent.',
              'Parameter Value[Sample pH]': 'Parameter.Value.Sample.pH.',
              'Parameter Value[Temperature]': 'Parameter.Value.Temperature.', 'Unit': 'Unit', 'Label': 'Label',
              'Protocol REF.2': 'Protocol.REF.2', 'Parameter Value[Instrument]': 'Parameter.Value.Instrument.',
              'Parameter Value[NMR Probe]': 'Parameter.Value.NMR.Probe.',
              'Parameter Value[Number of transients]': 'Parameter.Value.Number.of.transients.',
              'Parameter Value[Pulse sequence name]': 'Parameter.Value.Pulse.sequence.name.',
              'Acquisition Parameter Data File': 'Acquisition.Parameter.Data.File', 'Protocol REF.3': 'Protocol.REF.3',
              'NMR Assay Name': 'NMR.Assay.Name', 'Free Induction Decay Data File': 'Free.Induction.Decay.Data.File',
              'Protocol REF.4': 'Protocol.REF.4', 'Derived Spectral Data File': 'Derived.Spectral.Data.File',
              'Protocol REF.5': 'Protocol.REF.5', 'Data Transformation Name': 'Data.Transformation.Name',
              'Metabolite Assignment File': 'Metabolite.Assignment.File'}
    df = df[keep]
    df = df.rename(columns=rename)
    return df


def LCMS_assay_cleanup(df):
    '''
    Change / mapping LCMS Study dataframe column name
    :param df:
    :return:
    '''
    keep = ['Study', 'Sample Name', 'Protocol REF', 'Parameter Value[Post Extraction]',
            'Parameter Value[Derivatization]', 'Extract Name', 'Protocol REF.1',
            'Parameter Value[Chromatography Instrument]',
            'Parameter Value[Column model]', 'Parameter Value[Column type]',
            'Labeled Extract Name', 'Label', 'Protocol REF.2',
            'Parameter Value[Scan polarity]', 'Parameter Value[Scan m/z range]',
            'Parameter Value[Instrument]', 'Parameter Value[Ion source]',
            'Parameter Value[Mass analyzer]', 'MS Assay Name',
            'Raw Spectral Data File', 'Protocol REF.3', 'Normalization Name',
            'Derived Spectral Data File', 'Protocol REF.4',
            'Data Transformation Name', 'Metabolite Assignment File']

    rename = {'Study': 'Study',
              'Sample Name': 'Sample.Name',
              'Protocol REF': 'Protocol.REF.0',
              'Parameter Value[Post Extraction]': 'Parameter.Value.Post.Extraction.',
              'Parameter Value[Derivatization]': 'Parameter.Value.Derivatization.',
              'Extract Name': 'Extract.Name',
              'Protocol REF.1': 'Protocol.REF.1',
              'Parameter Value[Chromatography Instrument]': 'Parameter.Value.Chromatography.Instrument.',
              'Parameter Value[Column model]': 'Parameter.Value.Column.model.',
              'Parameter Value[Column type]': 'Parameter.Value.Column.type.',
              'Labeled Extract Name': 'Labeled.Extract.Name',
              'Label': 'Label',
              'Protocol REF.2': 'Protocol.REF.2',
              'Parameter Value[Scan polarity]': 'Parameter.Value.Scan.polarity.',
              'Parameter Value[Scan m/z range]': 'Parameter.Value.Scan.m/z.range.',
              'Parameter Value[Instrument]': 'Parameter.Value.Instrument.',
              'Parameter Value[Ion source]': 'Parameter.Value.Ion.source.',
              'Parameter Value[Mass analyzer]': 'Parameter.Value.Mass.analyzer.',
              'MS Assay Name': 'MS.Assay.Name',
              'Raw Spectral Data File': 'Raw.Spectral.Data.File',
              'Protocol REF.3': 'Protocol.REF.3',
              'Normalization Name': 'Normalization.Name',
              'Derived Spectral Data File': 'Derived.Spectral.Data.File',
              'Protocol REF.4': 'Protocol.REF.4',
              'Data Transformation Name': 'Data.Transformation.Name',
              'Metabolite Assignment File': 'Metabolite.Assignment.File'}
    k = pd.DataFrame(columns=keep)
    k = k.append(df, sort=False)
    df = k[keep]
    df = df.rename(columns=rename)
    return df


def sample_cleanup(df):
    '''
    Change / mapping sample file dataframe column name
    :param df:
    :return:
    '''
    keep = ['Study', 'Characteristics[Organism]', 'Characteristics[Organism part]', 'Protocol REF', 'Sample Name']
    rename = {'Characteristics[Organism]': 'Characteristics.Organism.',
              'Characteristics[Organism part]': 'Characteristics.Organism.part.',
              'Protocol REF': 'Protocol.REF',
              'Sample Name': 'Sample.Name'}

    k = pd.DataFrame(columns=keep)
    k = k.append(df, sort=False)
    df = k[keep]
    df = df.rename(columns=rename)
    return df


def atoi(text):
    return int(text) if text.isdigit() else text


def natural_keys(text):
    return [atoi(c) for c in re.split(r'(\d+)', text)]


def empty_study_filter(studyID):
    num = int(studyID.lower().split('mtbls')[1])
    return num > 1700


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

# def getStudyIDs(publicStudy=False):
#     def atoi(text):
#         return int(text) if text.isdigit() else text
#
#     def natural_keys(text):
#         return [atoi(c) for c in re.split('(\d+)', text)]
#
#     url = 'https://www.ebi.ac.uk/metabolights/webservice/study/list'
#     request = urllib.request.Request(url)
#     request.add_header('user_token', app.config.get('METABOLIGHTS_TOKEN'))
#     response = urllib.request.urlopen(request)
#     content = response.read().decode('utf-8')
#     j_content = json.loads(content)
#
#     studyIDs = j_content['content']
#     studyIDs.sort(key=natural_keys)
#
#     if publicStudy:
#         studyStatus = getStudyStatus()
#         studyIDs = [studyID for studyID in studyIDs if studyStatus[studyID] in ['Public', 'In Review']]
#     else:
#         studyStatus = getStudyStatus()
#         studyIDs = [studyID for studyID in studyIDs if
#                     studyStatus[studyID] in ['Public', 'In Review', 'Submitted', 'In Curation']]
#     return studyIDs


# def getStudyStatus():
#     query_user_access_rights = """
#              select case
#              when (status = 0 and placeholder = '1') then 'Placeholder'
#              when (status = 0 and placeholder='') then 'Submitted'
#              when status = 1 then 'In Curation'
#              when status = 2 then 'In Review'
#              when status = 3 then 'Public'
#              else 'Dormant' end as status,
#                 acc from studies;
#             """
#     token = app.config.get('METABOLIGHTS_TOKEN')
#
#     def execute_query(query, user_token, study_id=None):
#         try:
#             params = app.config.get('DB_PARAMS')
#             conn = psycopg2.connect(**params)
#             cursor = conn.cursor()
#             query = query.replace('\\', '')
#             if study_id is None:
#                 cursor.execute(query, [user_token])
#             else:
#                 query2 = query_user_access_rights.replace("#user_token#", user_token)
#                 query2 = query2.replace("#study_id#", study_id)
#                 cursor.execute(query2)
#             data = cursor.fetchall()
#             conn.close()
#
#             return data
#
#         except psycopg2.Error as e:
#             print("Unable to connect to the database")
#             print(e.pgcode)
#             print(e.pgerror)
#             print(traceback.format_exc())
#
#     study_list = execute_query(query_user_access_rights, token)
#     study_status = {}
#     for study in study_list:
#         study_status[study[1]] = study[0]
#
#     return study_status


# def getStudytype(sType, publicStudy=True):
#     def get_connection():
#         postgresql_pool = None
#         conn = None
#         cursor = None
#         try:
#             params = app.config.get('DB_PARAMS')
#             conn_pool_min = app.config.get('CONN_POOL_MIN')
#             conn_pool_max = app.config.get('CONN_POOL_MAX')
#             postgresql_pool = psycopg2.pool.SimpleConnectionPool(conn_pool_min, conn_pool_max, **params)
#             conn = postgresql_pool.getconn()
#             cursor = conn.cursor()
#         except Exception as e:
#             print("Could not query the database " + str(e))
#             if postgresql_pool:
#                 postgresql_pool.closeall
#         return postgresql_pool, conn, cursor
#
#     q2 = ' '
#     if publicStudy:
#         q2 = ' status in (2, 3) and '
#
#     if type(sType) == str:
#         q3 = "studytype = '{sType}'".format(sType=sType)
#
#     # fuzzy search
#     elif type(sType) == list:
#         DB_query = []
#         for q in sType:
#             query = "studytype like '%{q}%'".format(q=q)
#             DB_query.append(query)
#         q3 = ' and '.join(DB_query)
#
#     else:
#         return None
#
#     query = "SELECT acc,studytype FROM studies WHERE {q2} {q3};".format(q2=q2, q3=q3)
#     # query = q1 + q2 + q3 + ';'
#     print(query)
#     postgresql_pool, conn, cursor = get_connection()
#     cursor.execute(query)
#     res = cursor.fetchall()
#     studyID = [r[0] for r in res]
#     studytype = [r[1] for r in res]
#     return studyID, studytype


# def assay_sample_list2(studyID):
#     '''
#     get list of sample and assay from investigation file
#     :param studyID:
#     :return:
#     '''
#     import paramiko
#     import re
#
#     client = paramiko.SSHClient()
#     client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#
#     SSH_PARAMS = app.config.get('SSH_PARAMS')
#     client.connect(**SSH_PARAMS)
#     sftp_client = client.open_sftp()
#     address = '/net/isilonP/public/rw/homes/tc_cm01/metabolights/prod/studies/stage/private/' + studyID + '/i_Investigation.txt'
#     assay_list, sample_file = [], ''
#     try:
#         with sftp_client.open(address) as f:
#             for line in f.readlines():
#                 if line.startswith('Study Assay File Name'):
#                     assay_list = list(re.findall(r'"([^"]*)"', line))
#                 if line.startswith('Study File Name'):
#                     sample_file = re.findall(r'"([^"]*)"', line)[0]
#                 if len(assay_list) != 0 and sample_file != '':
#                     return assay_list, sample_file
#     except:
#         print('Fail to read investigation file from ' + studyID)


# def readSSHDataFrame(filePath):
#     '''
#     Load file from SSH server
#     :param filePath:
#     :return:
#     '''
#     import paramiko
#     client = paramiko.SSHClient()
#     client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#
#     SSH_PARAMS = app.config.get('SSH_PARAMS')
#     client.connect(**SSH_PARAMS)
#     sftp_client = client.open_sftp()
#     try:
#         with sftp_client.open(filePath) as f:
#             df = pd.read_csv(f, sep='\t')
#             return df
#     except:
#         print('Fail to load file from ' + filePath)

# def get_empty_studies2():
#     res = []
#
#     studyIDs = get_study_info(app.config.get('METABOLIGHTS_TOKEN'))
#     keys = ['studyID', 'username', 'status', 'placeholder']
#     for studyInfo in studyIDs:
#         source = '/metabolights/ws/studies/{study_id}?investigation_only=true'.format(study_id=studyInfo[0])
#         ws_url = 'http://wp-p3s-15.ebi.ac.uk:5000' + source
#
#         try:
#             print(studyInfo[0])
#             resp = requests.get(ws_url, headers={'user_token': app.config.get('METABOLIGHTS_TOKEN')})
#             if resp.status_code != 200:
#                 f = ['MetaboLights', 'Metaspace', 'Metabolon', 'Venkata Chandrasekhar', 'User Cochrane']
#                 if any(ext in studyInfo[1] for ext in f) or studyInfo[2] == 'Dormant':
#                     continue
#                 temp_dict = dict(zip(keys, studyInfo))
#                 temp_dict['investigation'] = 'None'
#                 res.append(temp_dict)
#                 logger.info('Fail to load i_Investigation.txt from {studyID}'.format(studyID=studyInfo[0]))
#                 print('Fail to load i_Investigation.txt from {studyID}'.format(studyID=studyInfo[0]))
#                 continue
#             # resp = requests.get(ws_url, headers={'user_token': app.config.get('METABOLIGHTS_TOKEN')})
#             data = resp.json()
#             # if len(data["isaInvestigation"]['ontologySourceReferences']) == 0:
#             #     temp_dict = dict(zip(keys, studyInfo))
#             #     temp_dict['investigation'] = 'empty'
#             #     res.append(temp_dict)
#             #     logger.info('Empty i_Investigation.txt in {studyID}'.format(studyID=studyInfo[0]))
#             #     print('Empty i_Investigation.txt in {studyID}'.format(studyID=studyInfo[0]))
#             #     continue
#             # elif studyInfo[3] == 'Yes':
#             #     temp_dict = dict(zip(keys, studyInfo))
#             #     temp_dict['investigation'] = ' '
#             #     res.append(temp_dict)
#             #     logger.info('placeholder flag in {studyID}'.format(studyID=studyInfo[0]))
#             #     print('placeholder flag in {studyID}'.format(studyID=studyInfo[0]))
#
#         except Exception as e:
#             logger.info(e.args)
#             print(e.args)
#             # print(studyInfo[0])
#             # f = ['MetaboLights', 'Metaspace', 'Metabolon', 'Venkata Chandrasekhar']
#             # if any(ext in studyInfo[1] for ext in f) or studyInfo[2] == 'Dormant':
#             #     continue
#             # temp_dict = dict(zip(keys, studyInfo))
#             # temp_dict['investigation'] = 'None'
#             # res.append(temp_dict)
#             # logger.info('Fail to load i_Investigation.txt from {studyID}'.format(studyID=studyInfo[0]))
#             # print('Fail to load i_Investigation.txt from {studyID}'.format(studyID=studyInfo[0]))
#             # continue
#     df = pd.DataFrame(res)
#     df.to_csv('empty studies updatea.tsv', sep='\t', index=False)
#     return res