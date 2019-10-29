#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-May-08
#  Modified by:   kenneth
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

import datetime
import re

import gspread
import numpy as np
import requests
from flask import jsonify
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


# # Allow for a more detailed logging when on DEBUG mode
# def log_request(request_obj):
#     if app.config.get('DEBUG'):
#         if app.config.get('DEBUG_LOG_HEADERS'):
#             logger.debug('REQUEST HEADERS -> %s', request_obj.headers)
#         if app.config.get('DEBUG_LOG_BODY'):
#             logger.debug('REQUEST BODY    -> %s', request_obj.data)
#         if app.config.get('DEBUG_LOG_JSON'):
#             try:
#                 logger.debug('REQUEST JSON    -> %s', request_obj.json)
#             except:
#                 logger.debug('REQUEST JSON    -> EMPTY')


class Ontology(Resource):

    @swagger.operation(
        summary="Get ontology onto_information",
        notes="Get ontology onto_information.",
        parameters=[
            {
                "name": "term",
                "description": "Ontology term",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },

            {
                "name": "branch",
                "description": "starting branch of ontology",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["factor", "role", "taxonomy", "characteristic", "publication", "design descriptor", "unit",
                         "column type", "instruments", "confidence", "sample type"]
            },

            {
                "name": "mapping",
                "description": "taxonomy search approach",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["typo", "exact", "fuzzy"]
            },

            {
                "name": "queryFields",
                "description": "Specifcy the fields to return, the default is all options: {MTBLS,MTBLS_Zooma,Zooma,OLS, Bioportal}",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },

            {
                "name": "ontology",
                "description": "Restrict a search to a set of ontologies",
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
        log_request(request)
        parser = reqparse.RequestParser()

        parser.add_argument('term', help="Ontology term")
        term = None
        if request.args:
            args = parser.parse_args(req=request)
            term = args['term']
            if term:
                term = term.strip()

        parser.add_argument('branch', help='Starting branch of ontology')
        branch = None
        if request.args:
            args = parser.parse_args(req=request)
            branch = args['branch']
            if branch:
                branch = branch.strip()

        parser.add_argument('mapping', help='Mapping approaches')
        mapping = None
        if request.args:
            args = parser.parse_args(req=request)
            mapping = args['mapping']

        parser.add_argument('queryFields', help='query Fields')
        queryFields = None  # ['MTBLS', 'MTBLS_Zooma', 'Zooma','OLS', 'Bioportal']
        if request.args:
            args = parser.parse_args(req=request)
            queryFields = args['queryFields']
            if queryFields:
                try:
                    reg = '\{([^}]+)\}'
                    queryFields = re.findall(reg, queryFields)[0].split(',')
                except:
                    try:
                        queryFields = queryFields.split(',')
                    except Exception as e:
                        print(e.args)

        parser.add_argument('ontology', help='ontology')
        ontology = None
        if request.args:
            args = parser.parse_args(req=request)
            ontology = args['ontology']
            if ontology:
                try:
                    reg = '\{([^}]+)\}'
                    ontology = re.findall(reg, ontology)[0].split(',')
                except:
                    try:
                        ontology = ontology.split(',')
                    except Exception as e:
                        print(e.args)

        result = []

        if term in [None, ''] and branch is None:
            return []

        if ontology not in [None, '']:  # if has ontology searching restriction
            logger.info('Search %s in' % ','.join(ontology))
            print('Search %s in' % ','.join(ontology))
            try:
                result = getOLSTerm(term, mapping, ontology=ontology)
            except Exception as e:
                print(e.args)
                logger.info(e.args)

        else:
            if queryFields in [None, '']:  # if found the term, STOP
                logger.info('Search %s from resources one by one' % term)
                print('Search %s from resources one by one' % term)
                result = getMetaboTerm(term, branch, mapping)

                if len(result) == 0:
                    print("Can't find query in MTBLS ontology, search metabolights-zooma.tsv")
                    logger.info("Can't find query in MTBLS ontology, search metabolights-zooma.tsv")
                    try:
                        result = getMetaboZoomaTerm(term, mapping)
                    except Exception as e:
                        print(e.args)
                        logger.info(e.args)

                if len(result) == 0:
                    print("Can't query it in Zooma.tsv, requesting OLS")
                    logger.info("Can't query it in Zooma.tsv, requesting OLS")
                    try:
                        result = getOLSTerm(term, mapping, ontology=ontology)
                    except Exception as e:
                        print(e.args)
                        logger.info(e.args)

                if len(result) == 0:
                    print("Can't find query in OLS, requesting Zooma")
                    logger.info("Can't find query in OLS, requesting Zooma")
                    try:
                        result = getZoomaTerm(term)
                    except Exception as e:
                        print(e.args)
                        logger.info(e.args)

                if len(result) == 0:
                    print("Can't query it in Zooma, request Bioportal")
                    logger.info("Can't query it in Zooma, request Bioportal")
                    try:
                        result = getBioportalTerm(term)
                    except  Exception as e:
                        print(e.args)
                        logger.info(e.args)

            else:
                if 'MTBLS' in queryFields:
                    result += getMetaboTerm(term, branch, mapping)

                if 'MTBLS_Zooma' in queryFields:
                    result += getMetaboZoomaTerm(term, mapping)

                if 'OLS' in queryFields:
                    result += getOLSTerm(term, mapping)

                if 'Zooma' in queryFields:
                    result += getZoomaTerm(term, mapping)

                if 'Bioportal' in queryFields:
                    result += getBioportalTerm(term)

        response = []

        result = removeDuplicated(result)

        # add WoRMs terms as a entity
        if branch == 'taxonomy':
            r = getWormsTerm(term)
            result += r
        else:
            pass

        if term not in [None, '']:
            exact = [x for x in result if x.name.lower() == term.lower()]
            rest = [x for x in result if x not in exact]

            # "factor", "role", "taxonomy", "characteristic", "publication", "design descriptor", "unit",
            #                          "column type", "instruments", "confidence", "sample type"

            if branch == 'taxonomy':
                priority = {'MTBLS': 0, 'NCBITAXON': 1, 'WoRMs': 2, 'EFO': 3, 'BTO': 4, 'CHEBI': 5, 'CHMO': 6,
                            'NCIT': 6,
                            'PO': 8}

            if branch == 'factor':
                priority = {'MTBLS': 0, 'EFO': 1, 'MESH': 2, 'BTO': 3, 'CHEBI': 4, 'CHMO': 5, 'NCIT': 6, 'PO': 7}

            if branch == 'design descriptor':
                priority = {'MTBLS': 0, 'EFO': 1, 'MESH': 2, 'BTO': 3, 'CHEBI': 4, 'CHMO': 5, 'NCIT': 6, 'PO': 7}

            else:
                priority = {'MTBLS': 0, 'EFO': 1, 'NCBITAXON': 2, 'BTO': 3, 'CHEBI': 4, 'CHMO': 5, 'NCIT': 6, 'PO': 7}

            exact = setPriority(exact, priority)
            rest = reorder(rest, term)
            result = exact + rest

        # result = removeDuplicated(result)

        for cls in result:
            temp = '''    {
                            "comments": [],
                            "annotationValue": "",
                            "annotationDefinition": "", 
                            "termAccession": "",
                            "wormsID": "", 
                            
                            "termSource": {
                                "comments": [],
                                "name": "",
                                "file": "",
                                "provenanceName": "",
                                "version": "",
                                "description": ""
                            }                            
                        }'''

            d = json.loads(str(temp))
            try:
                d['annotationValue'] = cls.name
                d["annotationDefinition"] = cls.definition
                if branch == 'taxonomy':
                    d['wormsID'] = cls.iri.rsplit('id=', 1)[-1]
                d["termAccession"] = cls.iri
                d['termSource']['name'] = cls.ontoName
                d['termSource']['provenanceName'] = cls.provenance_name

                if cls.ontoName == 'MTBLS':
                    d['termSource']['file'] = 'https://www.ebi.ac.uk/metabolights/'
                    d['termSource']['provenanceName'] = 'Metabolights'
                    d['termSource']['version'] = '1.0'
                    d['termSource']['description'] = 'Metabolights Ontology'
                # else:
                #     d['termSource']['file'] = cls.provenance_uri
                #     d['termSource']['description'] = cls.definition
                #     fullName, version = getOnto_info(cls.ontoName)
                #     d['termSource']['version'] = version
                #     d['termSource']['ontology_description'] = fullName
                #
                #     if cls.provenance_name != '':
                #         d['termSource']['provenance_name'] = str(cls.provenance_name)
                #     else:
                #         d['termSource']['provenance_name'] = str(cls.ontoName)

                # d['termSource']['description_url'] = str(getDescriptionURL(cls.ontoName, cls.iri))

            except:
                pass

            if cls.provenance_name == 'metabolights-zooma':
                d['termSource']['version'] = str(datetime.datetime.now().date())
            response.append(d)

        # response = [{'SubClass': x} for x in res]
        print('--' * 30)
        return jsonify({"OntologyTerm": response})

    # =========================== put =============================================

    @swagger.operation(
        summary="Put ontology entity to metabolights-zooma.tsv",
        notes="Put ontology entity to metabolights-zooma.tsv",
        parameters=[
            {
                "name": "term",
                "description": "Ontology term",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },

            {
                "name": "attribute_name",
                "description": "Attribute name",
                "required": True,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["factor", "role", "taxonomy", "characteristic", "publication", "design descriptor", "unit",
                         "column type", "instruments"]
            },

            {
                "name": "term_iri",
                "description": "iri/url of the mapping term",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },

            {
                "name": "study_ID",
                "description": "Study ID of the term",
                "required": True,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },

            {
                "name": "annotator",
                "description": "annotator's name",
                "required": True,
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
        log_request(request)

        parser = reqparse.RequestParser()
        parser.add_argument('term', help="Ontology term")
        term = None
        parser.add_argument('attribute_name', help='Attribute name')
        attribute_name = None
        parser.add_argument('term_iri', help='iri of the mapped term')
        term_iri = None
        parser.add_argument('study_ID', help='study_ID')
        study_ID = None
        parser.add_argument('annotator', help='annotator name')
        annotator = None

        if request.args:
            args = parser.parse_args(req=request)
            term = args['term']
            attribute_name = args['attribute_name']
            term_iri = args['term_iri']
            study_ID = args['study_ID']
            annotator = args['annotator']

        if term is None:
            abort(404, 'Please provide new term name')

        if term_iri is None:
            abort(404, 'Please provide mapped iri of the new term')

        if study_ID is None or annotator is None:
            abort(404, 'Please provide valid parameters for study identifier and file name')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, \
        submission_date, study_status = wsc.get_permissions("MTBLS1", user_token)

        if not is_curator:
            abort(403)
        file_name = app.config.get('MTBLS_ZOOMA_FILE')

        logger.info('Trying to load metabolights-zooma.tsv file')
        # Get the Assay table or create a new one if it does not already exist
        try:
            table_df = pd.read_csv(file_name, sep="\t", encoding='utf-8')
            table_df = table_df.replace(np.nan, '', regex=True)

            s2 = pd.Series(
                [study_ID, '', attribute_name, term, term_iri, annotator,
                 datetime.datetime.now().strftime('%d/%m/%Y %I:%M')],
                index=['STUDY',
                       'BIOENTITY',
                       'PROPERTY_TYPE',
                       'PROPERTY_VALUE',
                       'SEMANTIC_TAG',
                       'ANNOTATOR',
                       'ANNOTATION_DATE'])

            table_df = table_df.append(s2, ignore_index=True)
            table_df.to_csv(file_name, sep="\t", header=True, encoding='utf-8', index=False)
        except FileNotFoundError:
            abort(400, "The file %s was not found", file_name)


class Placeholder(Resource):
    @swagger.operation(
        summary="Get placeholder terms from study files",
        notes="Get placeholder terms",
        parameters=[
            {
                "name": "query",
                "description": "Data field to extract from study",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["factor", "design descriptor"]
            },

            {
                "name": "capture_type",
                "description": "particular type of data to extracted, placeholder/wrong_match",
                "required": False,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "defaultValue": "placeholder",
                "default": True,
                "enum": ["placeholder", "wrong_match"]
            },
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
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self):
        log_request(request)
        parser = reqparse.RequestParser()

        query = ''
        parser.add_argument('query', help='data field to extract from studies')
        if request.args:
            args = parser.parse_args(req=request)
            query = args['query']
            if query is None:
                abort(400)
            if query:
                query = query.strip().lower()

        capture_type = ''
        parser.add_argument('capture_type', help='capture type')
        if request.args:
            args = parser.parse_args(req=request)
            capture_type = args['capture_type']
            if capture_type is None:
                capture_type = 'placeholder'
            if capture_type:
                capture_type = capture_type.strip().lower()

        url = app.config.get('GOOGLE_SHEET_URL')
        sheet_name = ''
        col = []

        if query == 'factor':
            if capture_type == 'placeholder':
                sheet_name = 'factor placeholder'
            elif capture_type == 'wrong_match':
                sheet_name = 'factor wrong match'

            col = ['operation(Update/Add/Delete)', 'status (Done/Error)', 'studyID', 'name', 'annotationValue',
                   'termAccession']

        elif query == 'design descriptor':
            if capture_type == 'placeholder':
                sheet_name = 'descriptor placeholder'

            elif capture_type == 'wrong_match':
                sheet_name = 'descriptor wrong match'

            col = ['operation(Update/Add/Delete)', 'status (Done/Error)', 'studyID', 'name', 'matched_iri']

        else:
            abort(400)

        try:
            google_df = getGoogleSheet(url, sheet_name)

        except Exception as e:
            google_df = pd.DataFrame(columns=col)
            print(e.args)
            logger.info('Fail to load spreadsheet from Google')
            logger.info(e.args)

        df = pd.DataFrame(get_metainfo(query, capture_type))
        df_connect = pd.concat([google_df, df], ignore_index=True, sort=False)
        df_connect = df_connect.reindex(columns=col) \
            .replace(np.nan, '', regex=True) \
            .drop_duplicates(keep='first', subset=["studyID", "name"])

        adding_count = df_connect.shape[0] - google_df.shape[0]

        def extractNum(s):
            num = re.findall("\d+", s)[0]
            return int(num)

        df_connect['num'] = df_connect['studyID'].apply(extractNum)
        df_connect = df_connect.sort_values(by=['num'])
        df_connect = df_connect.drop('num', axis=1)

        replaceGoogleSheet(df_connect, url, sheet_name)
        return jsonify({'success': True, 'add': adding_count})

    # ============================ Placeholder put ===============================
    @swagger.operation(
        summary="Make changes according to google term sheets",
        notes="Update/add/Delete placeholder terms",
        parameters=[
            {
                "name": "query",
                "description": "Data field to change",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["factor", "design descriptor"]
            },

            {
                "name": "change_type",
                "description": "type of data to change, placeholder/wrong_match",
                "required": False,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "defaultValue": "placeholder",
                "default": True,
                "enum": ["placeholder", "wrong_match"]
            },
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
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def put(self):
        log_request(request)
        parser = reqparse.RequestParser()

        query = ''
        parser.add_argument('query', help='data field to update')
        if request.args:
            args = parser.parse_args(req=request)
            query = args['query']
            if query is None:
                abort(400)
            if query:
                query = query.strip().lower()

        capture_type = ''
        parser.add_argument('change_type', help='change type')
        if request.args:
            args = parser.parse_args(req=request)
            capture_type = args['change_type']
            if capture_type is None:
                capture_type = 'placeholder'
            if capture_type:
                capture_type = capture_type.strip().lower()

        google_url = app.config.get('GOOGLE_SHEET_URL')
        sheet_name = ''
        col = []

        # get sheet_name
        if query == 'factor':
            if capture_type == 'placeholder':
                sheet_name = 'factor placeholder'
            elif capture_type == 'wrong_match':
                sheet_name = 'factor wrong match'

            col = ['operation(Update/Add/Delete)', 'status (Done/Error)', 'studyID', 'name', 'annotationValue',
                   'termAccession']

        elif query == 'design descriptor':
            if capture_type == 'placeholder':
                sheet_name = 'descriptor placeholder'

            elif capture_type == 'wrong_match':
                sheet_name = 'descriptor wrong match'

            col = ['operation(Update/Add/Delete)', 'status (Done/Error)', 'studyID', 'name', 'matched_iri']

        else:
            abort(400)

        # Load google sheet
        google_df = getGoogleSheet(google_url, sheet_name)

        ch = google_df[(google_df['operation(Update/Add/Delete)'] != '') & (google_df['status (Done/Error)'] == '')]

        for index, row in ch.iterrows():
            if query == 'factor':
                operation, studyID, term, annotationValue, termAccession = \
                    row['operation(Update/Add/Delete)'], row['studyID'], row['name'], row['annotationValue'], row[
                        'termAccession']

                source = '/metabolights/ws/studies/{study_id}/factors'.format(study_id=studyID)
                ws_url = app.config.get('MTBLS_WS_HOST') + app.config.get('MTBLS_WS_PORT') + source

                # ws_url = 'https://www.ebi.ac.uk/metabolights/ws/studies/{study_id}/factors'.format(study_id=studyID)
                protocol = '''
                            {
                                "factorName": "",
                                "factorType": {
                                  "annotationValue": "",
                                  "termSource": {
                                    "name": "",
                                    "file": "",
                                    "version": "",
                                    "description": ""
                                  },
                                  "termAccession": ""
                                }                       
                            }
                            '''

                # Update factor
                if row['operation(Update/Add/Delete)'].lower() in ['update', 'u']:
                    try:
                        onto_name = getOnto_Name(termAccession)[0]
                        onto_iri, onto_version, onto_description = getOnto_info(onto_name)

                        temp = json.loads(protocol)
                        temp["factorName"] = term
                        temp["factorType"]["annotationValue"] = annotationValue
                        temp["factorType"]['termSource']['name'] = onto_name
                        temp["factorType"]['termSource']['file'] = onto_iri
                        temp["factorType"]['termSource']['version'] = onto_version
                        temp["factorType"]['termSource']['description'] = onto_description
                        temp["factorType"]['termAccession'] = termAccession

                        data = json.dumps({"factor": temp})

                        response = requests.put(ws_url, params={'name': term},
                                                headers={'user_token': app.config.get('METABOLIGHTS_TOKEN')},
                                                data=data)
                        print('Made correction from {term} to {matchterm}({matchiri}) in {studyID}'.
                              format(term=term, matchterm=annotationValue, matchiri=termAccession, studyID=studyID))

                        if response.status_code == 200:
                            google_df.loc[index, 'status (Done/Error)'] = 'Done'
                        else:
                            google_df.loc[index, 'status (Done/Error)'] = 'Error'

                        replaceGoogleSheet(google_df, google_url, sheet_name)

                    except Exception as e:
                        row['status (Done/Error)'] = 'Error'
                        logger.info(e)

                # Add factor
                elif row['operation(Update/Add/Delete)'].lower() in ['add', 'A']:
                    try:
                        row['status (Done/Error)'] = 'Done'
                    except Exception as e:
                        row['status (Done/Error)'] = 'Error'
                        logger.info(e)

                # Delete factor
                elif row['operation(Update/Add/Delete)'].lower() in ['delete', 'D']:
                    try:
                        row['status (Done/Error)'] = 'Done'
                    except Exception as e:
                        row['status (Done/Error)'] = 'Error'
                        logger.info(e)

                # Keep factor
                elif row['operation(Update/Add/Delete)'].lower() in ['keep', 'K']:
                    try:
                        row['status (Done/Error)'] = 'Done'
                    except Exception as e:
                        row['status (Done/Error)'] = 'Error'
                        logger.info(e)

                else:
                    logger.info('Wrong operation tag in the spreadsheet')
                    abort(400)


            elif query == 'design descriptor':

                operation, studyID, term, matched_iri = row['operation(Update/Add/Delete)'], row['studyID'], row[
                    'name'], row['matched_iri']

                source = 'metabolights/ws/studies/{study_id}/descriptors'.format(study_id=studyID)
                ws_url = app.config.get('MTBLS_WS_HOST') + app.config.get('MTBLS_WS_PORT') + source

                protocol = '''
                        {
                            "annotationValue": " ",
                            "termSource": {
                                "name": " ",
                                "file": " ",
                                "version": " ",
                                "description": " "
                            },
                            "termAccession": " "
                        }
                    '''

                # Update descriptor
                if row['operation(Update/Add/Delete)'].lower() in ['update', 'U']:
                    try:
                        onto_name = getOnto_Name(matched_iri)[0]
                        onto_iri, onto_version, onto_description = getOnto_info(onto_name)

                        temp = json.loads(protocol)
                        temp["annotationValue"] = term
                        temp["termSource"]["name"] = onto_name
                        temp["termSource"]["file"] = onto_iri
                        temp["termSource"]["version"] = onto_version
                        temp["termSource"]["description"] = onto_description
                        temp["termAccession"] = matched_iri

                        data = json.dumps({"studyDesignDescriptor": temp})

                        response = requests.put(google_url, params={'term': term},
                                                headers={'user_token': app.config.get('METABOLIGHTS_TOKEN')}, data=data)
                        print('Made correction from {term} to {matchterm}({matchiri}) in {studyID}'.
                              format(term=term, matchterm=term, matchiri=matched_iri, studyID=studyID))

                        if response.status_code == 200:
                            google_df.loc[index, 'status (Done/Error)'] = 'Done'
                        else:
                            google_df.loc[index, 'status (Done/Error)'] = 'Error'

                        replaceGoogleSheet(google_df, google_url, sheet_name)

                    except Exception as e:
                        row['status (Done/Error)'] = 'Error'
                        logger.info(e)

                # Add descriptor
                elif row['operation(Update/Add/Delete)'].lower() in ['add', 'A']:
                    try:
                        row['status (Done/Error)'] = 'Done'
                    except Exception as e:
                        row['status (Done/Error)'] = 'Error'
                        logger.info(e)

                # Delete descriptor
                elif row['operation(Update/Add/Delete)'].lower() in ['delete', 'D']:
                    try:
                        row['status (Done/Error)'] = 'Done'
                    except Exception as e:
                        row['status (Done/Error)'] = 'Error'
                        logger.info(e)

                # Keep descriptor
                elif row['operation(Update/Add/Delete)'].lower() in ['keep', 'K']:
                    try:
                        row['status (Done/Error)'] = 'Done'
                    except Exception as e:
                        row['status (Done/Error)'] = 'Error'
                        logger.info(e)

                else:
                    logger.info('Wrong operation tag in the spreadsheet')
                    abort(400)

            else:
                logger.info('Wrong query field requested')
                abort(404)


def get_metainfo(query, capture_type):
    '''
    get placeholder/wrong-match terms from study investigation file
    :param query: factor / descriptor ...
    :param capture_type: placeholder / wrong_match
    :return: list of dictionary results
    '''
    res = []

    def getStudyIDs():
        def atoi(text):
            return int(text) if text.isdigit() else text

        def natural_keys(text):
            return [atoi(c) for c in re.split('(\d+)', text)]

        url = 'https://www.ebi.ac.uk/metabolights/webservice/study/list'
        resp = requests.get(url, headers={'user_token': app.config.get('METABOLIGHTS_TOKEN')})
        studyIDs = resp.json()['content']
        studyIDs.sort(key=natural_keys)
        return studyIDs

    logger.info('Getting {query} {capture_type} terms'.format(query=query, capture_type=capture_type))
    studyIDs = getStudyIDs()

    for studyID in studyIDs:
        print(studyID)
        if query.lower() == "factor":
            url = 'https://www.ebi.ac.uk/metabolights/ws/studies/{study_id}/factors'.format(study_id=studyID)

            try:
                resp = requests.get(url, headers={'user_token': app.config.get('METABOLIGHTS_TOKEN')})
                data = resp.json()

                for factor in data["factors"]:
                    temp_dict = {'studyID': studyID,
                                 'name': factor['factorName'],
                                 'annotationValue': factor['factorType']['annotationValue'],
                                 'termAccession': factor['factorType']['termAccession']}
                    # Placeholder
                    if capture_type == 'placeholder':
                        if 'placeholder' in factor['factorType']['termAccession']:
                            res.append(temp_dict)

                    # Wrong match
                    elif capture_type == 'wrong_match':
                        if factor['factorName'].lower() != factor['factorType']['annotationValue'].lower():
                            res.append(temp_dict)
                    else:
                        abort(400)
            except:
                pass


        elif query.lower() == "design descriptor":
            url = 'https://www.ebi.ac.uk/metabolights/ws/studies/{study_id}/descriptors'.format(study_id=studyID)

            try:
                resp = requests.get(url, headers={'user_token': app.config.get('METABOLIGHTS_TOKEN')})
                data = resp.json()

                for descriptor in data['studyDesignDescriptors']:

                    temp_dict = {'studyID': studyID,
                                 'name': descriptor['annotationValue'],
                                 'matched_iri': descriptor['termAccession']}

                    # Placeholder
                    if capture_type == 'placeholder':
                        if 'placeholder' in temp_dict['matched_iri']:
                            res.append(temp_dict)

                    # Wrong match
                    elif capture_type == 'wrong_match':
                        if len(temp_dict['matched_iri']) == 0:
                            res.append(temp_dict)
                    else:
                        abort(400)
            except:
                pass
        else:
            abort(400)
    return res


def insertGoogleSheet(data, url, worksheetName):
    '''
    :param data: list of data
    :param url: url of google sheet
    :param worksheetName: worksheet name
    :return: Nan
    '''
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(app.config.get('GOOGLE_TOKEN'), scope)
    gc = gspread.authorize(credentials)
    try:
        wks = gc.open_by_url(url).worksheet(worksheetName)
        wks.append_row(data, value_input_option='RAW')
    except Exception as e:
        print(e.args)
        logger.info(e.args)


def setGoogleSheet(df, url, worksheetName):
    '''
    set whole dataframe to google sheet, if sheet existed create a new one
    :param df: dataframe want to save to google sheet
    :param url: url of google sheet
    :param worksheetName: worksheet name
    :return: Nan
    '''
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(app.config.get('GOOGLE_TOKEN'), scope)
    gc = gspread.authorize(credentials)
    try:
        wks = gc.open_by_url(url).worksheet(worksheetName)
        print(worksheetName + ' existed... create a new one')
        wks = gc.open_by_url(url).add_worksheet(title=worksheetName + '_1', rows=df.shape[0], cols=df.shape[1])
    except Exception as e:
        wks = gc.open_by_url(url).add_worksheet(title=worksheetName, rows=df.shape[0], cols=df.shape[1])
        logger.info(e.args)
    set_with_dataframe(wks, df)


def getGoogleSheet(url, worksheetName):
    '''
    get google sheet
    :param url: url of google sheet
    :param worksheetName: work sheet name
    :return: data frame
    '''
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(app.config.get('GOOGLE_TOKEN'), scope)
        gc = gspread.authorize(credentials)
        wks = gc.open_by_url(url).worksheet(worksheetName)
        content = wks.get_all_records()
        df = pd.DataFrame(content)
        return df
    except Exception as e:
        logger.info(e.args)


def replaceGoogleSheet(df, url, worksheetName):
    '''
    replace the old google sheet with new data frame, old sheet will be clear
    :param df: dataframe
    :param url: url of google sheet
    :param worksheetName: work sheet name
    :return: Nan
    '''
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(app.config.get('GOOGLE_TOKEN'), scope)
        gc = gspread.authorize(credentials)
        wks = gc.open_by_url(url).worksheet(worksheetName)
        wks.clear()
        set_with_dataframe(wks, df)
    except Exception as e:
        logger.info(e.args)
