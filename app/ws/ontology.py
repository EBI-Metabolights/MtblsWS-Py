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

import numpy as np
from flask import jsonify
from flask import request, abort
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger

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
                priority = {'MTBLS': 0, 'NCBITAXON': 1, 'WoRMs': 2, 'EFO': 3, 'BTO': 4, 'CHEBI': 5, 'CHMO': 6, 'NCIT': 6,
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
