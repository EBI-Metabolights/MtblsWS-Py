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
import json
import logging
import re
import ssl
from urllib.parse import quote_plus

import numpy as np
import pandas as pd
from flask import jsonify
from flask import request, abort, current_app as app
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from owlready2 import get_ontology, urllib, IRIS

from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.ontology_info import entity
from app.ws.ontology_info import onto_information
from app.ws.utils import log_request

logger = logging.getLogger('wslog')
iac = IsaApiClient()
wsc = WsClient()


# Allow for a more detailed logging when on DEBUG mode
def log_request(request_obj):
    if app.config.get('DEBUG'):
        if app.config.get('DEBUG_LOG_HEADERS'):
            logger.debug('REQUEST HEADERS -> %s', request_obj.headers)
        if app.config.get('DEBUG_LOG_BODY'):
            logger.debug('REQUEST BODY    -> %s', request_obj.data)
        if app.config.get('DEBUG_LOG_JSON'):
            try:
                logger.debug('REQUEST JSON    -> %s', request_obj.json)
            except:
                logger.debug('REQUEST JSON    -> EMPTY')


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
        exact = [x for x in result if x.name.lower() == term.lower()]
        rest = [x for x in result if x not in exact]

        if branch == 'taxonomy':
            priority = {'MTBLS': 0, 'NCBITAXON': 1, 'EFO': 2, 'BTO': 3, 'CHEBI': 4, 'CHMO': 5, 'NCIT': 6, 'PO': 7}
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
                    d['wormsID'] = getWoRMs(cls.name)
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


def OLSbranchSearch(keyword, branchName, ontoName):
    res = []
    if keyword in [None, '']:
        return res

    def getStartIRI(start, ontoName):
        url = 'https://www.ebi.ac.uk/ols/api/search?q=' + start + '&ontology=' + ontoName + '&queryFields=label'
        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        json_str = json.loads(content)
        res = json_str['response']['docs'][0]['iri']
        return urllib.parse.quote_plus(res)

    branchIRI = getStartIRI(branchName, ontoName)
    keyword = keyword.replace(' ', '%20')
    url = 'https://www.ebi.ac.uk/ols/api/search?q=' + keyword + '&rows=10&ontology=' + ontoName + '&allChildrenOf=' + branchIRI
    # print(url)
    fp = urllib.request.urlopen(url)
    content = fp.read().decode('utf-8')
    json_str = json.loads(content)

    for ele in json_str['response']['docs']:
        enti = entity(name=ele['label'],
                      iri=ele['iri'], ontoName=ontoName, provenance_name=ontoName)

        res.append(enti)
    return res


def getMetaboTerm(keyword, branch, mapping=''):
    logger.info('Search %s in Metabolights ontology' % keyword)
    print('Search "%s" in Metabolights ontology' % keyword)

    onto = get_ontology('./tests/Metabolights.owl').load()
    info = onto_information(onto)
    set_priortity = False

    res_cls = []
    result = []
    if keyword not in [None, '']:
        if branch:  # term = 1, branch = 1, search term in the branch
            start_cls = onto.search_one(label=branch)
            try:
                clses = info.get_subs(start_cls)
            except:
                logger.info("Can't find a branch called " + branch)
                print("Can't find a branch called " + branch)
                return []

        else:  # term = 1, branch = 0, search term in the whole ontology
            try:
                clses = list(onto.classes())
            except Exception as e:
                print(e.args)
                return []

        #  exact match
        for cls in clses:
            if keyword.lower() == cls.label[0].lower():
                subs = info.get_subs(cls)
                res_cls = [cls] + subs

        # if not exact match, do fuzzy match
        if len(res_cls) == 0:
            if mapping != 'exact':
                for cls in clses:
                    if cls.label[0].lower().startswith(keyword.lower()):
                        res_cls.append(cls)
                        res_cls += info.get_subs(cls)

        # synonym match
        if branch == 'taxonomy' or branch == 'factors':
            for cls in clses:
                try:
                    map = IRIS['http://www.geneontology.org/formats/oboInOwl#hasExactSynonym']
                    Synonym = list(map[cls])
                    if keyword.lower() in [syn.lower() for syn in Synonym]:
                        res_cls.append(cls)
                except Exception as e:
                    print(e.args)
                    pass

        if branch == 'instruments':
            r = OLSbranchSearch(keyword, 'instrument', 'msio')
            print()

        if branch == 'column type':
            result += OLSbranchSearch(keyword, 'chromatography', 'chmo')

    elif keyword in [None, ''] and branch:  # term = 0, branch = 1, return whole branch
        start_cls = onto.search_one(label=branch)
        try:
            res_cls = info.get_subs(start_cls, num=30)

            if branch == 'design descriptor':
                set_priortity = True
                first_priority_terms = ['ultra-performance liquid chromatography-mass spectrometry',
                                        'untargeted metabolites', 'targeted metabolites']

                for term in first_priority_terms:
                    ele = onto.search_one(label=term)
                    res_cls = [ele] + res_cls

        except Exception as e:
            logger.info("Can't find a branch called" + branch)
            print("Can't find a branch called" + branch)
            return []

        if branch == 'instruments':
            result += OLSbranchSearch("*", 'instrument', 'msio')

        if branch == 'column type':
            result += OLSbranchSearch("*", 'chromatography', 'chmo')

    else:  # term = 0, branch = 0, return []
        return []

    if len(res_cls) > 0:
        for cls in res_cls:

            enti = entity(name=cls.label[0], iri=cls.iri,
                          provenance_name='Metabolights')

            if cls.isDefinedBy:
                enti.definition = cls.isDefinedBy[0]

            if 'MTBLS' in cls.iri:
                enti.ontoName = 'MTBLS'
            else:
                try:
                    onto_name = getOnto_Name(enti.iri)[0]
                except:
                    onto_name = ''

                enti.ontoName = onto_name
                enti.provenance_name = onto_name

            result.append(enti)
        if not set_priortity:
            result.insert(0, result.pop())

    return result


def getMetaboZoomaTerm(keyword, mapping):
    logger.info('Searching Metabolights-zooma.tsv')
    print('Searching Metabolights-zooma.tsv')
    res = []

    if keyword in [None, '']:
        return res

    try:
        fileName = app.config.get('MTBLS_ZOOMA_FILE')  # metabolights_zooma.tsv
        df = pd.read_csv(fileName, sep="\t", header=0, encoding='utf-8')
        df = df.drop_duplicates(subset='PROPERTY_VALUE', keep="last")

        if mapping == 'exact':
            temp = df.loc[df['PROPERTY_VALUE'].str.lower() == keyword.lower()]
        else:
            temp1 = df.loc[df['PROPERTY_VALUE'].str.lower() == keyword.lower()]
            reg = "^" + keyword + "+"
            temp2 = df.loc[df['PROPERTY_VALUE'].str.contains(reg, case=False)]
            frame = [temp1, temp2]
            temp = pd.concat(frame).reset_index(drop=True)

        temp = temp.drop_duplicates(subset='PROPERTY_VALUE', keep="last", inplace=False)

        for i in range(len(temp)):
            iri = temp.iloc[i]['SEMANTIC_TAG']
            # name = ' '.join(
            #     [w.capitalize() if w.islower() else w for w in temp.iloc[i]['PROPERTY_VALUE'].split()])

            name = temp.iloc[i]['PROPERTY_VALUE'].capitalize()
            obo_ID = iri.rsplit('/', 1)[-1]

            enti = entity(name=name,
                          iri=iri,
                          provenance_name='metabolights-zooma',
                          provenance_uri='https://www.ebi.ac.uk/metabolights/',
                          Zooma_confidence='High')

            try:
                enti.ontoName, enti.definition = getOnto_Name(iri)
            except:
                enti.ontoName = 'MTBLS'

            res.append(enti)
    except Exception as e:
        logger.error('Fail to load metabolights-zooma.tsv' + str(e))

    return res


def getZoomaTerm(keyword, mapping=''):
    logger.info('Requesting Zooma...')
    print('Requesting Zooma...')
    res = []

    if keyword in [None, '']:
        return res

    try:
        # url = 'http://snarf.ebi.ac.uk:8480/spot/zooma/v2/api/services/annotate?propertyValue=' + keyword.replace(' ',"+")
        url = 'https://www.ebi.ac.uk/spot/zooma/v2/api/services/annotate?propertyValue=' + keyword.replace(' ', "+")
        # url = 'https://www.ebi.ac.uk/spot/zooma/v2/api/services/annotate?propertyValue=' + keyword.replace(' ', "+")
        ssl._create_default_https_context = ssl._create_unverified_context
        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf8')
        json_str = json.loads(content)
        for term in json_str:
            iri = term['semanticTags'][0]

            # name = ' '.join(
            #     [w.capitalize() if w.islower() else w for w in term["annotatedProperty"]['propertyValue'].split()])

            name = term["annotatedProperty"]['propertyValue'].capitalize()

            if mapping == 'exact' and name != keyword:
                continue

            enti = entity(name=name,
                          iri=iri,
                          Zooma_confidence=term['confidence'])

            if enti.ontoName == '':
                enti.ontoName, enti.definition = getOnto_Name(iri)

            try:
                enti.provenance_name = term['derivedFrom']['provenance']['source']['name']
            except:
                enti.provenance_name = enti.ontoName

            if enti.provenance_name == 'metabolights':
                res = [enti] + res
            else:
                res.append(enti)

            if len(res) >= 10:
                break
    except Exception as e:
        logger.error('getZooma' + str(e))
    return res


def getOLSTerm(keyword, map, ontology=''):
    logger.info('Requesting OLS...')
    print('Requesting OLS...')
    res = []

    if keyword in [None, '']:
        return res

    try:
        # https://www.ebi.ac.uk/ols/api/search?q=lung&groupField=true&queryFields=label,synonym&fieldList=iri,label,short_form,obo_id,ontology_name,ontology_prefix
        url = 'https://www.ebi.ac.uk/ols/api/search?q=' + keyword.replace(' ', "+") + \
              '&groupField=true' \
              '&queryFields=label,synonym' \
              '&type=class' \
              '&fieldList=iri,label,short_form,ontology_name,description,ontology_prefix' \
              '&rows=30'  # &exact=true
        if map == 'exact':
            url += '&exact=true'

        if ontology not in [None, '']:
            onto_list = ','.join(ontology)
            url += '&ontology=' + onto_list

        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        j_content = json.loads(content)
        responses = j_content["response"]['docs']

        for term in responses:
            # name = ' '.join([w.capitalize() if w.islower() else w for w in term['label'].split()])

            name = term['label'].capitalize()

            try:
                definition = term['description'][0]
            except:
                definition = ''

            try:
                ontoName, provenance_name = getOnto_Name(term['iri'])
            except:
                ontoName = ''
                provenance_name = ''

            enti = entity(name=name, iri=term['iri'], definition=definition, ontoName=ontoName,
                          provenance_name=provenance_name)

            res.append(enti)
            if len(res) >= 20:
                break

    except Exception as e:
        print(e.args)
        logger.error('getOLS' + str(e))
    return res


def getBioportalTerm(keyword):
    logger.info('Requesting Bioportal...')
    print('Requesting Bioportal...')
    res = []

    if keyword in [None, '']:
        return res

    try:
        url = 'http://data.bioontology.org/search?q=' + keyword.replace(' ', "+")  # + '&require_exact_match=true'
        request = urllib.request.Request(url)
        request.add_header('Authorization', 'apikey token=' + app.config.get('BIOPORTAL_TOKEN'))
        response = urllib.request.urlopen(request)
        content = response.read().decode('utf-8')
        j_content = json.loads(content)

        iri_record = []

        for term in j_content['collection']:
            iri = term['@id']
            if iri in iri_record:
                continue

            if 'mesh' in iri.lower():
                ontoName = 'MESH'
            elif 'nci' in iri.lower():
                ontoName = 'NCIT'
            elif 'bao' in iri.lower():
                ontoName = 'BAO'
            elif 'meddra' in iri.lower():
                ontoName = 'MEDDRA'
            else:
                ontoName = getOnto_Name(iri)[0]

            enti = entity(name=term['prefLabel'],
                          iri=iri,
                          ontoName=ontoName, provenance_name=ontoName)
            res.append(enti)
            iri_record.append(iri)
            if len(res) >= 5:
                break
    except Exception as e:
        logger.error('getBioportal' + str(e))
    return res


def getWoRMs(term):
    try:
        url = 'http://www.marinespecies.org/rest/AphiaIDByName/' + term.replace(' ', '%20') + "?marine_only=true"
        fp = urllib.request.urlopen(url)
        AphiaID = fp.read().decode('utf-8')
        return AphiaID
    except:
        return ''


def getOnto_info(pre_fix):
    try:
        if 'nmr' in pre_fix.lower():
            onto_id = 'NMRCV'
        else:
            onto_id = pre_fix

        url = 'https://www.ebi.ac.uk/ols/api/ontologies/' + onto_id
        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        j_content = json.loads(content)
        title = j_content['config']['title']
        version = j_content['config']['version']
        return title, version
    except:
        return '', ''


def getOnto_Name(iri):
    # get ontology name by giving iri of entity
    try:
        url = 'http://www.ebi.ac.uk/ols/api/terms/findByIdAndIsDefiningOntology?iri=' + iri
        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        j_content = json.loads(content)
        try:
            return j_content['_embedded']['terms'][0]['ontology_prefix'], \
                   j_content['_embedded']['terms'][0]['description'][0]
        except:
            return j_content['_embedded']['terms'][0]['ontology_prefix'], ''

    except:
        substring = iri.rsplit('/', 1)[-1]
        return ''.join(x for x in substring if x.isalpha()), ''


def getOnto_version(pre_fix):
    try:
        url = 'https://www.ebi.ac.uk/ols/api/ontologies/' + pre_fix
        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        j_content = json.loads(content)
        return j_content['config']['version']
    except:
        return ''


def getOnto_url(pre_fix):
    try:
        url = 'https://www.ebi.ac.uk/ols/api/ontologies/' + pre_fix
        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        j_content = json.loads(content)
        return j_content['config']['id']
    except:
        return ''


def setPriority(res_list, priority):
    res = sorted(res_list, key=lambda x: priority.get(x.ontoName, 1000))
    return res


def reorder(res_list, keyword):
    def sort_key(s, keyword):
        try:
            exact = s.lower() == keyword.lower()
        except:
            exact = False

        try:
            start = s.startswith(keyword)
        except:
            start = False
        try:
            partial = keyword in s
        except:
            partial = False

        return exact, start, partial

    try:
        res = sorted(res_list, key=lambda x: sort_key(x.name, keyword), reverse=True)
        return res
    except:
        return res_list


def removeDuplicated(res_list):
    iri_pool = []
    for res in res_list:
        if res.iri in iri_pool:
            res_list.remove(res)
        else:
            iri_pool.append(res.iri)
    return res_list


def getDescriptionURL(ontoName, iri):
    ir = quote_plus(quote_plus(iri))
    url = 'https://www.ebi.ac.uk/ols/api/ontologies/' + ontoName + '/terms/' + ir
    return url
