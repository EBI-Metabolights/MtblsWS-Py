import datetime
import json
import logging
import re
import ssl

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

from urllib.parse import quote_plus

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
                         "column type", "instruments"]
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

        result = []

        if term in [None, ''] and branch is None:
            return []
        if queryFields == None:  # if found the term, STOP

            logger.info('Search %s from resources one by one' % term)
            print('Search %s from resources one by one' % term)
            result = getMetaboTerm(term, branch)

            if len(result) == 0:
                print("Can't find query in MTBLS ontology, search metabolights-zooma.tsv")
                logger.info("Can't find query in MTBLS ontology, search metabolights-zooma.tsv")
                try:
                    result = getMetaboZoomaTerm(term)
                except Exception as e:
                    print(e.args)
                    logger.info(e.args)

            if len(result) == 0:
                print("Can't find query in metabolights-zooma.tsv, requesting Zooma")
                logger.info("Can't find query in MTBLS ontology, requesting Zooma")
                try:
                    result = getZoomaTerm(term)
                except Exception as e:
                    print(e.args)
                    logger.info(e.args)
            if len(result) == 0:
                print("Can't query it in Zooma, requesting OLS")
                logger.info("Can't query it in Zooma, requesting OLS")
                try:
                    result = getOLSTerm(term)
                except Exception as e:
                    print(e.args)
                    logger.info(e.args)
            if len(result) == 0:
                print("Can't query it in OLS, request Bioportal")
                logger.info("Can't query it in OLS, request Bioportal")
                try:
                    result = getBioportalTerm(term)
                except  Exception as e:
                    print(e.args)
                    logger.info(e.args)

        else:
            if 'MTBLS' in queryFields:
                result += getMetaboTerm(term, branch)

            if 'MTBLS_Zooma' in queryFields:
                result += getMetaboZoomaTerm(term, mapping='fuzzy')

            if 'Zooma' in queryFields:
                result += getZoomaTerm(term)

            if 'OLS' in queryFields:
                result += getOLSTerm(term)

            if 'Bioportal' in queryFields:
                result += getBioportalTerm(term)

        response = []

        a = result
        if queryFields and ('OLS' not in queryFields) and ('Bioportal' not in queryFields):
            result = setPriority(result)
            result = reorder(result, term)
        result = removeDuplicated(result)

        for cls in result:
            temp = '''    {
                            "comments": [],
                            "annotationValue": "investigator",
                            "termSource": {
                                "comments": [],
                                "ontology_name": "",
                                "ontology_description": "",
                                "file": "",
                                "provenance_name":"",
                                "version": "",
                                "description": "",
                                "description_url": ""
                            },
                            "termAccession": ""
                        }'''

            d = json.loads(str(temp))
            try:
                d['annotationValue'] = str(cls.name)
                d["termAccession"] = str(cls.iri)
                d['termSource']['ontology_name'] = str(cls.ontoName)
                if cls.ontoName == 'MTBLS':
                    d['termSource']['file'] = 'https://www.ebi.ac.uk/metabolights/'
                    d['termSource']['provenance_name'] = 'Metabolights'
                    d['termSource']['version'] = '1.0'
                    d['termSource']['ontology_description'] = 'Metabolights Ontology'
                else:
                    if cls.provenance_uri:
                        d['termSource']['file'] = str(cls.provenance_uri)
                    else:
                        d['termSource']['file'] = str(getOnto_url(cls.ontoName))
                    if cls.provenance_name:
                        d['termSource']['provenance_name'] = str(cls.provenance_name)
                    else:
                        d['termSource']['provenance_name'] = str(cls.ontoName)

                    if cls.definition:
                        d['termSource']['description'] = str(cls.definition)

                    d['termSource']['version'] = str(getOnto_version(cls.ontoName))
                    d['termSource']['ontology_description'] = str(getOnto_title(cls.ontoName))
                    d['termSource']['description_url'] = str(getDescriptionURL(cls.ontoName,cls.iri))
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
        # print(json_str['response']['docs'])
        res = json_str['response']['docs'][0]['iri']
        # return res
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
                      iri=ele['iri'],
                      obo_ID=ele['short_form'],
                      ontoName=ele['ontology_prefix'])
        res.append(enti)
    return res


def getMetaboTerm(keyword, branch):
    logger.info('Search %s in Metabolights ontology' % keyword)
    print('Search "%s" in Metabolights ontology' % keyword)
    onto = get_ontology('./tests/Metabolights.owl').load()
    info = onto_information(onto)

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
            res_cls = info.get_subs(start_cls, num=10)
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
            if 'MTBLS' in cls.iri:
                ontoName = 'MTBLS'
            else:
                ontoName = getOnto_Name(cls.iri)

            enti = entity(name=cls.label[0], iri=cls.iri, obo_ID=cls.name, ontoName=ontoName,
                          provenance_name='Metabolights', provenance_uri='https://www.ebi.ac.uk/metabolights/')
            result.append(enti)
        result.insert(0, result.pop())

    return result


def getMetaboZoomaTerm(keyword, mapping='fuzzy'):
    logger.info('Searching Metabolights-zooma.tsv')
    print('Searching Metabolights-zooma.tsv')
    res = []

    if keyword in [None, '']:
        return res

    try:
        fileName = app.config.get('MTBLS_ZOOMA_FILE')  # metabolights_zooma.tsv
        df = pd.read_csv(fileName, sep="\t", header=0, encoding='utf-8')
        df = df.drop_duplicates(subset='PROPERTY_VALUE', keep="last")

        if mapping == 'fuzzy':
            temp1 = df.loc[df['PROPERTY_VALUE'].str.lower() == keyword.lower()]
            reg = "^" + keyword + "+"
            temp2 = df.loc[df['PROPERTY_VALUE'].str.contains(reg, case=False)]
            frame = [temp1, temp2]
            temp = pd.concat(frame).reset_index(drop=True)
        else:
            temp = df.loc[df['PROPERTY_VALUE'].str.lower() == keyword.lower()]

        temp = temp.drop_duplicates(subset='PROPERTY_VALUE', keep="last", inplace=False)

        for i in range(len(temp)):
            iri = temp.iloc[i]['SEMANTIC_TAG']
            if 'mesh' in iri.lower():
                ontoName = 'MESH'
            elif 'nci' in iri.lower():
                ontoName = 'NCIT'
            elif 'bao' in iri.lower():
                ontoName = 'BAO'
            else:
                ontoName = getOnto_Name(iri)

            name = ' '.join(
                [w.title() if w.islower() else w for w in temp.iloc[i]['PROPERTY_VALUE'].split()])
            obo_ID = iri.rsplit('/', 1)[-1]

            enti = entity(name=name,
                          iri=iri,
                          obo_ID=iri.rsplit('/', 1)[-1],
                          ontoName=ontoName,
                          provenance_name='metabolights-zooma',
                          provenance_uri='https://www.ebi.ac.uk/metabolights/',
                          Zooma_confidence='High')
            res.append(enti)
    except Exception as e:
        logger.error('Fail to load metabolights-zooma.tsv' + str(e))

    return res


def getZoomaTerm(keyword):
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
            if 'mesh' in iri.lower():
                ontoName = 'MESH'
            elif 'nci' in iri.lower():
                ontoName = 'NCIT'
            elif 'bao' in iri.lower():
                ontoName = 'BAO'
            else:
                ontoName = getOnto_Name(iri)

            name = ' '.join(
                [w.title() if w.islower() else w for w in term["annotatedProperty"]['propertyValue'].split()])

            enti = entity(name=name,
                          iri=iri,
                          obo_ID=iri.rsplit('/', 1)[-1],
                          ontoName=ontoName,
                          Zooma_confidence=term['confidence'])

            try:
                provenance_name = term['derivedFrom']['provenance']['source']['name']
                provenance_uri = term['derivedFrom']['provenance']['source']['uri']
            except:
                provenance_name = ''
                provenance_uri = ''

            if 'http' not in provenance_name:
                enti.provenance_name = provenance_name
            else:
                enti.provenance_name = ontoName

            enti.provenance_uri = provenance_uri

            if enti.provenance_name == 'metabolights':
                res = [enti] + res
            else:
                res.append(enti)

            if len(res) >= 5:
                break
    except Exception as e:
        logger.error('getZooma' + str(e))
    return res


def getOLSTerm(keyword):
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
              'fieldList=iri,label,short_form,ontology_name,description,ontology_prefix' \
              '&rows=30'  # &exact=true
        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        j_content = json.loads(content)
        responses = j_content["response"]['docs']

        for term in responses:
            name = ' '.join(
                [w.title() if w.islower() else w for w in term['label'].split()])

            try:
                definition = term['description'][0]
            except:
                definition = None

            enti = entity(name=name,
                          iri=term['iri'],
                          obo_ID=term['short_form'],
                          ontoName=term['ontology_name'],
                          provenance_name=term['ontology_prefix'],
                          definition=definition)
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
                ontoName = getOnto_Name(iri)

            enti = entity(name=term['prefLabel'],
                          iri=iri,
                          obo_ID=iri.rsplit('/', 1)[-1],
                          ontoName=ontoName)
            res.append(enti)
            iri_record.append(iri)
            if len(res) >= 5:
                break
    except Exception as e:
        logger.error('getBioportal' + str(e))
    return res


def getOnto_Name(iri):
    # get ontology name by giving iri of entity
    substring = iri.rsplit('/', 1)[-1]
    return ''.join(x for x in substring if x.isalpha())


def getOnto_title(pre_fix):
    try:
        url = 'https://www.ebi.ac.uk/ols/api/ontologies/' + pre_fix
        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        j_content = json.loads(content)
        return j_content['config']['title']
    except:
        return ''


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


def setPriority(res_list):
    priority = {'MTBLS': 0, 'NCBITAXON': 1, 'BTO': 2, 'EFO': 3, 'CHEBI': 4, 'CHMO': 5, 'NCIT': 6, 'PO': 7}
    res = {}
    for enti in res_list:
        term_name = enti.name.lower()
        onto_name = enti.ontoName
        prior = priority.get(onto_name, 1000)

        if term_name in res:
            old_prior = priority.get(res[term_name].ontoName, 999)

            if prior < old_prior:
                res[term_name] = enti
        else:
            res[term_name] = enti

    return list(res.values())


def reorder(res_list, keyword):
    def sort_key(s, keyword):
        exact = s.lower() == keyword.lower()
        start = s.startswith(keyword)
        partial = keyword in s
        return exact, start, partial

    res = sorted(res_list, key=lambda x: sort_key(x.name, keyword), reverse=True)
    return res


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