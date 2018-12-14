# ontology
# Created by JKChang
# 05/07/2018, 15:05
# Tag:
# Description:
import datetime
import json
import logging
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
                "enum": ["typo", "exact"]
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

        parser.add_argument('branch', help='Starting branch of ontology')
        branch = None
        if request.args:
            args = parser.parse_args(req=request)
            branch = args['branch']

        parser.add_argument('mapping', help='Mapping approcaches')
        mapping = None
        if request.args:
            args = parser.parse_args(req=request)
            mapping = args['mapping']

        if term is None and branch is None:
            return []


        # Onto loading
        logger.info('Getting Ontology term %s', term)
        onto = get_ontology('./tests/Metabolights.owl').load()
        info = onto_information(onto)

        result = []

        # if only branch, search all branch

        if not term and branch:

            start_cls = onto.search_one(label=branch)
            if not start_cls:  # Todo, Fix this
                return []
            clses = info.get_subs(start_cls)

            for cls in clses:
                enti = entity(name=cls.label[0], iri=cls.iri, ontoName='MTBLS')
                result.append(enti)

            if len(result) > 0:
                result.insert(0, result.pop())

            if branch == 'instruments':
                r = OLSbranchSearch("*", 'instrument', 'msio')
                result = result + r

            if branch == 'column type':
                r = OLSbranchSearch("*", 'chromatography', 'chmo')
                result = result + r

        # if keyword !=  null
        elif term:
            if branch:
                start_cls = onto.search_one(label=branch)
            else:
                start_cls = onto.search_one(iri='http://www.w3.org/2002/07/owl#Thing')

            if not start_cls:  # Todo, Fix this
                return []

            clses = info.get_subs(start_cls)
            res_cls = []

            if branch == 'instruments':
                result = OLSbranchSearch(term, 'instrument', 'msio')

            if branch == 'column type':
                result = OLSbranchSearch(term, 'chromatography', 'chmo')

            # taxonomy
            if branch == 'taxonomy' or branch == 'factors':
                for cls in clses:
                    try:
                        map = IRIS['http://www.geneontology.org/formats/oboInOwl#hasExactSynonym']
                        Synonym = list(map[cls])
                        if term.lower() in [syn.lower() for syn in Synonym]:
                            res_cls.append(cls)
                    except:
                        pass

            # exact match
            if term.lower() in [cls.label[0].lower() for cls in clses]:
                if onto.search_one(label=term.lower()) is not None:
                    c = onto.search_one(label=term.lower())
                elif onto.search_one(label=term.title()) is not None:
                    c = onto.search_one(label=term.title())
                elif onto.search_one(label=term.capitalize()) is not None:
                    c = onto.search_one(label=term.capitalize())
                elif onto.search_one(label=term.upper()) is not None:
                    c = onto.search_one(label=term.upper())
                else:
                    for cls in clses:
                        if cls.label[0].lower() == term.lower():
                            c = cls

                subs = info.get_subs(c)
                res_cls = [c] + subs

                for cls in res_cls:
                    enti = entity(name=cls.label[0], iri=cls.iri, obo_ID=cls.name, ontoName='MTBLS')
                    result.append(enti)

            # fuzzy match
            if len(result) == 0:
                for cls in clses:
                    if cls.label[0].lower().startswith(term.lower()):
                        res_cls.append(cls)
                        subs = info.get_subs(cls)
                        res_cls = res_cls + subs

                for cls in clses:
                    if term.lower() in cls.label[0].lower() and cls not in res_cls:
                        res_cls.append(cls)

                for cls in res_cls:
                    enti = entity(name=cls.label[0], iri=cls.iri, obo_ID=cls.name, ontoName='MTBLS')
                    result.append(enti)

            # if branch == null, search whole ontology
            if len(result) == 0:
                try:
                    if onto.search_one(label=term.lower()) is not None:
                        c = onto.search_one(label=term.lower())
                    elif onto.search_one(label=term.title()) is not None:
                        c = onto.search_one(label=term.title())
                    else:
                        c = onto.search_one(label=term.upper())
                    enti = entity(name=c.label[0], iri=c.iri, ontoName='MTBLS')
                    result.append(enti)
                except Exception as e:
                    print(e.args)
                    logger.info(e.args)

            # metabolights-zooma.tsv search
            if len(result) == 0:
                print("Can't find query in MTBLS ontology, search metabolights-zooma.tsv")
                logger.info("Can't find query in MTBLS ontology, search metabolights-zooma.tsv")
                try:
                    result = getMetaboZoomaTerm(term)
                except Exception as e:
                    print(e.args)
                    logger.info(e.args)

            # Zooma Search
            if len(result) == 0:
                print("Can't find query inmetabolights-zooma.tsv, requesting Zooma")
                logger.info("Can't find query in MTBLS ontology, requesting Zooma")
                try:
                    temp = getZoomaTerm(term)
                    for t in temp:
                        if t.Zooma_confidence in ['GOOD', 'HIGH']:
                            result.append(t)
                except Exception as e:
                    print(e.args)
                    logger.info(e.args)

            # OLS Search
            if len(result) == 0:
                print("Can't query it in Zooma, requesting OLS")
                logger.info("Can't query it in Zooma, requesting OLS")
                try:
                    result = getOLSTerm(term)
                except  Exception as e:
                    print(e.args)
                    logger.info(e.args)

            # Bioportal Search
            if len(result) == 0:
                print("Can't query it in OLS, request Bioportal")
                try:
                    result = getBioportalTerm(term)
                except  Exception as e:
                    print(e.args)
                    logger.info(e.args)
                    print("Can't query it in Bioportal")

        else:
            print('Error')

        response = []

        result = removeDuplicated(result)

        for cls in result:
            temp = '''    {
                    "comments": [],
                    "annotationValue": "investigator",
                    "termSource": {
                        "comments": [],
                        "ontology_name": "",
                        "file": "",
                        "provenance_name":"",
                        "version": "",
                        "description": ""
                    },
                    "termAccession": ""
                }'''

            d = json.loads(str(temp))
            try:
                d['annotationValue'] = str(cls.name)
                d["termAccession"] = str(cls.iri)
                d['termSource']['ontology_name'] = str(cls.ontoName)
                d['termSource']['file'] = str(cls.provenance_uri)
                d['termSource']['provenance_name'] = str(cls.provenance_name)
                d['termSource']['version'] = str(getOnto_version(cls.ontoName))
                d['termSource']['description'] = str(getOnto_title(cls.ontoName))
            except:
                pass

            if cls.ontoName == 'MTBLS':
                d['termSource']['file'] = 'https://www.ebi.ac.uk/metabolights/'
                d['termSource']['provenance_name'] = 'metabolights'
                d['termSource']['version'] = '1.0'
                d['termSource']['description'] = 'Metabolights Ontology'

            if cls.provenance_name == 'metabolights-zooma':
                d['termSource']['version'] = str(datetime.datetime.now().date())
            response.append(d)

        # response = [{'SubClass': x} for x in res]
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

        read_access2, write_access2, obfuscation_code2, study_location2, release_date, submission_date, \
        study_status = wsc.get_permissions("MTBLS1", user_token)
        # TODO, add "is curator" to the return values. for now let's just see who can edit MTBLS1 = Curators only
        if not write_access2:
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


def OLSbranchSearch(query, branchName, ontoName):
    def getStartIRI(start, ontoName):
        url = 'https://www.ebi.ac.uk/ols/api/search?q=' + start + '&ontology=' + ontoName + '&queryFields=label'
        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        json_str = json.loads(content)
        # print(json_str['response']['docs'])
        res = json_str['response']['docs'][0]['iri']
        # return res
        return urllib.parse.quote_plus(res)

    res = []
    branchIRI = getStartIRI(branchName, ontoName)
    query = query.replace(' ', '%20')
    url = 'https://www.ebi.ac.uk/ols/api/search?q=' + query + '&rows=10&ontology=' + ontoName + '&allChildrenOf=' + branchIRI
    print(url)
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


def getMetaboZoomaTerm(keyword):
    res = []
    try:
        try:
            fileName = app.config.get('MTBLS_ZOOMA_FILE')  # metabolights_zooma.tsv
            df = pd.read_csv(fileName, sep="\t", header=0, encoding='utf-8')
            temp = df.loc[df['PROPERTY_VALUE'].str.contains(keyword, case=False)]

            for i in range(len(temp)):
                iri = temp.iloc[0]['SEMANTIC_TAG']
                if 'mesh' in iri.lower():
                    ontoName = 'MESH'
                elif 'nci' in iri.lower():
                    ontoName = 'NCIT'
                elif 'bao' in iri.lower():
                    ontoName = 'BAO'
                else:
                    ontoName = getOnto_Name(iri)

                name = temp.iloc[0]['PROPERTY_VALUE'].title()
                obo_ID = iri.rsplit('/', 1)[-1]
                ontoName = ontoName

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
    except Exception as e:
        logger.error('getMetaboZoomaTerm' + str(e))
    return res


def getZoomaTerm(keyword):
    res = []
    try:
        url = 'http://snarf.ebi.ac.uk:8480/spot/zooma/v2/api/services/annotate?propertyValue=' + keyword.replace(' ',
                                                                                                                 "+")
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

            enti = entity(name=term["annotatedProperty"]['propertyValue'].title(),
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
    res = []
    a = keyword
    try:
        # https://www.ebi.ac.uk/ols/api/search?q=lung&groupField=true&queryFields=label,synonym&fieldList=iri,label,short_form,obo_id,ontology_name,ontology_prefix
        url = 'https://www.ebi.ac.uk/ols/api/search?q=' + keyword.replace(' ', "+") + \
              '&groupField=true' \
              '&queryFields=label,synonym' \
              '&fieldList=iri,label,short_form,obo_id,ontology_name,ontology_prefix'  # '&exact=true' \
        fp = urllib.request.urlopen(url)
        content = fp.read().decode('utf-8')
        j_content = json.loads(content)
        responses = j_content["response"]['docs']

        for term in responses:
            enti = entity(name=term['label'].title(),
                          iri=term['iri'],
                          obo_ID=term['obo_id'],
                          ontoName=term['ontology_prefix'],
                          provenance_name=term['ontology_prefix'])
            res.append(enti)
            if len(res) >= 5:
                break

    except Exception as e:
        logger.error('getOLS' + str(e))
    return res


def getBioportalTerm(keyword):
    res = []
    try:
        url = 'http://data.bioontology.org/search?q=' + keyword.replace(' ', "+")  # + '&require_exact_match=true'
        request = urllib.request.Request(url)
        request.add_header('Authorization', 'apikey token=c60c5add-63c6-4485-8736-3f495146aee3')
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


def removeDuplicated(res_list):
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
