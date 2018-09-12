# ontology
# Created by JKChang
# 05/07/2018, 15:05
# Tag:
# Description:
import json
import logging

from flask import current_app as app
from flask import request, jsonify
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from owlready2 import *

from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.ontology_info import *

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
        summary="Get ontology information",
        notes="Get ontology information.",
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
                "enum": ["factors", "roles", "taxonomy", "characteristics", "publication","design descriptor","unit"]
            },

            {
                "name": "mapping",
                "description": "starting branch of ontology",
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

        # Onto loading
        logger.info('Getting Ontology term %s', term)

        onto = get_ontology('./tests/Metabolights.owl').load()
        info = information(onto)

        # Loading branch
        res_cls = []
        if branch:
            start_cls = onto.search_one(label=branch)
            clses = info.get_subs(start_cls)



            # Roles / Characteristics/ Publication/design descriptor/unit/factors
            if branch in ["roles", "characteristics", "publication","design descriptor","unit","factors"]:  # go sub
                if term:
                      for cls in clses:
                        if str(cls.label[0]) == term:
                            subs = info.get_subs(cls)
                            res_cls = [cls] + subs
                            break

                      if len(res_cls) == 0:
                            zoomaTerms = getZoomaTerm(term)
                            res_cls = zoomaTerms.keys()
                else: #if not keyword return the whole branch
                    res_cls = clses
            # taxonomy
            if branch == 'taxonomy' and term != None:
                if not mapping:
                    try:
                        res_cls.append(onto.search_one(label=term))
                    except:
                        print("can't find the term")
                        pass
                elif mapping == 'typo':
                    try:
                        c = onto.search_one(label=term)
                        map = IRIS['http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym']
                        res_cls = list(map[c])
                    except:
                        print("can't find the term")
                        pass

                elif mapping == 'exact':
                    try:
                        c = onto.search_one(label=term)
                        map = IRIS['http://www.geneontology.org/formats/oboInOwl#hasExactSynonym']
                        res_cls = list(map[c])
                    except:
                        print("can't find the term")
                        pass
                else:
                    res_cls = clses



        response = []

        for cls in res_cls:
            temp = '''    {
                    "comments": [],
                    "annotationValue": "investigator",
                    "termSource": {
                        "comments": [],
                        "name": " ",
                        "file": "http://data.bioontology.org/ontologies/EFO",
                        "version": "132",
                        "description": "Experimental Factor Ontology"
                    },
                    "termAccession": "http://www.ebi.ac.uk/efo/EFO_0001739"
                }'''

            d = json.loads(temp)
            try:
                d['annotationValue'] = str(cls.label[0])
                d['name'] = str(cls.namespace.name)
            except:
                d['annotationValue'] = cls
                d['name'] = mapping

            response.append(d)

        # response = [{'SubClass': x} for x in res]
        return jsonify({"OntologyTerm": response})


def getZoomaTerm(keyword):
    res = {}
    url = 'https://www.ebi.ac.uk/spot/zooma/v2/api/services/annotate?propertyValue=' + keyword.replace(' ', "+")
    fp = urllib.request.urlopen(url)
    content = fp.read()
    json_str = json.loads(content)
    for term in json_str:
        termName = term["annotatedProperty"]['propertyValue']
        termConfidence = term['confidence']
        termURL = term['semanticTags']
        res[termName] = termConfidence
    return res
