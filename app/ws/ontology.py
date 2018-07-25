# ontology
# Created by JKChang
# 05/07/2018, 15:05
# Tag:
# Description:
import json
import logging

from flask import current_app as app
from flask import request, jsonify
from flask_restful import Resource, abort, reqparse
from flask_restful_swagger import swagger
from owlready2 import get_ontology

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
                "required": True,
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
                "dataType": "string"
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
        if not term:
            abort(400)

        parser.add_argument('branch', help='Starting branch of ontology')
        branch = None
        if request.args:
            args = parser.parse_args(req=request)
            branch = args['branch']


        logger.info('Getting Ontology term %s', term)

        onto = get_ontology('./tests/Metabolights.owl').load()
        info = information(onto)

        # ---------------- FACTORS------------------------------
        res_cls = []

        if branch == 'factors' or branch == 'roles' or branch == 'taxonomy':
            start_cls = onto.search_one(label=branch)
            clses = info.get_subs(start_cls)


            if branch == 'factors':  # go seeAlso
                def find_factor(cluster, query):
                    for cls in cluster:
                        try:
                            factors = info.get_factors(cls)
                            if query in factors:
                                return cls
                        except:
                            pass
                    print('No factors for %s' % query)
                    return None

                if onto.search_one(label = term):
                    res_cls.append(onto.search_one(label = term))
                else:
                    res_cls.append(find_factor(clses,term))


            if branch == 'roles':  # go sub
                for cls in clses:
                    if str(cls.label[0]) == term:
                        res = info.get_subs(cls)
                        res.append(cls)
                        res_cls = res
                        break


            if branch == 'taxonomy':  # go super
                for cls in clses:
                    if str(cls.label[0]) == term:
                        res = info.get_supers(cls)
                        res_cls = res

        response = []

        for cls in res_cls:
            temp = '''    {
                    "comments": [],
                    "annotationValue": "investigator",
                    "termSource": {
                        "comments": [],
                        "name": "EFO",
                        "file": "http://data.bioontology.org/ontologies/EFO",
                        "version": "132",
                        "description": "Experimental Factor Ontology"
                    },
                    "termAccession": "http://www.ebi.ac.uk/efo/EFO_0001739"
                }'''

            d = json.loads(temp)
            d['annotationValue'] = cls.label
            d['name'] = cls.namespace.name
            response.append(d)

        # response = [{'SubClass': x} for x in res]
        return jsonify({"OntologyTerm": response})


