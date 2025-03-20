#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-May-08
#  Modified by:   Jiakang
#
#  Copyright 2019 EMBL - European Bioinformatics Institute
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#

import logging
from flask import request, jsonify
from flask_restful import Resource, reqparse, abort
from flask_restful_swagger import swagger
from app.config import get_settings
from app.study_folder_utils import convert_relative_to_real_path
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.ontology_info import MetaboLightsOntology, filter_term_definition, filter_term_label, load_ontology_file

logger = logging.getLogger('wslog')
iac = IsaApiClient()
wsc = WsClient()



class MtblsOntologyTerms(Resource):

    @swagger.operation(
        summary="Get Metabolights MTBLS controlled vocabolary and terms.",
        notes="Get MTBLS controlled vocabolary and terms.",
        parameters=[
            {
                "name": "keyword",
                "description": "Search keyword (e.i., ion)",
                "required": False,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "case_insensitive",
                "description": "Search in case insensitive mode.",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "Boolean",
                "defaultValue": True,
                "default": True
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
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self,):        
        
        query = request.args.get('keyword') if request.args.get('keyword') else None
        case_insensitive = True if request.args.get('case_insensitive') and request.args.get('case_insensitive').lower() == "true" else False
        
        filepath = convert_relative_to_real_path(get_settings().file_resources.mtbls_ontology_file)
        mtbl_ontology: MetaboLightsOntology = load_ontology_file(filepath)
        prefix = "http://www.ebi.ac.uk/metabolights/ontology"
        limit = 20
        result = [] 
        for filter_method in (filter_term_label, filter_term_definition):
            terms = mtbl_ontology.search_ontology_entities(label=query, include_contain_matches=True, include_case_insensitive_matches=case_insensitive, limit=limit, filter_method=filter_method)
            result = [{"termAccessionNumber": term.iri, 
                       "term": term.name, 
                       "termDescription": term.definition, 
                       "termSourceRef": "MTBLS" }
                      for term  in terms
                      if term.iri.startswith(prefix)
                      ]
            if len(result) > limit:
                break
                         
        return jsonify(result)
        

class MtblsOntologyTerm(Resource):

    @swagger.operation(
        summary="Get Metabolights MTBLS controlled vocabolary and terms.",
        notes="Get MTBLS controlled vocabolary and terms.",
        parameters=[
            {
                "name": "term_id",
                "description": "MTBLS term id (e.i., MTBLS_000003)",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "path",
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
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self, term_id: str):
        

        # 
        
        
        # query = request.args.get('q') if request.args.get('q') else None
        filepath = convert_relative_to_real_path(get_settings().file_resources.mtbls_ontology_file)
        mtbl_ontology: MetaboLightsOntology = load_ontology_file(filepath)
        prefix = "http://www.ebi.ac.uk/metabolights/ontology"

        search_uri = f"{prefix}/{term_id}"
        if search_uri in mtbl_ontology.entities:
            term = mtbl_ontology.entities[search_uri] 
            return jsonify({"termAccessionNumber": term.iri, "term": term.label, "termDescription": term.description, "termSourceRef": "MTBLS"})
        abort(404, message=f"{term_id} is not defined in MetaboLights ontology")