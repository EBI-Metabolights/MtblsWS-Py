import json
import logging
import os
import io
import requests
from flask import request, Response
from flask_restful import Resource, abort
from flask_restful_swagger import swagger
from app.config import get_settings

from app.ws.chebi.chebi_utils import chebi_search_v2, get_complete_chebi_entity_v2
from app.ws.chebi.wsproxy import ChebiWsException
from app.ws.utils import log_request
from app.ws.chebi_pipeline_utils import get_all_ontology_children_in_path

logger = logging.getLogger(__file__)


responseMessages = [
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
    },
    {
        "code": 501,
        "message": "Server error."
    },
]


class ChebiLiteEntity(Resource):

    @swagger.operation(
        summary="Get Chebi ID with InchiKey",
        nickname="Get Chebi ID with InchiKey",
        notes='''Add a new MetaboLights user account<pre><code>
    { 
         "query": 
            {
                "inchi_keys": ["RYYVLZVUVIJVGH-UHFFFAOYSA-N","RZVAJINKPMORJF-UHFFFAOYSA-N"],
                "names":["paracetamol","caffeine","4-acetamidophenol"]
            }
    }</pre></code>
    </p>For the names you can pass Chebi name, compound name, synonym, IUPAC NAME,  brand name''',
        parameters=[
            {
                "name": "query",
                "description": "Input as an object to query Chebi2 WS",
                "required": False,
                "allowMultiple": False,
                "paramType": "body",
                "type": "string",
                "format": "application/json"
            }
        ],
        responseMessages=responseMessages
    )
    def post(self):
        log_request(request)
        try:
            inchi_key_result = []
            name_search_result = []
            data_dict = json.loads(request.data.decode('utf-8'))
            query = data_dict['query']
            if query:
                inchi_keys = query['inchi_keys']
                if inchi_keys:
                    for inchi_key in inchi_keys:
                        search_result = chebi_search_v2(search_term=inchi_key)
                        inchi_key_result.append({inchi_key:search_result})
                names = query['names']
                if names:
                    for name in names:
                        search_result = chebi_search_v2(search_term=name)
                        name_search_result.append({name:search_result})
                
        except ChebiWsException as e:
            abort(501, message="Remote server error")
        result = {"search_results": inchi_key_result,"name_search_result":name_search_result}
        return result

class ChebiEntity(Resource):
    @swagger.operation(
        summary="Get details of compound with Chebi ID",
        nickname="Get details of compound with Chebi ID",
        notes="Get details of compound with Chebi ID",
        parameters=[
            {
                "name": "chebi_id",
                "description": "ChEBI ID",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            }
        ],
        responseMessages=responseMessages
    )
    def get(self, chebi_id):
        log_request(request)

        if not chebi_id:
            abort(400, message="Invalid ChEBI id")
        try:
            complete_entity = get_complete_chebi_entity_v2(chebi_id=chebi_id)
            if not complete_entity:
                return abort(404, message=f"Entity not found with ChEBI id {chebi_id}")
            return complete_entity
        except ChebiWsException as e:
            abort(501, message=f"Remote server error {e.message}")

class ChebiOntologyChildren(Resource):
    @swagger.operation(
        summary="Get all ontology children in path with Chebi ID",
        nickname="Get all ontology children in path with Chebi ID",
        notes="Get all ontology children in path with Chebi ID",
        parameters=[
            {
                "name": "acid_chebi_id",
                "description": "ChEBI ID",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            }, 
            {
                "name": "relation",
                "description": "Ontology relation",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["has_functional_parent", "has_parent_hydride", "has_part",
                         "has_role", "is_a", "is_conjugate_acid_of",
                         "is_conjugate_base_of", "is_enantiomer_of", "is_part_of", "is_substituent_group_from",
                         "is_tautomer_of"]
            },
            {
                "name": "three_star_only",
                "description": "if True, it will only include 3 stars compounds. Use False to get 2 and 3 stars entries..",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "type": "string",
                "defaultValue": "false",
                "enum": ["true","false"]
            }
        ],
        responseMessages=responseMessages
    )
    def get(self, acid_chebi_id):
        log_request(request)

        if not acid_chebi_id:
            abort(400, message="Invalid ChEBI id")
        relation = request.args.get('relation')
        relation = relation.strip()
        three_star_only = request.args.get('three_star_only')
        try:
            chebi_ids = get_all_ontology_children_in_path(acid_chebi_id=acid_chebi_id, relation=relation, three_star_only=three_star_only)
            if not chebi_ids:
                return abort(404, message=f"Children not found for ChEBI id {acid_chebi_id}")
            return chebi_ids
        except ChebiWsException as e:
            abort(501, message=f"Remote server error {e.message}")

class ChebiImageProxy(Resource):

    @swagger.operation(
        summary="[Deprecated] Get image by chebi id",
        nickname="Get image by chebi id",
        notes="Get image by chebi id",
        parameters=[
            {
                "name": "chebiIdentifier",
                "description": "chebiIdentifier without CHEBI: prefix",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            }
        ],
        responseMessages=responseMessages
    )
    def get(self, chebiIdentifier: str):
        image_name = chebiIdentifier
        chebiIdentifier = chebiIdentifier.replace(".png", "")
        
        if not chebiIdentifier.isnumeric() or len(chebiIdentifier) > 8:
            abort(404,message= "invalid chebi id")
        chebi_url = "https://www.ebi.ac.uk/chebi/displayImage.do"
        default_params = "defaultImage=true&imageIndex=0&dimensions=500&scaleMolecule=false"
        chebi_id_param = f"chebiId=CHEBI:{chebiIdentifier}"
        settings = get_settings()
        url = f"{chebi_url}?{default_params}&{chebi_id_param}"
        img_root_path = settings.chebi.caches.images_cache_path
        image_path = os.path.join(img_root_path, image_name)
        try: 
            with requests.get(url, stream=True) as r:
                with open(image_path, "wb") as f:
                    f.write(r.content)
                bytes = io.BytesIO(r.content)
                
                return Response(bytes, mimetype="image/png", direct_passthrough=True)

        except Exception as exc:
            abort(404, message=f"{str(exc)}")