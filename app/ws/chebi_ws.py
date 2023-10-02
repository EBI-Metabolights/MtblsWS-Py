import logging
import os
import io
import requests
from flask import request, abort, Response, current_app as app
from flask_restful import Resource
from flask_restful_swagger import swagger
from app.config import get_settings

from app.ws.chebi.types import SearchCategory, StarsCategory
from app.ws.chebi.wsproxy import ChebiWsException, get_chebi_ws_proxy
from app.ws.utils import log_request

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
        summary="Get Chebi ID with name",
        nickname="Get Chebi ID with name",
        notes="Get Chebi ID with name",
        parameters=[
            {
                "name": "compound_name",
                "description": "Compound Name",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            }
        ],
        responseMessages=responseMessages
    )
    def get(self, compound_name):
        log_request(request)

        if not compound_name:
            abort(400, "Invalid compound name")

        if not get_chebi_ws_proxy():
            abort(501, "Remote server error")

        try:
            search_result = get_chebi_ws_proxy().get_lite_entity_list(compound_name.lower(),
                                                                         SearchCategory.ALL_NAMES, 20,
                                                                         StarsCategory.ALL)

            if not search_result:
                return abort(404, f"Entity not found with name {compound_name}")

            result = list()
            for search in search_result:
                result.append(search.dict())
            return result
        except ChebiWsException as e:
            abort(501, "Remote server error")


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
            abort(400, "Invalid ChEBI id")

        if not get_chebi_ws_proxy():
            abort(501, "Remote server error")

        try:
            search_result = get_chebi_ws_proxy().get_complete_entity(chebi_id)

            if not search_result:
                return abort(404, f"Entity not found with ChEBI id {chebi_id}")

            return search_result.dict()
        except ChebiWsException as e:
            abort(501, "Remote server error", e.message)


class ChebiImageProxy(Resource):

    @swagger.operation(
        summary="Get image by chebi id",
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
            abort(404, "invalid chebi id")
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
            abort(404, f"{str(exc)}")