import logging

from flask import request, abort, current_app as app
from flask_restful import Resource
from flask_restful_swagger import swagger

from app.ws.chebi.settings import get_chebi_ws_settings
from app.ws.chebi.types import SearchCategory, StarsCategory
from app.ws.chebi.wsproxy import ChebiWsProxy, ChebiWsException
from app.ws.utils import log_request

logger = logging.getLogger(__file__)

_chebi_proxy = ChebiWsProxy()


def get_chebi_proxy():
    if not _chebi_proxy.settings:
        _chebi_proxy.setup(get_chebi_ws_settings(app))
    return _chebi_proxy


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

        if not get_chebi_proxy():
            abort(501, "Remote server error")

        try:
            search_result = get_chebi_proxy().get_lite_entity_list(compound_name.lower(), SearchCategory.ALL_NAMES, 20,
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

        if not get_chebi_proxy():
            abort(400, "Invalid ChEBI id")

        if not get_chebi_proxy():
            abort(501, "Remote server error")

        try:
            search_result = get_chebi_proxy().get_complete_entity(chebi_id)

            if not search_result:
                return abort(404, f"Entity not found with ChEBI id {chebi_id}")

            return search_result.dict()
        except ChebiWsException as e:
            abort(501, "Remote server error")
