#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Apr-17
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

import logging

from flask import request
from flask_restful import Resource, abort
from flask_restful_swagger import swagger

from app.utils import metabolights_exception_handler
from app.ws.auth.permissions import validate_submission_update, validate_submission_view
from app.ws.db_connection import biostudies_acc_to_mtbls, biostudies_accession
from app.ws.mtblsWSclient import WsClient

logger = logging.getLogger("wslog")
wsc = WsClient()


class BioStudies(Resource):
    @swagger.operation(
        summary="Get the BioStudies accession mapped to this study",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def get(self, study_id):
        result = validate_submission_view(request)
        study_id = result.context.study_id

        _, data = biostudies_accession(study_id, None, method="query")

        return {"BioStudies": data[0]}

    @swagger.operation(
        summary="Add a BioStudies accession to this study",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "biostudies_acc",
                "description": "BioStudies accession",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def post(self, study_id):
        result = validate_submission_update(request)
        study_id = result.context.study_id
        # query validation
        biostudies_acc = request.args.get("biostudies_acc")

        if biostudies_acc is None:
            abort(404)

        _, data = biostudies_accession(study_id, biostudies_acc, method="add")

        return {"BioStudies": data[0]}

    @swagger.operation(
        summary="Remove the BioStudies accession mapped to this study",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def delete(self, study_id):
        result = validate_submission_update(request)
        study_id = result.context.study_id

        status, data = biostudies_accession(study_id, None, method="delete")

        return {"BioStudies": data[0]}


class BioStudiesFromMTBLS(Resource):
    @swagger.operation(
        summary="Get any MTBLS accessions mapped to this BioStudies accession",
        parameters=[
            {
                "name": "biostudies_acc",
                "description": "BioStudies accession",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def get(self):
        result = validate_submission_view(request)
        study_id = result.context.study_id

        biostudies_acc = request.args.get("biostudies_acc")
        if biostudies_acc is None:
            abort(404)

        study_id = biostudies_acc_to_mtbls(biostudies_acc)
        if not study_id:
            abort(403, message=f"No study id for {biostudies_acc}")

        return {"BioStudies": study_id}
