#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Aug-21
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

from flask_restful import Resource, abort
from flask_restful_swagger import swagger
from app.config import get_settings
from app.ws.db_connection import mtblc_on_chebi_accession
import os


class EnzymePortalHelper(Resource):
    @swagger.operation(
        summary="Check if a ChEBI compound is in MetaboLights",
        notes="Search using standard ChEBI accessions, like CHEBI:16345",
        parameters=[
            {
                "name": "chebi_id",
                "description": "ChEBI Compound Identifier",
                "required": True,
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
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self, chebi_id):

        # param validation
        if chebi_id is None:
            abort(404)

        status, data = mtblc_on_chebi_accession(chebi_id.upper())
        if not status:
            abort(404, message=str(data))

        mtblc = data[0]
        mtblc_url = get_settings().server.service.ws_app_base_link
        mtblc_url = mtblc_url + os.sep + mtblc + os.sep + '#biology'

        return {"metabolights_id": data[0], "metabolights_url": mtblc_url}
