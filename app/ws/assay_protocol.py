#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Mar-15
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

from flask import request, abort, jsonify
from flask_restful import Resource
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import *
from flask_restful_swagger import swagger
import logging

logger = logging.getLogger('wslog')
iac = IsaApiClient()
wsc = WsClient()


class GetProtocolForAssays(Resource):
    @swagger.operation(
        summary="Get protocol and assay mappings for a study",
        nickname="Get assays linked to protocols for a given study",
        notes="For each assay get an overview of the protocols referenced",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
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
                "message": "OK. The TSV table is returned"
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
    def get(self, study_id):
        # param validation
        if study_id is None:
            logger.info('No study_id given')
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info('Assay Table mapping: Getting ISA-JSON Study %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

        all_protocols = []
        for protocol in isa_study.protocols:
            prot_name = protocol.name
            if prot_name == 'Sample collection':
                continue  # Skip as this is not reported in the assay, but rather in the sample sheet

            prot_ref = "Protocol REF"
            prot_list = []

            for assay in isa_study.assays:
                file_name = os.path.join(study_location, assay.filename)

                logger.info('Trying to load TSV file (%s) for Study %s', file_name, study_id)
                # Get the Assay table or create a new one if it does not already exist
                try:
                    file_df = read_tsv(file_name)
                except FileNotFoundError:
                    abort(400, "The file " + file_name + " was not found")

                all_rows_dict = totuples(file_df.reset_index(), 'rows')
                all_rows = all_rows_dict['rows']
                row = all_rows_dict['rows'][0]  # Get row 1
                # row.pop('index')  # Remove the additional 'index' column

                # Next we look for all occurrences of "Protocol REF" columns, as these are the protocol deliminators
                idx = 0  # This is the index (position) of the column. This is needed for later updates
                full_column_list = []
                for key, value in row.items():

                    if key == 'Sample Name' or key == 'index':
                        idx += 1
                        continue  # No need for this as this is not part of an assay protocol, but links to samples

                    if key.startswith(prot_ref) and value == prot_name:
                        correct_protocol = True  # So, we are under the correct protocol section of the assay
                    elif key.startswith(prot_ref) and value != prot_name:
                        correct_protocol = False

                    if correct_protocol:
                        column_list = {"column": key}
                        # Todo, all unique values in this column, add []
                        unique_entries = [value]

                        for row in all_rows:
                            for key2, value2 in row.items():
                                if key == key2:
                                    if value2 not in unique_entries:
                                        unique_entries.append(value2)

                        column_list.update({"data": unique_entries})
                        column_list.update({"index": str(idx)})
                        full_column_list.append(column_list)

                    idx += 1

                prot_list.append({assay.filename: full_column_list})

            all_protocols.append({prot_name: prot_list})

        return jsonify({"protocols": all_protocols})



