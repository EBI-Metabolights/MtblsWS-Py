#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Jan-16
#  Modified by:   kenneth
#
#  Copyright 2020 EMBL - European Bioinformatics Institute
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

import logging
import os.path

from flask import request, abort
from flask_restful import Resource
from flask_restful_swagger import swagger
from app.ws.isaApiClient import IsaApiClient
from app.ws.db_connection import get_all_study_acc, create_maf_info_table, add_maf_info_data
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import read_tsv

logger = logging.getLogger('wslog')
wsc = WsClient()
iac = IsaApiClient()

complete_maf = []


class MAfStats(Resource):
    @swagger.operation(
        summary="Update MAF stats for all MetaboLights studies (curator only)",
        parameters=[
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
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication. "
                           "Please provide a study id and a valid user token"
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token"
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def post(self):

        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None:
            abort(401)

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions('MTBLS2', user_token)
        if not is_curator:
            abort(403)

        status = update_maf_stats(user_token)

        if status:
            return {'Success': "MAF statistics updated in database"}
        else:
            return {'Error': "MAF statistics could not be updated in database"}


def update_maf_stats(user_token):

    for acc in get_all_study_acc():
        study_id = acc[0]
        print(study_id)
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)

        try:
            isa_study, isa_inv, std_path = iac.get_isa_study(study_id=study_id, api_key=user_token,
                                                             skip_load_tables=True, study_location=study_location)
        except Exception as e:
            logger.error("Failed to load ISA-Tab files for study " + study_id + ". " + str(e))
            continue

        for assay in isa_study.assays:
            file_name = os.path.join(study_location, assay.filename)
            logger.info('Trying to load TSV file (%s) for Study %s', file_name, study_id)
            # Get the Assay table or create a new one if it does not already exist
            try:
                file_df = read_tsv(file_name)
            except Exception as e:
                logger.error("The file " + file_name + " was not found")
            try:
                assay_maf_name = file_df['Metabolite Assignment File'].iloc[0]
                if not assay_maf_name:
                    continue  # No MAF referenced in this assay
            except KeyError:
                continue  # No MAF column found in this assay

            maf_file_name = os.path.join(study_location, assay_maf_name)  # MAF sheet

            try:
                maf_df = read_tsv(maf_file_name)
            except Exception as e:
                logger.error("The file " + maf_file_name + " was not found")

            for idx, row in maf_df.iterrows():
                maf_row = {}
                try:
                    database_identifier = row['database_identifier']
                    metabolite_identification = row['metabolite_identification']
                    maf_row.update({"acc": study_id})
                    maf_row.update({"database_identifier": database_identifier})
                    maf_row.update({"metabolite_identification": metabolite_identification})
                    maf_row.update({"database_found": is_identified(database_identifier)})
                    maf_row.update({"metabolite_found": is_identified(database_identifier)})
                except Exception as e:
                    logger.error('MAF stats failed for ' + study_id + '. Error: ' + str(e))
                    continue

                complete_maf.append(maf_row)

    status, msg = update_database_stats()

    return status, msg


def update_database_stats():
    create_maf_info_table()  # Truncate, drop and create the database table

    for row in complete_maf:
        acc = row['acc']
        database_identifier = row['database_identifier']
        metabolite_identification = row['metabolite_identification']
        database_found = row['database_found']
        metabolite_found = row['metabolite_found']
        status, msg = add_maf_info_data(str(acc),
                                        str(database_identifier),
                                        str(metabolite_identification),
                                        str(database_found),
                                        str(metabolite_found))
        if not status:
            return status, msg

    return True, 'Database successfully updated'


def is_identified(identifier):
    unknown_list = "unknown", "un-known", "n/a", "un_known", "not known", "not-known", "not_known", "unidentified", \
                   "not identified", "unmatched"

    identified = 0
    if not identifier:
        return identified

    identifier = identifier.lower()
    if identifier in unknown_list:
        identified = 0
    else:
        identified = 1

    return identified






