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

from app.ws.db_connection import get_all_study_acc, database_maf_info_table_actions, add_maf_info_data, \
    insert_update_data
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.settings.utils import get_study_settings
from app.ws.utils import read_tsv

logger = logging.getLogger('wslog')
wsc = WsClient()
iac = IsaApiClient()


class StudyStats(Resource):
    @swagger.operation(
        summary="Update sample, assay and maf stats for all MetaboLights studies (curator only)",
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

        if update_maf_stats(user_token):
            return {'Success': "Study statistics updated in database"}

        return {'Error': "Study statistics could not be updated in database"}


def update_maf_stats(user_token):

    #database_maf_info_table_actions()  # Truncate, drop and create the database table

    for acc in get_all_study_acc():
        study_id = acc[0]
        maf_len = 0
        sample_len = 0
        assay_len = 0
        print("------------------------------------------ " + study_id + " ------------------------------------------")

        try:
            database_maf_info_table_actions(study_id)
        except ValueError:
            logger.error("Failed to update database for " + study_id)
            continue
        
        study_metadata_location = os.path.join(get_study_settings().mounted_paths.study_metadata_files_root_path, study_id)
        try:
            isa_study, isa_inv, std_path = iac.get_isa_study(study_id=study_id, api_key=user_token,
                                                             skip_load_tables=True, study_location=study_metadata_location)
        except Exception as e:
            logger.error("Failed to load ISA-Tab files for study " + study_id + ". " + str(e))
            continue  # Cannot find the required metadata files, skip to the next study

        try:
            number_of_files = sum([len(files) for r, d, files in os.walk(study_metadata_location)])
        except:
            number_of_files = 0

        try:
            sample_file_name = isa_study.filename
            sample_df = read_tsv(os.path.join(study_metadata_location, sample_file_name))
            sample_len = sample_df.shape[0]
        except FileNotFoundError:
            logger.warning('No sample file found for ' + study_id)

        for assay in isa_study.assays:
            complete_maf = []
            file_name = os.path.join(study_metadata_location, assay.filename)
            logger.info('Trying to load TSV file (%s) for Study %s', file_name, study_id)
            # Get the Assay table or create a new one if it does not already exist
            try:
                assay_file_df = read_tsv(file_name)
            except Exception as e:
                logger.error("The file " + file_name + " was not found")
            try:
                assay_len = assay_len + assay_file_df.shape[0]
                assay_maf_name = assay_file_df['Metabolite Assignment File'].iloc[0]
                if not assay_maf_name:
                    continue  # No MAF referenced in this assay
            except Exception:
                logger.error("Error in identifying MAF column in assay")
                continue  # No MAF column found in this assay

            maf_file_name = os.path.join(study_metadata_location, assay_maf_name)  # MAF sheet

            if os.path.isfile(maf_file_name):
                try:
                    maf_df = read_tsv(maf_file_name)
                except Exception as e:
                    logger.error("The file " + maf_file_name + " was not found")

                print(study_id + " - Rows: " + str(len(maf_df)) + ". File: " + maf_file_name)
            else:
                print("Could not find file " + maf_file_name)
                continue

            maf_len = maf_len + maf_df.shape[0]

            for idx, row in maf_df.iterrows():
                maf_row = {}
                try:
                    database_identifier = row['database_identifier']
                    metabolite_identification = row['metabolite_identification']
                    maf_row.update({"acc": study_id})
                    maf_row.update({"database_identifier": database_identifier})
                    maf_row.update({"metabolite_identification": metabolite_identification})
                    maf_row.update({"database_found": is_identified(database_identifier)})
                    maf_row.update({"metabolite_found": is_identified(metabolite_identification)})
                except Exception as e:
                    logger.error('MAF stats failed for ' + study_id + '. Error: ' + str(e))
                    continue

                complete_maf.append(maf_row)

            status, msg = update_database_stats(complete_maf)  # Update once per MAF

        study_sql = "UPDATE STUDIES SET sample_rows = " + str(sample_len) + ", assay_rows = " + str(assay_len) + \
                    ", maf_rows = " + str(maf_len) + ", number_of_files = " + str(number_of_files) + \
                    " WHERE ACC = '" + str(study_id) + "';"

        status, msg = insert_update_data(study_sql)
        print("Database updated: " + study_sql)

    return status, msg


def update_database_stats(complete_maf_list):
    status = False
    msg = 'Database successfully updated'

    for row in complete_maf_list:
        acc = row['acc']
        acc = acc.strip()
        database_identifier = row['database_identifier']
        database_identifier = clean_string(database_identifier)
        metabolite_identification = row['metabolite_identification']
        metabolite_id = clean_string(metabolite_identification)
        if metabolite_id != metabolite_identification:
            # print('Compound name "' + metabolite_identification + '" changed to "' + metabolite_id + '"')
            metabolite_identification = metabolite_id
        database_found = row['database_found']
        database_found = clean_string(database_found)
        metabolite_found = row['metabolite_found']
        metabolite_found = clean_string(metabolite_found)
        status, msg = add_maf_info_data(acc, database_identifier, metabolite_identification,
                                        database_found, metabolite_found)
        if not status:
            return status, msg

    return status, msg


def clean_string(string):
    new_string = ""
    if string:
        new_string = str(string).strip().replace("'", "").replace("  ", " ").replace("\t", "").replace("*", "")
    return new_string


def is_identified(maf_identifier):
    unknown_list = "unknown", "un-known", "n/a", "un_known", "not known", "not-known", "not_known", "unidentified", \
                   "not identified", "unmatched", "0", "na", "nan"

    identified = '0'
    if not maf_identifier:
        return identified

    maf_ident = maf_identifier.lower()

    if 'unknown' in maf_ident or 'unk-' in maf_ident or 'x - ' in maf_ident or 'm/z' in maf_ident:
        return identified

    if maf_ident in unknown_list:
        identified = '0'
    else:
        identified = '1'

    return identified






