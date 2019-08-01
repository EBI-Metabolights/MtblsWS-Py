#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Feb-26
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

import os
import glob
import logging
import pandas as pd
from flask_restful import Resource
from flask_restful_swagger import swagger
from flask import current_app as app, request, abort
from flask.json import jsonify
from app.ws.mtblsWSclient import WsClient
from app.ws.isaApiClient import IsaApiClient
from app.ws.utils import validate_mzml_files, convert_to_isa, copy_file, read_tsv, write_tsv, \
    update_correct_sample_file_name, get_year_plus_one
from app.ws.db_connection import update_release_date

wsc = WsClient()
iac = IsaApiClient()
logger = logging.getLogger('wslog')


class Metabolon(Resource):
    @swagger.operation(
        summary='Confirm all files are uploaded',
        notes='''Confirm that all raw/mzML files has been uploaded to this studies upload folder. </br>
        Files uploaded for clients will be added to the final study before templates are applied</br>
        </P> 
        This may take some time as mzML validation and conversion to ISA-Tab will now take place''',
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
            },
            {
                "code": 417,
                "message": "Unexpected result."
            }
        ]
    )
    def post(self, study_id):

        # param validation
        if study_id is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        message = {'Success': 'All conversion steps completed successfully'}

        # Validate all mzML files, in both study and upload folders
        # This method also copy files to the study folder and adds a new extension in the upload folder.
        try:
            val_message = 'Could not validate all the mzML files'
            val_status, val_message = validate_mzml_files(study_id, obfuscation_code, study_location)
        except:
            abort(417, val_message)

        # Adding the success to the final message we will return
        if val_status:
            message.update({'mzML validation': 'Successful'})
        else:
            abort(417, val_message)

        # Create ISA-Tab files using mzml2isa
        try:
            conv_message = 'Could not convert all the mzML files to ISA-Tab'
            conv_status, conv_message = convert_to_isa(study_location, '')
        except:
            abort(417, conv_message)

        if conv_status:
            message.update({'mzML2ISA conversion': 'Successful'})
        else:
            abort(417, conv_message)

        # Split the two pos/neg assays from Metabolon into 4
        try:
            split_message = 'Could not correctly split the assays'
            split_status, split_message = split_metabolon_assays(study_location, study_id)
        except:
            abort(417, split_message)

        if split_status:
            message.update({'Assay splitting': 'Successful'})
        else:
            abort(417, split_message)

        # copy Metabolon investigation file into the study folder
        try:
            copy_status, copy_message = copy_metabolon_template(study_id, user_token, study_location)
        except:
            abort(417, 'Could not copy the Metabolon template, the investigation file still needs replacing')

        if copy_status:
            message.update({'Investigation template replacement': 'Successful'})
        else:
            abort(417, copy_message)

        return jsonify(message)


def copy_metabolon_template(study_id, user_token, study_location):
    status, message = True, "Copied Metabolon template into study " + study_id
    template_study_id = app.config.get('PARTNER_TEMPLATE_METABOLON')
    invest_file = 'i_Investigation.txt'

    # Get the correct location of the Metabolon template study
    template_study_location = study_location.replace(study_id.upper(), template_study_id)
    template_study_location = os.path.join(template_study_location, invest_file)
    dest_file = os.path.join(study_location, invest_file)

    try:
        copy_file(template_study_location, dest_file)
    except:
        return False, "Could not copy Metabolon template into study " + study_id

    try:
        # Updating the ISA-Tab investigation file with the correct study id
        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id=study_id, api_key=user_token, skip_load_tables=True, study_location=study_location)

        isa_study.identifier = study_id  # Adding the study identifier

        # Also make sure the sample file is in the standard format of 's_MTBLSnnnn.txt'
        isa_study, sample_file_name = update_correct_sample_file_name(isa_study, study_location, study_id)

        # Set publication date to one year in the future
        study_date = get_year_plus_one(isa_format=True)
        isa_study.public_release_date = study_date

        # Updated the files with the study accession
        iac.write_isa_study(
            inv_obj=isa_inv, api_key=user_token, std_path=study_location,
            save_investigation_copy=False, save_samples_copy=False, save_assays_copy=False
        )

        try:
            update_release_date(study_id, study_date)
            wsc.reindex_study(study_id, user_token)
        except:
            logger.info("Could not updated database and re-index study " + study_id)
    except:
        return False, "Could not update Metabolon template for study " + study_id

    return status, message


def split_metabolon_assays(study_location, study_id):
    p_start = 'a__POS'
    n_start = 'a__NEG'
    end = '_m'
    pos = p_start + end
    neg = n_start + end
    sample_col = 'Sample Name'

    for a_files in glob.glob(os.path.join(study_location, 'a__*_metabolite_profiling_mass_spectrometry.txt')):
        if pos in a_files:
            p_assay = read_tsv(a_files)
            p_filename = a_files
            try:
                # split based on 'POSEAR' and 'POSLAT'
                write_tsv(p_assay.loc[p_assay[sample_col].str.contains('POSEAR')],
                          p_filename.replace(pos, p_start + '_1' + end))
                write_tsv(p_assay.loc[p_assay[sample_col].str.contains('POSLAT')],
                          p_filename.replace(pos, p_start + '_2' + end))
            except:
                return False, "Failed to generate 2 POSITIVE ISA-Tab assay files for study " + study_id

        elif neg in a_files:
            n_assay = read_tsv(a_files)
            n_filename = a_files
            try:
                # split based on 'NEG' and 'POL'
                write_tsv(n_assay.loc[n_assay[sample_col].str.contains('NEG')],
                          n_filename.replace(neg, n_start + '_1' + end))
                write_tsv(n_assay.loc[n_assay[sample_col].str.contains('POL')],
                          n_filename.replace(neg, n_start + '_2' + end))
            except:
                return False, "Failed to generate 2 NEGATIVE ISA-Tab assay files for study " + study_id

    status, message = True, "Generated 4 ISA-Tab assay files for study " + study_id

    return status, message
