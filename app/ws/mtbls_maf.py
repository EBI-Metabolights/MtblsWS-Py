#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Jan-08
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

import json
import logging
import os

import pandas as pd

from flask import request
from flask_restful import Resource, reqparse, abort
from flask_restful_swagger import swagger

from app.ws.mtblsWSclient import WsClient
from app.ws.report_builders.combined_maf_builder import CombinedMafBuilder
from app.ws.settings.utils import get_study_settings
from app.ws.study.user_service import UserService
from app.ws.utils import create_maf, read_tsv, write_tsv

logger = logging.getLogger('wslog')
# MetaboLights (Java-Based) WebService client
wsc = WsClient()


# Convert panda DataFrame to json tuples object
def totuples(df, text) -> dict:
    d = [
        dict([
            (colname, row[i])
            for i, colname in enumerate(df.columns)
        ])
        for row in df.values
    ]
    return {text: d}


def get_table_header(table_df):
    # Get an indexed header row
    df_header = pd.DataFrame(list(table_df))  # Get the header row only
    df_header = df_header.reset_index().to_dict(orient='list')
    mapping = {}
    print(df_header)
    for i in range(0, len(df_header['index'])):
        mapping[df_header[0][i]] = df_header['index'][i]
    return mapping


def insert_row(idx, df, df_insert):
    return  pd.concat([df.iloc[:idx, ], df_insert, df.iloc[idx:, ]], ignore_index=True).reset_index(drop=True)


class MtblsMAFSearch(Resource):
    """Get MAF from studies (assays)"""
    @swagger.operation(
        summary="Search for metabolite onto_information to use in the Metabolite Annotation file",
        nickname="MAF search",
        notes="Get a given MAF associated with assay {assay_id} for a MTBLS Study with {study_id} in JSON format",
        parameters=[
            {
                "name": "query_type",
                "description": "The type of data to search for",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
                "enum": ["name", "databaseid", "inchi", "smiles"]
            },
            {
                "name": "search_value",
                "description": "The search string",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The metabolite search result is returned"
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
    def get(self, query_type):
        # param validation
        if query_type is None:
            abort(404)

        
        
        search_value = None
        if request.args:
            
            search_value = request.args.get('search_value')

        if search_value is None:
            abort(404)

        search_res = wsc.get_maf_search(query_type, search_value)
        return search_res


class MetaboliteAnnotationFile(Resource):
    @swagger.operation(
        summary="[Deprecated] Read, and add missing samples for a MAF",
        nickname="Get MAF for a given MTBLS Assay",
        notes='''Create or update a Metabolite Annotation File for an assay.
<pre><code> 
{  
  "data": [ 
    { "assay_file_name": "a_some_assay_file.txt" },
    { "assay_file_name": "a_some_assay_file-1.txt" } 
  ]
}
</code></pre>''',
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
                "name": "data",
                "description": "Assay File names",
                "required": True,
                "allowMultiple": False,
                "paramType": "body",
                "dataType": "string"
            },
            {
                "name": "user-token",
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
                "message": "OK. The Metabolite Annotation File (MAF) is returned"
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
                "message": "Incorrect parameters provided"
            }
        ]
    )
    def post(self, study_id):
        data_dict = json.loads(request.data.decode('utf-8'))
        assay_file_names = data_dict['data']

        # param validation
        if study_id is None:
            abort(417)

        # param validation
        if assay_file_names is None:
            abort(417, message='Please ensure the JSON has at least one "assay_file_name" element')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info('MAF: Getting ISA-JSON Study %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location_deprecated, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        maf_feedback = ""
        study_location = os.path.join(get_study_settings().mounted_paths.study_metadata_files_root_path, study_id)
        for assay_file_name in assay_file_names:
            annotation_file_name = None
            assay_file = assay_file_name['assay_file_name']
            full_assay_file_name = os.path.join(study_location, assay_file)
            if not os.path.isfile(full_assay_file_name):
                abort(406, message="Assay file " + assay_file + " does not exist")
            assay_df = read_tsv(full_assay_file_name)
            annotation_file_name = assay_df['Metabolite Assignment File'].iloc[0]

            maf_df, new_annotation_file_name, new_column_counter = \
                create_maf(None, study_location, assay_file, annotation_file_name=annotation_file_name)
            if annotation_file_name != new_annotation_file_name:
                assay_df['Metabolite Assignment File'] = new_annotation_file_name
                write_tsv(assay_df, full_assay_file_name)
                annotation_file_name = new_annotation_file_name

            if maf_df.empty:
                abort(406, message="MAF file could not be created or updated")

            maf_feedback = maf_feedback + ". New row(s):" + str(new_column_counter) + " for assay file " + \
                            annotation_file_name

        return {"success": "Added/Updated MAF(s)" + maf_feedback}

class CombineMetaboliteAnnotationFiles(Resource):
    @swagger.operation(
        summary="[Deprecated] Combine MAFs for a list of studies",
        nickname="Combine MAF files",
        notes='''Combine MAF files for a given list of studies, by analytical method. If a study contains assays with 
        more than one analytical method, it will endeavour to try and select only the MAFs that correspond to the chosen 
        analytical method.
    <pre><code> 
    {  
      "data": [ 
        "MTBLS1",
        "MTBLS2",
        "MTBLS3"
      ],
      "method": "NMR"
    }
    </code></pre>''',
        parameters=[
            {
                "name": "data",
                "description": "Studies to combine MAFs for, and the analytical method",
                "required": True,
                "allowMultiple": False,
                "paramType": "body",
                "dataType": "string"
            },
            {
                "name": "user-token",
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
                "message": "OK. The Metabolite Annotation File (MAF) is returned"
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
                "message": "Incorrect parameters provided"
            }
        ]
    )
    def post(self):
        data_dict = json.loads(request.data.decode('utf-8'))
        studies_to_combine = data_dict['data']
        method = data_dict['method']

        if studies_to_combine is None:
            abort(417)

        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        
        UserService.get_instance().validate_user_has_curator_role(user_token)
        combiBuilder = CombinedMafBuilder(
            studies_to_combine=studies_to_combine,
            method=method
        )

        combiBuilder.build()
        r_d = {
            'status': f'Combined Maf File Built, with {len(combiBuilder.unopenable_maf_register)} MAFs unable to be '
                      f'opened: {str(combiBuilder.unopenable_maf_register)}. There were '
                      f'{len(combiBuilder.missing_maf_register)} studies that had no MAF files: '
                      f'{str(combiBuilder.missing_maf_register)} .There were '
                      f'{len(combiBuilder.no_relevant_maf_register)} studies that had MAF files, but no '
                      f'MAF file matching the given analytical method {method}: '
                      f'{str(combiBuilder.no_relevant_maf_register)}'
        }
        # logging it out in the event of the swagger UI crashing as these are useful numbers
        logger.info(r_d['status'])
        return r_d
