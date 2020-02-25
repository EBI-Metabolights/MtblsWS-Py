#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Feb-14
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
import numpy as np
import pandas as pd

from flask import request, abort
from flask.json import jsonify
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import logger, log_request, read_tsv
from app.ws.isaApiClient import IsaApiClient


logger = logging.getLogger('wslog')
wsc = WsClient()
iac = IsaApiClient()

def diff_pd(df1, df2):
   # https://stackoverflow.com/questions/17095101/outputting-difference-in-two-pandas-dataframes-side-by-side-highlighting-the-d
    """Identify differences between two pandas DataFrames"""
    assert (df1.columns == df2.columns).all(), \
        "DataFrame column names are different"
    if any(df1.dtypes != df2.dtypes):
        "Data Types are different, trying to convert"
        df2 = df2.astype(df1.dtypes)
    if df1.equals(df2):
        return None
    else:
        # need to account for np.nan != np.nan returning True
        diff_mask = (df1 != df2) & ~(df1.isnull() & df2.isnull())
        ne_stacked = diff_mask.stack()
        changed = ne_stacked[ne_stacked]
        changed.index.names = ['id', 'col']
        difference_locations = np.where(diff_mask)
        changed_from = df1.values[difference_locations]
        changed_to = df2.values[difference_locations]
        return pd.DataFrame({'from': changed_from, 'to': changed_to},
                            index=changed.index)

class CompareTsvFiles(Resource):
    @swagger.operation(
        summary="Find the difference between two tsv (ISA-Tab) files",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "filename1",
                "description": "TSV filename one",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "filename2",
                "description": "TSV filename two",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
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
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Study does not exist or your do not have access to this study."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self, study_id):

        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']

        if user_token is None:
            abort(401)

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('filename1', help='TSV filename one')
        parser.add_argument('filename2', help='TSV filename two')
        assay_filename = None
        if request.args:
            args = parser.parse_args(req=request)
            filename1 = args['filename1'].lower() if args['filename1'] else None
            filename2 = args['filename2'].lower() if args['filename2'] else None
        if not filename1 or not filename2:
            logger.warning("Missing TSV filenames.")
            abort(404, "Missing TSV filenames.")

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(401, "Study does not exist or your do not have access to this study.")

        location = study_location
        df1 = read_tsv(filename1)
        df2 = read_tsv(filename2)
        diff_df = diff_pd(df1, df2)
        return jsonify({"entries": diff_df})
