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
import os

import numpy as np
import pandas as pd
from flask import request
from flask_restful import Resource, abort
from flask_restful_swagger import swagger

from app.utils import metabolights_exception_handler
from app.ws.auth.permissions import validate_submission_view
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.study.utils import get_study_metadata_path
from app.ws.utils import log_request, read_tsv

logger = logging.getLogger("wslog")
wsc = WsClient()
iac = IsaApiClient()


def diff_pd(df1, df2):
    # https://stackoverflow.com/questions/17095101/outputting-difference-in-two-pandas-dataframes-side-by-side-highlighting-the-d
    """Identify differences between two pandas DataFrames"""
    if any(df1.columns != df2.columns):
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
        changed.index.names = ["id", "col"]
        difference_locations = np.where(diff_mask)
        changed_from = df1.values[difference_locations]
        changed_to = df2.values[difference_locations]
        return pd.DataFrame(
            {"from": changed_from, "to": changed_to}, index=changed.index
        )


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
                "dataType": "string",
            },
            {
                "name": "filename1",
                "description": "TSV filename one",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "filename2",
                "description": "TSV filename two",
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
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Study does not exist or your do not have access to this study.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def get(self, study_id):
        log_request(request)
        result = validate_submission_view(request)
        study_id = result.context.study_id
        study_location = get_study_metadata_path(study_id)

        filename1 = (
            request.args.get("filename1") if request.args.get("filename1") else None
        )
        filename2 = (
            request.args.get("filename2") if request.args.get("filename2") else None
        )
        if not filename1 or not filename2:
            logger.warning("Missing TSV filenames.")
            abort(404, message="Missing TSV filenames.")

        df1 = read_tsv(str(os.path.join(study_location, filename1)))
        df2 = read_tsv(str(os.path.join(study_location, filename2)))
        diff_df = diff_pd(df1, df2)
        return diff_df.to_json()
