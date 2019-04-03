import logging, json, pandas as pd, os
import numpy as np
from flask import request, abort
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import read_tsv, write_tsv
from app.ws.mtbls_maf import totuples, get_table_header

logger = logging.getLogger('wslog')
# MetaboLights (Java-Based) WebService client
wsc = WsClient()


def split_rows(maf_df):
    # Split rows with pipe-lines "|"
    new_maf = pd.DataFrame(explode(explode(explode(maf_df.values, 0), 1), 2), columns=maf_df.columns)
    return new_maf


def explode(v, i, sep='|'):
    v = v.astype(str)
    n, m = v.shape
    a = v[:, i]
    bslc = np.r_[0:i, i + 1:m]
    asrt = np.append(i, bslc).argsort()
    b = v[:, bslc]
    a = np.core.defchararray.split(a, sep)
    A = np.concatenate(a)[:, None]
    counts = [len(x) for x in a.tolist()]
    rpt = np.arange(n).repeat(counts)
    return np.concatenate([A, b[rpt]], axis=1)[:, asrt]


class SplitMaf(Resource):
    @swagger.operation(
        summary="MAF pipeline splitter",
        nickname="Add rows based on pipeline splitting",
        notes="Split a given Metabolite Annotation File based on pipelines in cells. "
              "A new MAF will be created with extension '.split'",
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
                "name": "annotation_file_name",
                "description": "Metabolite Annotation File name",
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
            }
        ]
    )
    def post(self, study_id, annotation_file_name):

        # param validation
        if study_id is None or annotation_file_name is None:
            abort(404, 'Please provide valid parameters for study identifier and annotation file name')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not is_curator:
            abort(403)

        annotation_file_name = os.path.join(study_location, annotation_file_name)
        maf_df = read_tsv(annotation_file_name)
        maf_len = len(maf_df.index)

        # Any rows to split?
        new_maf_df = split_rows(maf_df)
        new_maf_len = len(new_maf_df.index)

        if maf_len != new_maf_len:  # We did find |, so we create a new MAF
            write_tsv(new_maf_df, annotation_file_name + ".split")

        # Dict for the data (rows)
        df_data_dict = totuples(new_maf_df.reset_index(), 'rows')
        # Get an indexed header row
        df_header = get_table_header(new_maf_df)

        return {"maf_rows": maf_len, "new_maf_rows": new_maf_len, "header": df_header, "data": df_data_dict}