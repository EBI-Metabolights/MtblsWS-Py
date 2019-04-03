import logging, pandas as pd, os
import numpy as np
from flask import request, abort
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import read_tsv, write_tsv
from app.ws.mtbls_maf import totuples, get_table_header
from app.ws.isaApiClient import IsaApiClient
from app.ws.study_files import get_all_files_from_filesystem

logger = logging.getLogger('wslog')
# MetaboLights (Java-Based) WebService client
wsc = WsClient()

iac = IsaApiClient()


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


def check_maf_for_pipes(study_location, annotation_file_name):
    annotation_file_name = os.path.join(study_location, annotation_file_name)
    maf_df = read_tsv(annotation_file_name)
    maf_len = len(maf_df.index)

    # Any rows to split?
    new_maf_df = split_rows(maf_df)
    new_maf_len = len(new_maf_df.index)

    if maf_len != new_maf_len:  # We did find |, so we create a new MAF
        write_tsv(new_maf_df, annotation_file_name + ".split")

    return maf_df, maf_len, new_maf_df, new_maf_len


def search_and_update_maf(study_location, annotation_file_name):
    annotation_file_name = os.path.join(study_location, annotation_file_name)
    maf_df = read_tsv(annotation_file_name)
    maf_len = len(maf_df.index)

    # Any rows to split?
    new_maf_df = split_rows(maf_df)
    new_maf_len = len(new_maf_df.index)

    if maf_len != new_maf_len:  # We did find | so we have to use the new dataframe
        maf_df = new_maf_df

    standard_maf_columns = {"database_identifier": 0, "chemical_formula": 1, "smiles": 2, "inchi": 3}
    maf_compound_name_column = "metabolite_identification"

    # Remove existing row values first, because that's what we do ;-)
    for column_name in standard_maf_columns:
        maf_df.iloc[:, standard_maf_columns[column_name]] = ""

    row_idx = 0
    # Search using the compound name
    for idx, comp_name in enumerate(maf_df[maf_compound_name_column]):

        search_res = wsc.get_maf_search("name", comp_name)
        if search_res['content']:
            name = None
            result = search_res['content'][0]
            database_identifier = result["databaseId"]
            chemical_formula = result["formula"]
            smiles = result["smiles"]
            inchi = result["inchi"]
            name = result["name"]

            if name:  # comp_name == name:  # We have an exact match
                if database_identifier:
                    maf_df.iloc[row_idx, int(standard_maf_columns['database_identifier'])] = database_identifier
                if chemical_formula:
                    maf_df.iloc[row_idx, int(standard_maf_columns['chemical_formula'])] = chemical_formula
                if smiles:
                    maf_df.iloc[row_idx, int(standard_maf_columns['smiles'])] = smiles
                if inchi:
                    maf_df.iloc[row_idx, int(standard_maf_columns['inchi'])] = inchi
            row_idx += 1

    write_tsv(maf_df, annotation_file_name + ".annotated")

    return maf_df, maf_len, new_maf_df, new_maf_len


class SplitMaf(Resource):
    @swagger.operation(
        summary="MAF pipeline splitter (curator only)",
        nickname="Add rows based on pipeline splitting",
        notes="Split a given Metabolite Annotation File based on pipelines in cells. "
              "A new MAF will be created with extension '.split'. "
              "If no annotation_file_name is given, all MAF in the study is processed",
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
                "required": False,
                "allowEmptyValue": True,
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
    def post(self, study_id):

        # param validation
        if study_id is None:
            abort(404, 'Please provide valid parameter for study identifier')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not is_curator:
            abort(403)

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('annotation_file_name', help="Metabolite Annotation File", location="args")
        args = parser.parse_args()
        annotation_file_name = args['annotation_file_name']

        if annotation_file_name is None:
            # Loop through all m_*_v2_maf.tsv files
            study_files, upload_files, upload_diff, upload_location = \
                get_all_files_from_filesystem(
                    study_id, obfuscation_code, study_location, directory=None, include_raw_data=False)
            maf_count = 0
            maf_changed = 0
            for file in study_files:
                file_name = file['file']
                if file_name.startswith('m_') and file_name.endswith('_v2_maf.tsv'):
                    maf_count += 1
                    maf_df, maf_len, new_maf_df, new_maf_len = check_maf_for_pipes(study_location, file_name)
                    if maf_len != new_maf_len:
                        maf_changed += 1
        else:
            maf_df, maf_len, new_maf_df, new_maf_len = check_maf_for_pipes(study_location, annotation_file_name)
            # Dict for the data (rows)
            df_data_dict = totuples(new_maf_df.reset_index(), 'rows')
            # Get an indexed header row
            df_header = get_table_header(new_maf_df)

            return {"maf_rows": maf_len, "new_maf_rows": new_maf_len, "header": df_header, "data": df_data_dict}

        return {"success": str(maf_count) + " MAF files checked for pipelines, " +
                           str(maf_changed) + " files needed updating."}


class SearchNamesMaf(Resource):
    @swagger.operation(
        summary="Search using compound names in MAF (curator only)",
        nickname="Search compound names",
        notes="Search and populate a given Metabolite Annotation File based on the 'metabolite_identification' column. "
              "A new MAF will be created with extension '.annotated'. "
              "If no annotation_file_name is given, all MAF in the study is processed",
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
                "required": False,
                "allowEmptyValue": True,
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
    def post(self, study_id):

        # param validation
        if study_id is None:
            abort(404, 'Please provide valid parameter for study identifier')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not is_curator:
            abort(403)

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('annotation_file_name', help="Metabolite Annotation File", location="args")
        args = parser.parse_args()
        annotation_file_name = args['annotation_file_name']

        if annotation_file_name is None:
            # Loop through all m_*_v2_maf.tsv files
            study_files, upload_files, upload_diff, upload_location = \
                get_all_files_from_filesystem(
                    study_id, obfuscation_code, study_location, directory=None, include_raw_data=False)
            maf_count = 0
            maf_changed = 0
            for file in study_files:
                file_name = file['file']
                if file_name.startswith('m_') and file_name.endswith('_v2_maf.tsv'):
                    maf_count += 1
                    maf_df, maf_len, new_maf_df, new_maf_len = search_and_update_maf(study_location, file_name)
                    if maf_len != new_maf_len:
                        maf_changed += 1
        else:
            maf_df, maf_len, new_maf_df, new_maf_len = search_and_update_maf(study_location, annotation_file_name)
            # Dict for the data (rows)
            df_data_dict = totuples(new_maf_df.reset_index(), 'rows')
            # Get an indexed header row
            df_header = get_table_header(new_maf_df)

            return {"in_maf_rows": maf_len, "out_maf_rows": new_maf_len, "header": df_header, "data": df_data_dict}

        return {"success": str(maf_count) + " MAF files checked for pipelines, " +
                           str(maf_changed) + " files needed updating."}

