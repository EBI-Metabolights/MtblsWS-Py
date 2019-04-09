import logging, pandas as pd, os
import numpy as np
import requests
import cirpy
import string
from flask import request, abort
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from pubchempy import get_compounds
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
    try:
        maf_df = read_tsv(annotation_file_name)
    except FileNotFoundError:
        abort(400, "The file " + annotation_file_name + " was not found")
    maf_len = len(maf_df.index)

    # Any rows to split?
    new_maf_df = split_rows(maf_df)
    new_maf_len = len(new_maf_df.index)

    if maf_len != new_maf_len:  # We did find |, so we create a new MAF
        write_tsv(new_maf_df, annotation_file_name + ".split")

    return maf_df, maf_len, new_maf_df, new_maf_len


def search_and_update_maf(study_location, annotation_file_name):
    short_file_name = os.path.join(study_location, annotation_file_name.replace('.tsv', ''))
    annotation_file_name = os.path.join(study_location, annotation_file_name)
    pd.options.mode.chained_assignment = None  # default='warn'
    try:
        maf_df = read_tsv(annotation_file_name)
    except FileNotFoundError:
        abort(400, "The file " + annotation_file_name + " was not found")
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

    pubchem_df = create_pubchem_df(maf_df)

    row_idx = 0
    # Search using the compound name
    for idx, comp_name in enumerate(maf_df[maf_compound_name_column]):
        search_res = wsc.get_maf_search("name", comp_name)
        pc_name, pc_inchi, pc_inchi_key, pc_smiles, pc_cid, pc_formula = pubchem_search(comp_name)

        if search_res['content']:
            name = None
            result = search_res['content'][0]
            database_identifier = result["databaseId"]
            chemical_formula = result["formula"]
            smiles = result["smiles"]
            inchi = result["inchi"]
            name = result["name"]

            pubchem_df.iloc[row_idx, 0] = database_identifier
            pubchem_df.iloc[row_idx, 1] = chemical_formula
            pubchem_df.iloc[row_idx, 2] = smiles
            pubchem_df.iloc[row_idx, 3] = inchi
            # 4 is name / metabolite_identification from MAF
            pubchem_df.iloc[row_idx, 5] = pc_name
            pubchem_df.iloc[row_idx, 6] = pc_cid
            # 7 PubChem CID from InChIKey search (Cactus, OBSIN)
            # 8 ChemSpider ID (CSID) from INCHIKEY
            # 9 final smiles
            # 10 final inchi
            # 11 final inchikey
            pubchem_df.iloc[row_idx, 12] = pc_smiles
            pubchem_df.iloc[row_idx, 13] = catus_search(comp_name, 'smiles')
            pubchem_df.iloc[row_idx, 14] = opsin_search(comp_name, 'smiles')

            pubchem_df.iloc[row_idx, 15] = pc_inchi
            pubchem_df.iloc[row_idx, 16] = catus_search(comp_name, 'stdinchi')
            pubchem_df.iloc[row_idx, 17] = opsin_search(comp_name, 'stdinchi')

            pubchem_df.iloc[row_idx, 18] = pc_inchi_key
            pubchem_df.iloc[row_idx, 19] = catus_search(comp_name, 'stdinchikey')
            pubchem_df.iloc[row_idx, 20] = opsin_search(comp_name, 'stdinchikey')
            pubchem_df.iloc[row_idx, 21] = pc_formula

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

    write_tsv(maf_df, short_file_name + "_annotated.tsv")
    write_tsv(pubchem_df, short_file_name + "_pubchem.tsv")

    return maf_df, maf_len, new_maf_df, new_maf_len


def create_pubchem_df(maf_df):
    # These are simply the fixed spreadsheet column headers
    pubchem_df = maf_df[['database_identifier', 'chemical_formula', 'smiles', 'inchi', 'metabolite_identification']]
    pubchem_df['iupac_name'] = ''       # 5
    pubchem_df['pubchem_cid'] = ''      # 6
    pubchem_df['pubchem_cid_ik'] = ''   # 7  PubChem CID from InChIKey search (Cactus, OBSIN)
    pubchem_df['csid_ik'] = ''          # 8  ChemSpider ID (CSID) from INCHIKEY

    pubchem_df['final_smiles'] = ''     # 9
    pubchem_df['final_inchi'] = ''      # 10
    pubchem_df['final_inchi_key'] = ''  # 11

    pubchem_df['pubchem_smiles'] = ''   # 12
    pubchem_df['cactus_smiles'] = ''    # 13
    pubchem_df['opsin_smiles'] = ''     # 14

    pubchem_df['pubchem_inchi'] = ''    # 15
    pubchem_df['cactus_inchi'] = ''     # 16
    pubchem_df['opsin_inchi'] = ''      # 17

    pubchem_df['pubchem_inchi_key'] = ''    # 18
    pubchem_df['cactus_inchi_key'] = ''     # 19
    pubchem_df['opsin_inchi_key'] = ''      # 20

    pubchem_df['pubchem_formula'] = ''      # 21

    return pubchem_df


def opsin_search(comp_name, req_type):
    result = ""
    opsing_url = 'https://opsin.ch.cam.ac.uk/opsin/'
    url = opsing_url + comp_name + '.json'
    resp = requests.get(url)
    if resp.status_code == 200:
        json_resp = resp.json()
        result = json_resp[req_type]
    return result


def catus_search(comp_name, type):
    result = cirpy.resolve(comp_name, type)

    if result:
        if type == 'stdinchikey':
            return result.replace('InChIKey=', '')
    return result


def pubchem_search(comp_name):
    iupac = ''
    inchi = ''
    inchi_key = ''
    smiles = ''
    cid = ''
    formula = ''

    # For this to work on Mac, run: cd "/Applications/Python 3.6/"; sudo "./Install Certificates.command
    try:
        pubchem_compound = get_compounds(comp_name, namespace='name')
        compound = pubchem_compound[0]  # Only read the first record from PubChem = preferred entry
        inchi = compound.inchi
        inchi_key = compound.inchikey
        smiles = compound.canonical_smiles
        iupac = compound.iupac_name
        cid = compound.cid
        formula = compound.molecular_formula
        logger.debug('Searching PubChem for "' + comp_name + '", got cid "' + cid + '" and iupac name "' + iupac + '"')
    except:
        logger.error("Unable to search PubChem for compound " + comp_name)

    return iupac, inchi, inchi_key, smiles, cid, formula


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
              "New MAF files will be created with extensions '_annotated.tsv' and '_pubchem.tsv'. "
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
