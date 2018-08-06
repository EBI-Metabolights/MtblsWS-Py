import logging, json, pandas as pd, numpy as np
from flask import request, abort, jsonify
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient

"""
MTBLS MAF

Manage the metabolite annotation files (MAF) from a MTBLS studies.  MAF JSON obtained from the Java-based MTBLS WS
"""

logger = logging.getLogger('wslog')
# MetaboLights (Java-Based) WebService client
wsc = WsClient()


# Convert panda DataFrame to json tuples object
def totuples(df, text):
    d = [
        dict([
            (colname, row[i])
            for i, colname in enumerate(df.columns)
        ])
        for row in df.values
    ]
    return {text: d}


def get_maf_header(maf_df):
    # Get an indexed header row
    df_header = pd.DataFrame(list(maf_df))  # Get the header row only
    df_header = df_header.reset_index().to_dict(orient='list')
    mapping = {}
    print(df_header)
    for i in range(0, len(df_header['index'])):
        mapping[df_header[0][i]] = df_header['index'][i]
    return mapping


class MtblsMAFSearch(Resource):
    """Get MAF from studies (assays)"""
    @swagger.operation(
        summary="Search for metabolite information to use in the Metabolite Annotation file",
        nickname="MAF search",
        notes="Get a given MAF associated with assay {assay_id} for a MTBLS Study with {study_id} in JSON format",
        parameters=[
            {
                "name": "search_type",
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
    def get(self, search_type):
        # param validation
        if search_type is None:
            abort(404)

        parser = reqparse.RequestParser()
        parser.add_argument('search_value', help="The search string")
        search_value = None
        if request.args:
            args = parser.parse_args(req=request)
            search_value = args['search_value']

        if search_value is None:
            abort(404)

        search_res = wsc.get_maf_search(search_type, search_value)
        return search_res


class MetaboliteAnnotationFile(Resource):
    """Get MAF from filesystem"""
    @swagger.operation(
        summary="Read, and add missing samples for a MAF for a MTBLS study",
        nickname="Get MAF for a given MTBLS Assay",
        notes="Get a given Metabolite Annotation File for a MTBLS Study with in JSON format.",
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
                "name": "assay_file_name",
                "description": "Assay File name",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "technology",
                "description": "Assay technology type, MS or NMR",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["MS", "NMR"]
            },
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": False,
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
    def get(self, study_id, annotation_file_name):
        parser = reqparse.RequestParser()
        parser.add_argument('technology', help="Assay technology type, MS or NMR")
        parser.add_argument('assay_file_name', help="Assay File name")
        technology = None
        assay_file_name = None
        if request.args:
            args = parser.parse_args(req=request)
            technology = args['technology']
            assay_file_name = args['assay_file_name']

        resource_folder = "./resources/"
        # param validation
        if study_id is None or assay_file_name is None or technology is None or annotation_file_name is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info('MAF: Getting ISA-JSON Study %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)

        study_path = wsc.get_study_location(study_id, user_token)

        # Fixed colunm headers to look for in the MAF, defaults to MS
        sample_name = 'Sample Name'
        assay_name = 'MS Assay Name'
        annotation_file_template = resource_folder + 'm_metabolite_profiling_mass_spectrometry_v2_maf.tsv'

        # NMR MAF and assay name
        if technology == "NMR":
            annotation_file_template = resource_folder + 'm_metabolite_profiling_NMR_spectroscopy_v2_maf.tsv'
            assay_name = 'NMR Assay Name'

        col_names = [sample_name, assay_name]

        annotation_file_name = study_path + "/" + annotation_file_name
        assay_file_name = study_path + "/" + assay_file_name

        # Get the MAF table or create a new one if it does not already exist
        try:
            maf_df = pd.read_csv(annotation_file_name, sep="\t", header=0, encoding='utf-8')
        except FileNotFoundError:
            maf_df = pd.read_csv(annotation_file_template, sep="\t", header=0, encoding='utf-8')
        # Get rid of empty numerical values
        maf_df = maf_df.replace(np.nan, '', regex=True)

        # Read NMR or MS Assay Name first, if that is empty, use Sample Name
        assay_df = pd.read_csv(assay_file_name, sep="\t", usecols=col_names, header=0, encoding='utf-8')
        assay_df = assay_df.replace(np.nan, '', regex=True)

        # Get the MS/NMR Assay Name or Sample names from the assay
        assay_sample_names = assay_df[assay_name]
        if assay_sample_names is None:
            assay_sample_names = assay_df[sample_name]

        # Does the column already exist?
        for row in assay_sample_names.iteritems():
            s_name = row[1]  # "database_identifier"
            try:
                in_maf = maf_df.columns.get_loc(s_name)
            except KeyError:
                in_maf = 0

            if in_maf == 0:
                # Add the new columns to the MAF
                maf_df[s_name] = ""

        # Write the new empty columns back in the file
        maf_df.to_csv(annotation_file_name, sep="\t", encoding='utf-8', index=False)

        # Get an indexed header row
        df_header = get_maf_header(maf_df)

        # Get the rows from the maf
        df_data_dict = totuples(maf_df.reset_index(), 'rows')

        return {'mafHeader': df_header, 'mafData': df_data_dict}

    """Create MAF for a given study"""
    @swagger.operation(
        summary="Update MAF for an assay for a given study",
        nickname="Update MAF",
        notes="Update a Metabolite Annotation File (MAF) for a given Study.",
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
                "name": "row_num",
                "description": "The row number of the cell to update (exclude header)",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "integer"
            },
            {
                "name": "column_name",
                "description": "The column name of the cell to update",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "cell_value",
                "description": "The value of the cell to update",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The MAF has been updated."
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
    def put(self, study_id, annotation_file_name):
        parser = reqparse.RequestParser()
        parser.add_argument('row_num', help="The row number of the cell to update (exclude header)")
        parser.add_argument('column_name', help="The column name of the cell to update")
        parser.add_argument('cell_value', help="The column name of the cell to update")
        row_num = None
        column_name = None
        cell_value = None
        if request.args:
            args = parser.parse_args(req=request)
            row_num = args['row_num']
            column_name = args['column_name']
            cell_value = args['cell_value']

        # param validation
        if study_id is None or annotation_file_name is None or row_num is None or column_name is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)

        study_path = wsc.get_study_location(study_id, user_token)
        annotation_file_name = study_path + "/" + annotation_file_name

        maf_df = pd.read_csv(annotation_file_name, sep="\t", header=0, encoding='utf-8')
        maf_df = maf_df.replace(np.nan, '', regex=True)

        try:
            row = int(row_num)
            column = maf_df.columns.get_loc(column_name)
            maf_df.iloc[row, column] = cell_value
        except Exception:
            logger.warning('Could not find row (' + row_num + '( and/or column (' + column_name + ') in the table')

        # Write the new empty columns back in the file
        maf_df.to_csv(annotation_file_name, sep="\t", encoding='utf-8', index=False)

        # Convert panda DataFrame (single row) to json tuples object
        def totuples(df, rown):
            d = [
                dict([
                    (colname, row[rown])
                    for rown, colname in enumerate(df.columns)
                ])
                for row in df.values
            ]
            return {'mafdata': d}

        df_dict = totuples(maf_df.reset_index(), row)

        return df_dict


class ReadMetaboliteAnnotationFile(Resource):
    """Get MAF from filesystem"""
    @swagger.operation(
        summary="Get MAF for a study using annotation filename",
        nickname="Get MAF for a given MTBLS Assay",
        notes="Get a given Metabolite Annotation File for a MTBLS Study with in JSON format.",
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
                "required": False,
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
    def get(self, study_id, annotation_file_name):
        # param validation
        if study_id is None or annotation_file_name is None:
            logger.info('No study_id and/or annotation file name given')
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info('MAF: Getting ISA-JSON Study %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)

        study_path = wsc.get_study_location(study_id, user_token)
        annotation_file_name = study_path + "/" + annotation_file_name
        logger.info('Trying to load MAF (%s) for Study %s', annotation_file_name, study_id)
        # Get the MAF table or create a new one if it does not already exist
        maf_df = pd.read_csv(annotation_file_name, sep="\t", header=0, encoding='utf-8')
        # Get rid of empty numerical values
        maf_df = maf_df.replace(np.nan, '', regex=True)

        df_data_dict = totuples(maf_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_maf_header(maf_df)

        return {'mafHeader': df_header, 'mafData': df_data_dict}

    @swagger.operation(
        summary="Add a new row to the given annotation file",
        nickname="Add MAF row",
        notes="Update a Metabolite Annotation File (MAF) for a given Study.",
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
                "name": "new_row",
                "description": "The row to add to the annotation file",
                "required": True,
                "allowMultiple": False,
                "paramType": "body",
                "dataType": "string"
            },
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The MAF has been updated."
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

        data_dict = json.loads(request.data.decode('utf-8'))
        new_row = data_dict['mafdata']
        for element in new_row:
            element.pop('index', None)  #Remove "index:n" element from the (JSON) row, this is the original row number

        # param validation
        if study_id is None or annotation_file_name is None or new_row is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)

        study_path = wsc.get_study_location(study_id, user_token)
        annotation_file_name = study_path + "/" + annotation_file_name

        maf_df = pd.read_csv(annotation_file_name, sep="\t", header=0, encoding='utf-8')
        maf_df = maf_df.replace(np.nan, '', regex=True)  # Remove NaN
        maf_df = maf_df.append(new_row, ignore_index=True)  # Add new row to the spreadsheet

        # Write the new row back in the file
        maf_df.to_csv(annotation_file_name, sep="\t", encoding='utf-8', index=False)

        df_data_dict = totuples(maf_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_maf_header(maf_df)

        return {'mafHeader': df_header, 'mafData': df_data_dict}


    @swagger.operation(
        summary="Delete a row of the given annotation file",
        nickname="Delete MAF row",
        notes="Update a Metabolite Annotation File (MAF) for a given Study.",
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
                "name": "row_num",
                "description": "The row number to remove",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "integer"
            },
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The MAF has been updated."
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
    def put(self, study_id, annotation_file_name):

        parser = reqparse.RequestParser()
        parser.add_argument('row_num', help="The row number of the cell to remove (exclude header)")
        row_num = None
        if request.args:
            args = parser.parse_args(req=request)
            row_num = args['row_num']

        # param validation
        if study_id is None or annotation_file_name is None or row_num is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)

        study_path = wsc.get_study_location(study_id, user_token)
        annotation_file_name = study_path + "/" + annotation_file_name

        maf_df = pd.read_csv(annotation_file_name, sep="\t", header=0, encoding='utf-8')
        maf_df = maf_df.replace(np.nan, '', regex=True)  # Remove NaN
        maf_df = maf_df.drop(maf_df.index[int(row_num)])  # Drop a row in the spreadsheet

        # Write the updated file
        maf_df.to_csv(annotation_file_name, sep="\t", encoding='utf-8', index=False)

        df_data_dict = totuples(maf_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_maf_header(maf_df)

        return {'mafHeader': df_header, 'mafData': df_data_dict}


