import logging, json, pandas as pd, numpy as np, os
from flask import request, abort
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
    return df.iloc[:idx, ].append(df_insert, ignore_index=True).append(df.iloc[idx:, ]).reset_index(drop=True)


class MtblsMAFSearch(Resource):
    """Get MAF from studies (assays)"""
    @swagger.operation(
        summary="Search for metabolite onto_information to use in the Metabolite Annotation file",
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
        summary="Read, and add missing samples for a MAF",
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

        logger.info('MAF: Getting ISA-JSON Study %s', study_id)
        # check for access rights
        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        # Fixed colunm headers to look for in the MAF, defaults to MS
        sample_name = 'Sample Name'
        assay_name = 'MS Assay Name'
        annotation_file_template = resource_folder + 'm_metabolite_profiling_mass_spectrometry_v2_maf.tsv'

        # NMR MAF and assay name
        if technology == "NMR":
            annotation_file_template = resource_folder + 'm_metabolite_profiling_NMR_spectroscopy_v2_maf.tsv'
            assay_name = 'NMR Assay Name'

        col_names = [sample_name, assay_name]

        annotation_file_name = os.path.join(study_location, annotation_file_name)
        assay_file_name = os.path.join(study_location, assay_file_name)

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
        df_header = get_table_header(maf_df)

        # Get the rows from the maf
        df_data_dict = totuples(maf_df.reset_index(), 'rows')

        return {'header': df_header, 'data': df_data_dict}

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
                "required": True,
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
        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        annotation_file_name = os.path.join(study_location, annotation_file_name)

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
            return {'data': d}

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
    def get(self, study_id, annotation_file_name):
        # param validation
        if study_id is None or annotation_file_name is None:
            logger.info('No study_id and/or annotation file name given')
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info('MAF: Getting ISA-JSON Study %s', study_id)

        # check for access rights
        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        annotation_file_name = os.path.join(study_location, annotation_file_name)
        logger.info('Trying to load MAF (%s) for Study %s', annotation_file_name, study_id)
        # Get the MAF table or create a new one if it does not already exist
        maf_df = pd.read_csv(annotation_file_name, sep="\t", header=0, encoding='utf-8')
        # Get rid of empty numerical values
        maf_df = maf_df.replace(np.nan, '', regex=True)

        df_data_dict = totuples(maf_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_table_header(maf_df)

        return {'header': df_header, 'data': df_data_dict}

    @swagger.operation(
        summary="Add a new row to the given annotation file <b>(Deprecated)</b>",
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
                "required": True,
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

        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            new_row = data_dict['data']
        except KeyError:
            new_row = None

        if new_row is None:
            abort(404, "Please provide valid data for updated new row(s). "
                       "The JSON string has to have a 'data' element")

        try:
            for element in new_row:
                element.pop('index', None)  # Remove "index:n" element, this is the original row number
        except:
            logger.info('No index (row num) supplied, ignoring')

        # param validation
        if study_id is None or annotation_file_name is None:
            abort(404, 'Please provide valid parameters for study identifier and annotation file name')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        annotation_file_name = os.path.join(study_location, annotation_file_name)

        maf_df = pd.read_csv(annotation_file_name, sep="\t", header=0, encoding='utf-8')
        maf_df = maf_df.replace(np.nan, '', regex=True)  # Remove NaN
        maf_df = maf_df.append(new_row, ignore_index=True)  # Add new row to the spreadsheet

        # Write the new row back in the file
        maf_df.to_csv(annotation_file_name, sep="\t", encoding='utf-8', index=False)

        df_data_dict = totuples(maf_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_table_header(maf_df)

        return {'header': df_header, 'data': df_data_dict}

    @swagger.operation(
        summary="Update existing rows in the given annotation file <b>(Deprecated)</b>",
        nickname="Update MAF rows",
        notes="Update rows in the Metabolite Annotation File (MAF) for a given Study.",
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
                "description": "The row(s) to update in the annotation file",
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
                "required": True,
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
                "message": "Not found or missing parameters. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def put(self, study_id, annotation_file_name):

        # param validation
        if study_id is None or annotation_file_name is None:
            abort(404, 'Please provide valid parameters for study identifier and annotation file name')

        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            new_rows = data_dict['data']  # Use "index:n" element from the (JSON) row, this is the original row number
        except KeyError:
            new_rows = None

        if new_rows is None:
            abort(404, "Please provide valid data for updated new row(s). "
                       "The JSON string has to have a 'data' element")

        for row in new_rows:
            try:
                row_index = row['index']  # Check if we have a value in the row number(s)
            except KeyError:
                row_index = None

            if new_rows is None or row_index is None:
                abort(404, "Please provide valid data for the updated row(s). "
                           "The JSON string has to have an 'index:n' element in each (JSON) row, "
                           "this is the original row number. The header row can not be updated")

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        annotation_file_name = os.path.join(study_location, annotation_file_name)

        maf_df = pd.read_csv(annotation_file_name, sep="\t", header=0, encoding='utf-8')
        maf_df = maf_df.replace(np.nan, '', regex=True)  # Remove NaN

        for row in new_rows:
            try:
                row_index_int = int(row['index'])
            except:
                row_index_int is None

            if row_index_int is not None:
                maf_df = maf_df.drop(maf_df.index[row_index_int])  # Remove the old row from the spreadsheet
                # pop the "index:n" from the new_row before updating
                row.pop('index', None)  # Remove "index:n" element, this is the original row number
                maf_df = insert_row(row_index_int, maf_df, row)  # Update the row in the spreadsheet

        # Write the new row back in the file
        maf_df.to_csv(annotation_file_name, sep="\t", encoding='utf-8', index=False)

        df_data_dict = totuples(maf_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_table_header(maf_df)

        return {'header': df_header, 'data': df_data_dict}

    @swagger.operation(
        summary="Delete a row of the given annotation file <b>(Deprecated)</b>",
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
                "description": "The row number(s) to remove, comma separated if more than one",
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
                "required": True,
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
    def delete(self, study_id, annotation_file_name):

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('row_num', help="The row number of the cell(s) to remove (exclude header)", location="args")
        args = parser.parse_args()
        row_num = args['row_num']

        # param validation
        if study_id is None or annotation_file_name is None or row_num is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        annotation_file_name = os.path.join(study_location, annotation_file_name)

        maf_df = pd.read_csv(annotation_file_name, sep="\t", header=0, encoding='utf-8')
        maf_df = maf_df.replace(np.nan, '', regex=True)  # Remove NaN
        row_nums = row_num.split(",")

        # Need to remove the highest row number first as the DataFrame dynamically re-orders when one row is removed
        sorted_num_rows = [int(x) for x in row_nums]
        sorted_num_rows.sort(reverse=True)
        for num in sorted_num_rows:
            maf_df = maf_df.drop(maf_df.index[num])  # Drop row(s) in the spreadsheet

        # Write the updated file
        maf_df.to_csv(annotation_file_name, sep="\t", encoding='utf-8', index=False)

        # To be sure we read the file again
        maf_df = pd.read_csv(annotation_file_name, sep="\t", header=0, encoding='utf-8')
        maf_df = maf_df.replace(np.nan, '', regex=True)  # Remove NaN

        df_data_dict = totuples(maf_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_table_header(maf_df)

        return {'header': df_header, 'data': df_data_dict}
