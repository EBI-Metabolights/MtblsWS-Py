import logging
import pandas as pd
import numpy as np
import json
import re
from flask import request, abort
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import get_table_header, totuples, copy_files_and_folders, validate_rows

"""
MTBLS Table Columns manipulator

Manage the CSV/TSV tables in MTBLS studies.
"""

logger = logging.getLogger('wslog')

# MetaboLights (Java-Based) WebService client
wsc = WsClient()


def insert_row(idx, df, df_insert):
    return df.iloc[:idx, ].append(df_insert, ignore_index=True).append(df.iloc[idx:, ]).reset_index(drop=True)


class SimpleColumns(Resource):
    @swagger.operation(
        summary="Add a new column to the given TSV file",
        nickname="Add table column",
        notes="Update an csv/tsv table for a given Study",
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
                "name": "file_name",
                "description": "the CSV or TSV file name",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "new_column_name",
                "description": "The column to add to the file",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "new_column_position",
                "description": "The position of the new column (column #)",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "integer"
            },
            {
                "name": "new_column_default_value",
                "description": "The default value to add to all rows in the column",
                "required": False,
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
                "message": "OK. The table has been updated."
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
    def post(self, study_id, file_name):

        parser = reqparse.RequestParser()
        parser.add_argument('new_column_name', help="Name of new column")
        new_column_name = None
        parser.add_argument('new_column_position', help="The position (column #) of new column")
        new_column_position = None
        parser.add_argument('new_column_default_value', help="The (optional) default value of new column")
        new_column_default_value = None

        if request.args:
            args = parser.parse_args(req=request)
            new_column_name = args['new_column_name']
            new_column_position = args['new_column_position']
            new_column_default_value = args['new_column_default_value']

        if new_column_name is None:
            abort(404, "Please provide valid name for the new column")

        # param validation
        if study_id is None or file_name is None:
            abort(404, 'Please provide valid parameters for study identifier and file name')
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permisions(study_id, user_token)
        if not write_access:
            abort(403)

        file_name = study_location + "/" + file_name

        table_df = pd.read_csv(file_name, sep="\t", header=0, encoding='utf-8')
        table_df = table_df.replace(np.nan, '', regex=True)  # Remove NaN

        #  Need to add values for each existing row (not header)
        new_col = []
        for row_val in range(table_df.shape[0]):
            new_col.append(new_column_default_value)

        table_df.insert(loc=int(new_column_position), column=new_column_name, value=new_col, allow_duplicates=True)  # Add new column to the spreadsheet

        df_data_dict = totuples(table_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_table_header(table_df)

        # Remove all ".n" numbers at the end of duplicated column names
        table_df.rename(columns=lambda x: re.sub(r'\.[0-9]+$', '', x), inplace=True)

        # Write the new row back in the file
        table_df.to_csv(file_name, sep="\t", encoding='utf-8', index=False)

        return {'header': df_header, 'data': df_data_dict}


class ComplexColumns(Resource):
    @swagger.operation(
        summary="Add new columns to the given TSV file",
        nickname="Add table columns",
        notes="Update an csv/tsv table for a given Study",
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
                "name": "file_name",
                "description": "the CSV or TSV file name",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "new_columns",
                "description": "The columns to add to the file",
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
                "message": "OK. The table has been updated."
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
    def post(self, study_id, file_name):

        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            new_columns = data_dict['data']
        except KeyError:
            new_columns = None

        if new_columns is None:
            abort(404, "Please provide valid key-value pairs for the new columns."
                       "The JSON string has to have a 'data' element")

        # param validation
        if study_id is None or file_name is None:
            abort(404, 'Please provide valid parameters for study identifier and/or file name')
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permisions(study_id, user_token)
        if not write_access:
            abort(403)

        file_name = study_location + "/" + file_name

        table_df = pd.read_csv(file_name, sep="\t", header=0, encoding='utf-8')
        table_df = table_df.replace(np.nan, '', regex=True)  # Remove NaN

        for column in new_columns:
            new_column_default_value = column['value']
            new_column_name = column['name']
            new_column_position = column['index']

            #  Need to add values for each existing row (not header)
            new_col = []
            for row_val in range(table_df.shape[0]):
                new_col.append(new_column_default_value)

            # Add new column to the spreadsheet
            table_df.insert(loc=int(new_column_position), column=new_column_name, value=new_col, allow_duplicates=True)

        # Get an indexed header row
        df_header = get_table_header(table_df)

        # Get all indexed rows
        df_data_dict = totuples(table_df.reset_index(), 'rows')

        # Remove all ".n" numbers at the end of duplicated column names
        table_df.rename(columns=lambda x: re.sub(r'\.[0-9]+$', '', x), inplace=True)
        table_df.to_csv(file_name, sep="\t", encoding='utf-8', index=False)
        return {'header': df_header, 'rows': df_data_dict}


class ColumnsRows(Resource):
    @swagger.operation(
        summary="Update a given cell, based on row and column index",
        nickname="Add table columns",
        notes="Update an TSV table for a given Study",
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
                "name": "file_name",
                "description": "the TSV file name",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "column_row_index",
                "description": "The number of the column and row = cell to update",
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
                "message": "OK. The table has been updated."
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
                "message": "The row or column does not exist. (Note: Both indexes start at 0)"
            }
        ]
    )
    def post(self, study_id, file_name):

        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            columns_rows = data_dict['data']
        except KeyError:
            columns_rows = None

        if columns_rows is None:
            abort(404, "Please provide valid key-value pairs for the cell value."
                       "The JSON string has to have a 'data' element")

        # param validation
        if study_id is None or file_name is None:
            abort(404, 'Please provide valid parameters for study identifier and/or file name')
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permisions(study_id, user_token)
        if not write_access:
            abort(403)

        file_name = study_location + "/" + file_name

        table_df = pd.read_csv(file_name, sep="\t", header=0, encoding='utf-8')
        table_df = table_df.replace(np.nan, '', regex=True)  # Remove NaN

        for column in columns_rows:
            cell_value = column['value']
            row_index = column['row']
            column_index = column['column']
            #  Need to add values for column and row (not header)
            try:
                for row_val in range(table_df.shape[0]):
                    table_df.iloc[int(row_index), int(column_index)] = cell_value
            except ValueError:
                abort(417)

        # Write the new row back in the file
        table_df.to_csv(file_name, sep="\t", encoding='utf-8', index=False)

        df_data_dict = totuples(table_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_table_header(table_df)

        return {'header': df_header, 'rows': df_data_dict}


class AddRows(Resource):
    @swagger.operation(
        summary="Add a new row to the given TSV file",
        nickname="Add TSV table row",
        notes="Update an TSV table for a given Study.",
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
                "name": "file_name",
                "description": "TSV File name",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "new_row",
                "description": "The row to add to the TSV file",
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
                "message": "OK. The TSV table has been updated."
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
                "message": "The column name given does not exist in the TSV file"
            }
        ]
    )
    def post(self, study_id, file_name):

        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            new_row = data_dict['data']
        except (KeyError):
            new_row = None

        if new_row is None:
            abort(404, "Please provide valid data for updated new row(s). "
                       "The JSON string has to have a 'data' element")

        try:
            for element in new_row:
                element.pop('index', None)  # Remove "index:n" element from the (JSON) row, this is the original row number
        except:
            logger.info('No index (row num) supplied, ignoring')

        # param validation
        if study_id is None or file_name is None:
            abort(404, 'Please provide valid parameters for study identifier and TSV file name')
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permisions(study_id, user_token)
        if not write_access:
            abort(403)

        file_name = study_location + "/" + file_name

        file_df = pd.read_csv(file_name, sep="\t", header=0, encoding='utf-8')
        file_df = file_df.replace(np.nan, '', regex=True)  # Remove NaN values

        # Validate column names in new rows
        valid_column_name, message = validate_rows(file_df, new_row)
        if not valid_column_name:
            abort(417, message)

        file_df = file_df.append(new_row, ignore_index=True)  # Add new row to the spreadsheet (TSV file)

        # Remove all ".n" numbers at the end of duplicated column names
        file_df.rename(columns=lambda x: re.sub(r'\.[0-9]+$', '', x), inplace=True)

        # Write the new row back in the file
        file_df.to_csv(file_name, sep="\t", encoding='utf-8', index=False)

        # Get an indexed header row
        df_header = get_table_header(file_df)

        # Get the updated data table
        df_data_dict = totuples(file_df.reset_index(), 'rows')

        return {'header': df_header, 'data': df_data_dict}


class UpdateRows(Resource):
    @swagger.operation(
        summary="Update existing rows in the given TSV file",
        nickname="Update TSV rows",
        notes="Update rows in the TSV file for a given Study.",
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
                "name": "file_name",
                "description": "TSV File name",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "new_row",
                "description": "The row(s) to update in the TSV file",
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
                "message": "OK. The TSV file has been updated."
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
            },
            {
                "code": 417,
                "message": "The column name given does not exist in the TSV file"
            }
        ]
    )
    def post(self, study_id, file_name):

        # param validation
        if study_id is None or file_name is None:
            abort(404, 'Please provide valid parameters for study identifier and TSV file name')
        study_id = study_id.upper()

        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            new_rows = data_dict[
                'data']  # Use "index:n" element from the (JSON) row, this is the original row number
        except (KeyError):
            new_rows = None

        if new_rows is None:
            abort(404, "Please provide valid data for updated new row(s). "
                       "The JSON string has to have a 'data' element")

        for row in new_rows:
            try:
                row_index = row['index']  # Check if we have a value in the row number(s)
            except (KeyError):
                row_index = None

            if new_rows is None or row_index is None:
                abort(404, "Please provide valid data for the updated row(s). "
                           "The JSON string has to have an 'index:n' element in each (JSON) row. "
                           "The header row can not be updated")

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permisions(study_id, user_token)
        if not write_access:
            abort(403)

        # TODO, don't use xNIX notation for file separator
        file_name = study_location + "/" + file_name

        file_df = pd.read_csv(file_name, sep="\t", header=0, encoding='utf-8')
        file_df = file_df.replace(np.nan, '', regex=True)  # Remove NaN

        for row in new_rows:
            try:
                row_index_int = int(row['index'])
            except:
                row_index_int is None

            # Validate column names in new rows
            valid_column_name, message = validate_rows(file_df, row)
            if not valid_column_name:
                abort(417, message)

            if row_index_int is not None:
                file_df = file_df.drop(file_df.index[row_index_int])  # Remove the old row from the spreadsheet
                # pop the "index:n" from the new_row before updating
                row.pop('index', None)  # Remove "index:n" element from the (JSON) row, this is the original row number
                file_df = insert_row(row_index_int, file_df, row)  # Update the row in the spreadsheet

        # Remove all ".n" numbers at the end of duplicated column names
        file_df.rename(columns=lambda x: re.sub(r'\.[0-9]+$', '', x), inplace=True)

        # Write the new row back in the file
        file_df.to_csv(file_name, sep="\t", encoding='utf-8', index=False)

        df_data_dict = totuples(file_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_table_header(file_df)

        return {'header': df_header, 'data': df_data_dict}


class DeleteRows(Resource):
    @swagger.operation(
        summary="Delete a row of the given TSV file",
        nickname="Delete TSV row",
        notes="Update TSV file for a given Study.",
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
                "name": "file_name",
                "description": "TSV File name",
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
                "message": "OK. The TSV file has been updated."
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
    def delete(self, study_id, file_name):

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('row_num', help="The row number of the cell(s) to remove (exclude header)", location="args")
        args = parser.parse_args()
        row_num = args['row_num']

        # param validation
        if study_id is None or file_name is None or row_num is None:
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permisions(study_id, user_token)
        if not write_access:
            abort(403)

        file_name = study_location + "/" + file_name

        file_df = pd.read_csv(file_name, sep="\t", header=0, encoding='utf-8')
        file_df = file_df.replace(np.nan, '', regex=True)  # Remove NaN
        row_nums = row_num.split(",")

        # Need to remove the highest row number first as the DataFrame dynamically re-orders when one row is removed
        sorted_num_rows = [int(x) for x in row_nums]
        sorted_num_rows.sort(reverse=True)
        for num in sorted_num_rows:
            file_df = file_df.drop(file_df.index[num])  # Drop row(s) in the spreadsheet

        # Remove all ".n" numbers at the end of duplicated column names
        file_df.rename(columns=lambda x: re.sub(r'\.[0-9]+$', '', x), inplace=True)

        # Write the updated file
        file_df.to_csv(file_name, sep="\t", encoding='utf-8', index=False)

        # To be sure we read the file again
        file_df = pd.read_csv(file_name, sep="\t", header=0, encoding='utf-8')
        file_df = file_df.replace(np.nan, '', regex=True)  # Remove NaN

        df_data_dict = totuples(file_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_table_header(file_df)

        return {'header': df_header, 'data': df_data_dict}


class GetTsvFile(Resource):
    class EditAssayFile(Resource):
        @swagger.operation(
            summary="Get TSV table for a study using assay filename",
            nickname="Get TSV table for a given study",
            notes="Get a given TSV table for a MTBLS Study with in JSON format.",
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
                    "name": "file_name",
                    "description": "TSV file name",
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
                    "message": "OK. The TSV table is returned"
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
        def get(self, study_id, file_name):
            # param validation
            if study_id is None or file_name is None:
                logger.info('No study_id and/or TSV file name given')
                abort(404)
            study_id = study_id.upper()

            # User authentication
            user_token = None
            if "user_token" in request.headers:
                user_token = request.headers["user_token"]

            logger.info('Assay Table: Getting ISA-JSON Study %s', study_id)
            # check for access rights
            read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
                wsc.get_permisions(study_id, user_token)
            if not read_access:
                abort(403)

            file_name = study_location + "/" + file_name

            logger.info('Trying to load TSV file (%s) for Study %s', file_name, study_id)
            # Get the Assay table or create a new one if it does not already exist
            file_df = pd.read_csv(file_name, sep="\t", header=0, encoding='utf-8')

            # Get rid of empty numerical values
            file_df = file_df.replace(np.nan, '', regex=True)

            df_data_dict = totuples(file_df.reset_index(), 'rows')

            # Get an indexed header row
            df_header = get_table_header(file_df)

            return {'header': df_header, 'data': df_data_dict}


class CopyFilesFolders(Resource):
    @swagger.operation(
        summary="Copy files from upload folder to study folder",
        nickname="Copy from upload folder",
        notes="Copies files/folder from the upload directory to the study directory",
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
                "message": "OK. Files/Folders were copied across."
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
    def get(self, study_id):

        # param validation
        if study_id is None:
            abort(404, 'Please provide valid parameter for study identifier')
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permisions(study_id, user_token)
        if not write_access:
            abort(403)

        data_dict = json.loads(wsc.create_upload_folder(study_id, user_token))
        upload_path = data_dict["message"]

        status, message = copy_files_and_folders(upload_path, study_location)
        if status:
            return {'Success': 'Copied files from ' + upload_path}
        else:
            return {'Error': message}
