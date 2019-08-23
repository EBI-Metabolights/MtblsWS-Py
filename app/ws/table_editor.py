#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Apr-05
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

import logging
import json
import os
import pandas as pd
import numpy as np
from flask import request, abort, current_app as app
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from app.ws.mtblsStudy import write_audit_files
from app.ws.utils import get_table_header, totuples, validate_row, log_request, read_tsv, write_tsv

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
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        file_name = os.path.join(study_location, file_name)
        try:
            table_df = read_tsv(file_name)
        except FileNotFoundError:
            abort(400, "The file " + file_name + " was not found")

        audit_status, dest_path = write_audit_files(study_location)

        #  Need to add values for each existing row (not header)
        new_col = []
        for row_val in range(table_df.shape[0]):
            new_col.append(new_column_default_value)

        # Add new column to the spreadsheet
        table_df.insert(loc=int(new_column_position), column=new_column_name, value=new_col, allow_duplicates=True)

        df_data_dict = totuples(table_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_table_header(table_df)

        message = write_tsv(table_df, file_name)

        return {'header': df_header, 'data': df_data_dict, 'message': message}


class ComplexColumns(Resource):
    @swagger.operation(
        summary="Add new columns to the given TSV file",
        nickname="Add table columns",
        notes="Update an csv/tsv table for a given Study. "
              "Please note that if the column name already exists at the given position, "
              "this will <b>update</b> the rows for the column",
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
                "description": "The columns to add to/update in the file",
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
            abort(417, "Please provide valid key-value pairs for the new columns."
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
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        file_name = os.path.join(study_location, file_name)
        try:
            table_df = read_tsv(file_name)
        except FileNotFoundError:
            abort(400, "The file " + file_name + " was not found")

        audit_status, dest_path = write_audit_files(study_location)

        # Get an indexed header row
        df_header = get_table_header(table_df)

        for column in new_columns:
            new_column_default_value = column['value']
            new_column_name = column['name']
            new_column_position = column['index']

            #  Need to add values for each existing row (not header)
            new_col = []
            for row_val in range(table_df.shape[0]):
                new_col.append(new_column_default_value)

            # Check if we already have the column in the current position
            try:
                header_name = table_df.iloc[:, new_column_position].name
            except:
                header_name = ""

            if header_name == new_column_name:  # We should update the existing column
                table_df.iloc[:, new_column_position] = new_col
            else:
                # Add new column to the spreadsheet
                table_df.insert(loc=int(new_column_position), column=new_column_name,
                                value=new_col, allow_duplicates=True)

        # Get an (updated) indexed header row
        df_header = get_table_header(table_df)

        # Get all indexed rows
        df_data_dict = totuples(table_df.reset_index(), 'rows')

        message = write_tsv(table_df, file_name)

        return {'header': df_header, 'rows': df_data_dict, 'message': message}

    @swagger.operation(
        summary="Delete columns from a tsv file",
        nickname="Delete columns from a tsv file",
        notes='''Delete given columns from a sample, assay or MAF sheet (tsv files).
<pre><code> 
{  
  "data": { 
    "columns": [
         "column name 1" , 
         "column name 2" 
    ]
  }
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
                "name": "file_name",
                "description": "the CSV or TSV file name",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "tsv_files",
                "description": "TSV File names",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False
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
            },
            {
                "code": 417,
                "message": "Incorrect parameters provided"
            }
        ]
    )
    def delete(self, study_id, file_name):

        # param validation
        if study_id is None or file_name is None:
            abort(417)

        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            delete_columns = data_dict['data']
        except Exception as e:
            abort(417, str(e))

        # param validation
        columns = delete_columns['columns']
        if columns is None:
            abort(417, 'Please ensure the JSON contains a "columns" element')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        audit_status, dest_path = write_audit_files(study_location)

        for column in columns:
            tsv_file = os.path.join(study_location, file_name)
            if not os.path.isfile(tsv_file):
                abort(406, "File " + file_name + " does not exist")
            else:
                file_df = read_tsv(tsv_file)
                try:
                    file_df.drop(column, axis=1, inplace=True)
                    write_tsv(file_df, tsv_file)
                except Exception as e:
                    logger.error("Could not remove column '" + column + "' from file " + file_name)
                    logger.error(str(e))

        return {"Success": "Removed column(s) from " + file_name}


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
    def put(self, study_id, file_name):

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
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        file_name = os.path.join(study_location, file_name)
        try:
            table_df = read_tsv(file_name)
        except FileNotFoundError:
            abort(400, "The file " + file_name + " was not found")

        for column in columns_rows:
            cell_value = column['value']
            row_index = column['row']
            column_index = column['column']
            #  Need to add values for column and row (not header)
            try:
                # for row_val in range(table_df.shape[0]):
                table_df.iloc[int(row_index), int(column_index)] = cell_value
            except ValueError:
                logger.error("Unable to find the required 'value', 'row' and 'column' values. Value: "
                             + cell_value + ", row: " + row_index + ", column: " + column)
                abort(417, "Unable to find the required 'value', 'row' and 'column' values. Value: "
                      + cell_value + ", row: " + row_index + ", column: " + column)

        # Write the new row back in the file
        message = write_tsv(table_df, file_name)

        df_data_dict = totuples(table_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_table_header(table_df)

        return {'header': df_header, 'rows': df_data_dict, 'message': message}


class AddRows(Resource):
    @swagger.operation(
        summary="Add a new row to the given TSV file",
        nickname="Add TSV table row",
        notes='''Update an TSV table for a given Study.
        <p>Please make sure you add a value for echo column/cell combination. 
        Use the GET method to see all the columns for this tsv file<br> If you do not provide the row "index" parameter,
        the row will be added at the end of the TSV table
<pre><code>{
    "data": {
        "index": 4,
        "rows": [
            {
                "column name 1": "cell value 1",
                "column name 2": "cell value 2",
                "etc": "etc"
            }
        ]
    }
}</code></pre>''',
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
        log_request(request)
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['data']
            new_row = data['rows']
        except KeyError:
            new_row = None
            data = None

        if new_row is None:
            abort(417, "Please provide valid data for updated new row(s). The JSON string has to have a 'rows' element")

        try:
            for element in new_row:
                element.pop('index', None)  # Remove "index:n" element, this is the original row number
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
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        if file_name == 'metabolights_zooma.tsv':  # This will edit the MetaboLights Zooma mapping file
            if not is_curator:
                abort(403)
            file_name = app.config.get('MTBLS_ZOOMA_FILE')
        else:
            file_name = os.path.join(study_location, file_name)

        try:
            file_df = read_tsv(file_name)
        except FileNotFoundError:
            abort(400, "The file name was not found")

        # Validate column names in new rows
        valid_column_name, message = validate_row(file_df, new_row, "post")
        if not valid_column_name:
            abort(417, message)

        if data:
            try:
                start_index = data['index']
                if start_index == -1:
                    start_index = 0
                start_index = start_index - 0.5

            except KeyError:
                start_index = len(file_df.index)

            for row in new_row:
                if not row:
                    col0 = file_df.columns[0]
                    row = {col0: ""}

                line = pd.DataFrame(row, index=[start_index])
                file_df = file_df.append(line, ignore_index=False)
                file_df = file_df.sort_index().reset_index(drop=True)
                start_index += 1

        # if new_row[0]:
        #     file_df = file_df.append(new_row, ignore_index=True)  # Add new row to the spreadsheet (TSV file)
        # else:
        #     file_df = file_df.append(pd.Series(), ignore_index=True)

        file_df = file_df.replace(np.nan, '', regex=True)
        message = write_tsv(file_df, file_name)

        # Get an indexed header row
        df_header = get_table_header(file_df)

        # Get the updated data table
        try:
            df_data_dict = totuples(read_tsv(file_name), 'rows')
        except FileNotFoundError:
            abort(400, "The file " + file_name + " was not found")

        return {'header': df_header, 'data': df_data_dict, 'message': message}

    @swagger.operation(
        summary="Update existing rows in the given TSV file",
        nickname="Update TSV rows",
        notes='''Update rows in the TSV file for a given Study.
        <p>Please make sure you add a value for echo column/cell combination. 
        Use the GET method to see all the columns for this tsv file<br>
<pre><code>{
  "data": [
    {
      "column name 1": "cell value 1",
      "column name 2": "cell value 2",
      "etc": "etc"
    }
  ]
}</code></pre>''',
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
                "code": 406,
                "message": "Please provide valid parameters for study identifier and TSV file name"
            },
            {
                "code": 417,
                "message": "The column name given does not exist in the TSV file"
            }
        ]
    )
    def put(self, study_id, file_name):
        # param validation
        if study_id is None or file_name is None:
            abort(406, 'Please provide valid parameters for study identifier and TSV file name')
        study_id = study_id.upper()

        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            new_rows = data_dict['data']  # Use "index:n" element, this is the original row number
        except KeyError:
            new_rows = None

        if new_rows is None:
            abort(404, "Please provide valid data for updated new row(s). "
                       "The JSON string has to have a 'data' element")

        for row in new_rows:
            try:
                row_index = row['index']  # Check if we have a value in the row number(s)
            except (KeyError, Exception):
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
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        file_name = os.path.join(study_location, file_name)

        try:
            file_df = read_tsv(file_name)
        except FileNotFoundError:
            abort(400, "The file " + file_name + " was not found")

        for row in new_rows:
            try:
                row_index_int = int(row['index'])
            except:
                row_index_int is None

            # Validate column names in new rows
            valid_column_name, message = validate_row(file_df, row, 'put')
            if not valid_column_name:
                abort(417, message)

            if row_index_int is not None:
                file_df = file_df.drop(file_df.index[row_index_int])  # Remove the old row from the spreadsheet
                # pop the "index:n" from the new_row before updating
                row.pop('index', None)  # Remove "index:n" element, this is the original row number
                file_df = insert_row(row_index_int, file_df, row)  # Update the row in the spreadsheet

        message = write_tsv(file_df, file_name)

        df_data_dict = totuples(file_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_table_header(file_df)

        return {'header': df_header, 'data': df_data_dict, 'message': message}

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
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        file_name = os.path.join(study_location, file_name)
        try:
            file_df = read_tsv(file_name)
        except FileNotFoundError:
            abort(400, "The file " + file_name + " was not found")

        row_nums = row_num.split(",")

        # Need to remove the highest row number first as the DataFrame dynamically re-orders when one row is removed
        sorted_num_rows = [int(x) for x in row_nums]
        sorted_num_rows.sort(reverse=True)
        for num in sorted_num_rows:
            file_df = file_df.drop(file_df.index[num])  # Drop row(s) in the spreadsheet

        message = write_tsv(file_df, file_name)

        # To be sure we read the file again
        try:
            file_df = read_tsv(file_name)
        except FileNotFoundError:
            abort(400, "The file " + file_name + " was not found")

        df_data_dict = totuples(file_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_table_header(file_df)

        return {'header': df_header, 'data': df_data_dict, 'message': message}


class GetTsvFile(Resource):
    @swagger.operation(
        summary="Get TSV table for a study using filename",
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
        file_name_param = file_name  # store the passed filename for simplicity

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info('Assay Table: Getting ISA-JSON Study %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        if file_name == 'metabolights_zooma.tsv':  # This will edit the MetaboLights Zooma mapping file
            if not is_curator:
                abort(403)
            file_name = app.config.get('MTBLS_ZOOMA_FILE')
        else:
            file_name = os.path.join(study_location, file_name)

        logger.info('Trying to load TSV file (%s) for Study %s', file_name, study_id)
        # Get the Assay table or create a new one if it does not already exist
        try:
            file_df = read_tsv(file_name)
        except FileNotFoundError:
            abort(400, "The file " + file_name + " was not found")

        df_data_dict = totuples(file_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_table_header(file_df, study_id, file_name_param)

        return {'header': df_header, 'data': df_data_dict}
