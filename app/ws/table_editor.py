#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Jan-15
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
from flask.json import jsonify

import numpy as np
import pandas as pd
from flask import request
from flask_restful import Resource, reqparse, abort
from flask_restful_swagger import swagger
from app.config import get_settings

from app.utils import MetabolightsException, metabolights_exception_handler, MetabolightsDBException
from app.ws.auth.utils import get_permission_by_obfuscation_code, get_permission_by_study_id
from app.ws.db.dbmanager import DBManager
from app.ws.db.models import StudyAccessPermission
from app.ws.study import commons
from app.ws.study.folder_utils import write_audit_files
from app.ws.study.study_service import identify_study_id
from app.ws.utils import delete_column_from_tsv_file, get_table_header, totuples, validate_row, log_request, read_tsv, write_tsv, \
    read_tsv_with_filter
from app.ws.db.schemes import Study
from app.ws.db.types import StudyStatus
import glob
"""
MTBLS Table Columns manipulator

Manage the CSV/TSV tables in MTBLS studies.
"""

logger = logging.getLogger('wslog')


def insert_row(idx, df, df_insert):
    if isinstance(df_insert, dict):
        df_insert = pd.DataFrame([df_insert])
    return pd.concat([df.iloc[:idx, ], df_insert, df.iloc[idx:, ]], ignore_index=True).reset_index(drop=True)

def filter_dataframe(filename: str, df: pd.DataFrame, df_data_dict, df_header) -> pd.DataFrame:
    if filename.startswith("m_") and filename.endswith(".tsv"):
        filtered_df = df
        filtered_df_header = df_header
        selected_columns = []
        for column in df.columns:
            header, ext = os.path.splitext(column)
            if header in default_maf_columns:
                selected_columns.append(column)
        filtered_df = df[selected_columns]
        filtered_df_header = get_table_header(filtered_df)
        df_data_dict = totuples(filtered_df.reset_index(), 'rows')
        return df_data_dict, filtered_df_header
    return df_data_dict, df_header

def get_dataframe(filename, file_path) -> pd.DataFrame:
        maf_file = False
        if filename.startswith("m_") and filename.endswith(".tsv"):
            maf_file = True
        try:
            if maf_file:
                col_names = pd.read_csv(file_path, sep="\t", nrows=0).columns
                selected_columns = []
                for column in col_names:
                    header, ext = os.path.splitext(column)
                    if header in default_maf_columns:
                        selected_columns.append(column)
                file_df = read_tsv(file_path, selected_columns)
            else:
                file_df = read_tsv(file_path)
            return file_df
        except Exception:
            abort(400, message="The file name was not found")

class SimpleColumns(Resource):
    @swagger.operation(
        summary="Add a new column to the given TSV file",
        nickname="Add table column",
        notes="Update an csv/tsv table for a given Study. Only '.tsv', '.csv' or '.txt' files are allowed.",
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

        
        
        new_column_name = None
        
        new_column_position = None
        
        new_column_default_value = None

        if request.args:
            
            new_column_name = request.args.get('new_column_name')
            new_column_position = request.args.get('new_column_position')
            new_column_default_value = request.args.get('new_column_default_value')

        if new_column_name is None:
            abort(404, message="Please provide valid name for the new column")

        # param validation
        if study_id is None or file_name is None:
            abort(404, message='Please provide valid parameters for study identifier and file name')
        study_id = study_id.upper()

        fname, ext = os.path.splitext(file_name)
        ext = ext.lower()
        if ext not in ('.tsv', '.csv', '.txt'):
            abort(400, message="The file " + file_name + " is not a valid TSV or CSV file")

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = commons.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)
        file_basename = file_name
        file_name = os.path.join(study_location, file_name)
        try:
            table_df = read_tsv(file_name)
        except FileNotFoundError:
            abort(400, message="The file " + file_name + " was not found")

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
        df_data_dict, df_header = filter_dataframe(file_basename, table_df, df_data_dict, df_header)
        return {'header': df_header, 'data': df_data_dict, 'message': message}


class ComplexColumns(Resource):
    @swagger.operation(
        summary="Add new columns to the given TSV file",
        nickname="Add table columns",
        notes="Update an csv/tsv table for a given Study. Only '.tsv', '.csv' or '.txt' files are allowed. "
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
            abort(417, message="Please provide valid key-value pairs for the new columns. The JSON string has to have a 'data' element")

        # param validation
        if study_id is None or file_name is None:
            abort(404, message='Please provide valid parameters for study identifier and/or file name')
        study_id = study_id.upper()

        fname, ext = os.path.splitext(file_name)
        ext = ext.lower()
        if ext not in ('.tsv', '.csv', '.txt'):
            abort(400, message="The file " + file_name + " is not a valid TSV or CSV file")

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = commons.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)
        file_basename = file_name
        file_name = os.path.join(study_location, file_name)
        try:
            table_df = read_tsv(file_name)
        except FileNotFoundError:
            abort(400, message="The file " + file_name + " was not found")

        audit_status, dest_path = write_audit_files(study_location)

        # Get an indexed header row
        df_header = get_table_header(table_df)

        for column in sorted(new_columns, key=lambda x: x['index']):
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
        df_data_dict, df_header = filter_dataframe(file_basename, table_df, df_data_dict, df_header)
        return {'header': df_header, 'rows': df_data_dict, 'message': message}

    @swagger.operation(
        summary="Delete columns from a tsv file",
        nickname="Delete columns from a tsv file",
        notes='''Delete given columns from a sample, assay or MAF sheet (tsv files). 
        Only '.tsv', '.csv' or '.txt' files are allowed.
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
    def delete(self, study_id, file_name):

        # param validation
        if study_id is None or file_name is None:
            abort(417, message="Please provide a study id and TSV file name")

        fname, ext = os.path.splitext(file_name)
        ext = ext.lower()
        if ext not in ('.tsv', '.csv', '.txt'):
            abort(400, message="The file " + file_name + " is not a valid TSV or CSV file")

        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            delete_columns = data_dict['data']
        except Exception as e:
            abort(417, message=str(e))

        # param validation
        columns = delete_columns['columns']
        if columns is None:
            abort(417, message='Please ensure the JSON contains a "columns" element')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            commons.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        audit_status, dest_path = write_audit_files(study_location)

        
        tsv_file = os.path.join(study_location, file_name)
        if not os.path.isfile(tsv_file):
            abort(406, message="File " + file_name + " does not exist")
        else:
            file_df = read_tsv(tsv_file)
            try:
                for column in columns:
                    delete_column_from_tsv_file(file_df, column)
                write_tsv(file_df, tsv_file)
            except Exception as e:
                logger.error("Could not remove column '" + column + "' from file " + file_name)
                logger.error(str(e))
                return {"Success": "Failed to remove column(s) from " + file_name} 

        return {"Success": "Removed column(s) from " + file_name}


class ColumnsRows(Resource):
    @swagger.operation(
        summary="Update a given cell, based on row and column index",
        nickname="Update table columns",
        notes="""Update an TSV table for a given Study. Only '.tsv', '.csv' or '.txt' files are allowed.
        
        <code><pre>
        {
        "data": [
            {
                "row": 1,
                "column": 0,
                "value": "test"
            },
            {
                "row": 1,
                "column": 1,
                "value": "test2"
            }
            ]
        }
        </pre>
        </code>
        """,
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
    @metabolights_exception_handler
    def put(self, study_id, file_name):

        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            columns_rows = data_dict['data']
        except KeyError:
            columns_rows = None

        if columns_rows is None:
            abort(404, message="Please provide valid key-value pairs for the cell value."
                       "The JSON string has to have a 'data' element")

        # param validation
        if study_id is None or file_name is None:
            abort(404, message='Please provide valid parameters for study identifier and/or file name')

        fname, ext = os.path.splitext(file_name)
        ext = ext.lower()
        if ext not in ('.tsv', '.csv', '.txt'):
            abort(400, message="The file " + file_name + " is not a valid TSV or CSV file")

        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = commons.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)
        file_basename: str = file_name
        maf_file = True if file_basename.startswith("m_") and file_basename.endswith(".tsv") else False
        file_name = os.path.join(study_location, file_name)
        headers = {}
        try:
            table_df: pd.DataFrame = read_tsv(file_name)
            headers = {idx: column for idx, column in enumerate(table_df.columns)}
        except FileNotFoundError:
            abort(404, message="The file " + file_name + " was not found or not valid")
        other_columns = {}
        for column in columns_rows:
            cell_value = column['value']
            row_index = column['row']
            column_index = column['column']
            if maf_file:
                other_columns[column_index] = table_df.columns[column_index]
            #  Need to add values for column and row (not header)
            try:
                # for row_val in range(table_df.shape[0]):
                table_df.iloc[int(row_index), int(column_index)] = cell_value
            except ValueError as e:
                logger.error("(ValueError) Unable to find the required 'value', 'row' and 'column' values. Value: "
                             + cell_value + ", row: " + row_index + ", column: " + column + ". " + str(e))
                abort(417, message="(ValueError) Unable to find the required 'value', 'row' and 'column' values. Value: "
                      + cell_value + ", row: " + row_index + ", column: " + column)
            except IndexError as e:
                logger.error("(IndexError) Unable to find the required 'value', 'row' and 'column' values. Value: "
                             + cell_value + ", row: " + row_index + ", column: " + column + ". " + str(e))
                abort(417, message="(IndexError) Unable to find the required 'value', 'row' and 'column' values. Value: "
                      + cell_value + ", row: " + row_index + ", column: " + column)
        success = False
        try: 
            # Write the new row back in the file
            message = write_tsv(table_df, file_name)
            success = True if "success" in message.lower() else False
            # df_data_dict = totuples(table_df.reset_index(), 'rows')
            # Get an indexed header row
            # df_header = get_table_header(table_df)
            # df_data_dict, df_header = filter_dataframe(file_basename, table_df, df_data_dict, df_header)
            
            if maf_file:
                filtered_headers = other_columns
                filtered_headers.update({x:headers[x] for x in headers if headers[x] in default_maf_columns})
            else:
                filtered_headers = headers
                
            return {'header': filtered_headers, 'updates': columns_rows, "success": success, 'message': message}
        except Exception as ex:
            raise MetabolightsException(http_code=500, message= f"Update error {str(ex)}")

class AddRows(Resource):
    @swagger.operation(
        summary="Add a new row to the given TSV file",
        nickname="Add TSV table row",
        notes='''Update an TSV table for a given Study. Only '.tsv', '.csv' or '.txt' files are allowed.
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
            abort(417, message="Please provide valid data for updated new row(s). The JSON string has to have a 'rows' element")

        try:
            for element in new_row:
                element.pop('index', None)  # Remove "index:n" element, this is the original row number
        except:
            logger.info('No index (row num) supplied, ignoring')

        # param validation
        if study_id is None or file_name is None:
            abort(404, message='Please provide valid parameters for study identifier and TSV file name')

        fname, ext = os.path.splitext(file_name)
        ext = ext.lower()
        if ext not in ('.tsv', '.csv', '.txt'):
            abort(400, message="The file " + file_name + " is not a valid TSV or CSV file")

        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = commons.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)
        file_basename = file_name
        if file_name == 'metabolights_zooma.tsv':  # This will edit the MetaboLights Zooma mapping file
            if not is_curator:
                abort(403)
            file_name = get_settings().file_resources.mtbls_zooma_file
        else:
            file_name = os.path.join(study_location, file_name)

        try:
            file_df = read_tsv(file_name)
        except FileNotFoundError:
            abort(400, message="The file name was not found")

        # Validate column names in new rows
        valid_column_name, message = validate_row(file_df, new_row, "post")
        if not valid_column_name:
            abort(417, message=message)

        if data:
            try:
                start_index = data['index']
                if start_index == -1:
                    start_index = 0
                start_index = start_index - 0.5

            except KeyError:
                start_index = len(file_df.index)

            # Map the complete row first, update with new_row
            complete_row = {}
            for col in file_df.columns:
                complete_row[col] = ""

            if not new_row:
                logger.warning("No new row information provided. Adding empty row " + file_name + ", row " + str(complete_row))
            else:
                for row in new_row:
                    complete_row.update(row)
                    row = complete_row
                    line = pd.DataFrame(row, index=[start_index])
                    file_df = pd.concat([file_df, line], ignore_index=False)
                    file_df = file_df.sort_index().reset_index(drop=True)
                    start_index += 1

            file_df = file_df.replace(np.nan, '', regex=True)
            message = write_tsv(file_df, file_name)

        # Get an indexed header row
        df_header = get_table_header(file_df)

        # Get the updated data table
        try:
            df_data_dict = totuples(get_dataframe(file_basename, file_name), 'rows')
        except FileNotFoundError:
            abort(400, message="The file " + file_name + " was not found")
        df_data_dict, df_header = filter_dataframe(file_basename, file_df, df_data_dict, df_header)
        return {'header': df_header, 'data': df_data_dict, 'message': message}

    @swagger.operation(
        summary="Update existing rows in the given TSV file",
        nickname="Update TSV rows",
        notes='''Update rows in the TSV file for a given Study. Only '.tsv', '.csv' or '.txt' files are allowed.
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
            abort(406, message='Please provide valid parameters for study identifier and TSV file name')

        fname, ext = os.path.splitext(file_name)
        ext = ext.lower()
        if ext not in ('.tsv', '.csv', '.txt'):
            abort(400, message="The file " + file_name + " is not a valid TSV or CSV file")

        study_id = study_id.upper()

        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            new_rows = data_dict['data']  # Use "index:n" element, this is the original row number
        except KeyError:
            new_rows = None

        if new_rows is None:
            abort(404, message="Please provide valid data for updated new row(s). "
                       "The JSON string has to have a 'data' element")

        for row in new_rows:
            try:
                row_index = row['index']  # Check if we have a value in the row number(s)
            except (KeyError, Exception):
                row_index = None

            if new_rows is None or row_index is None:
                abort(404, message="Please provide valid data for the updated row(s). "
                           "The JSON string has to have an 'index:n' element in each (JSON) row. "
                           "The header row can not be updated")

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = commons.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)
        file_basename = file_name
        file_name = os.path.join(study_location, file_name)

        try:
            file_df = read_tsv(file_name)
        except FileNotFoundError:
            abort(400, message="The file " + file_name + " was not found")

        for row in new_rows:
            try:
                row_index_int = int(row['index'])
            except:
                row_index_int = None

            # Validate column names in new rows
            valid_column_name, message = validate_row(file_df, row, 'put')
            if not valid_column_name:
                abort(417, message=message)

            if row_index_int is not None:
                file_df = file_df.drop(file_df.index[row_index_int])  # Remove the old row from the spreadsheet
                # pop the "index:n" from the new_row before updating
                row.pop('index', None)  # Remove "index:n" element, this is the original row number
                file_df = insert_row(row_index_int, file_df, row)  # Update the row in the spreadsheet

        message = write_tsv(file_df, file_name)

        df_data_dict = totuples(file_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_table_header(file_df)
        df_data_dict, df_header = filter_dataframe(file_basename, file_df, df_data_dict, df_header)
        return {'header': df_header, 'data': df_data_dict, 'message': message}

    @swagger.operation(
        summary="Delete a row of the given TSV file",
        nickname="Delete TSV row",
        notes="Update TSV file for a given Study. Only '.tsv', '.csv' or '.txt' files are allowed",
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
        
        row_num = request.args.get('row_num')

        # param validation
        if study_id is None or file_name is None or row_num is None:
            abort(404)

        fname, ext = os.path.splitext(file_name)
        ext = ext.lower()
        if ext not in ('.tsv', '.csv', '.txt'):
            abort(400, message="The file " + file_name + " is not a valid TSV or CSV file")

        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = commons.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)
        file_basename = file_name
        file_name = os.path.join(study_location, file_name)
        try:
            file_df = read_tsv(file_name)
        except FileNotFoundError:
            abort(400, message="The file " + file_name + " was not found")

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
            abort(400, message="The file " + file_name + " was not found")

        df_data_dict = totuples(file_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_table_header(file_df)
        df_data_dict, df_header = filter_dataframe(file_basename, file_df, df_data_dict, df_header)
        return {'header': df_header, 'data': df_data_dict, 'message': message}

    @swagger.operation(
        summary="Get a row of the given TSV file",
        nickname="Get TSV row",
        notes="Get TSV file for a given Study. Only '.tsv', '.csv' or '.txt' files are allowed",
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
                "description": "The row number(s) to get, comma separated if more than one",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
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
    def get(self, study_id, file_name):
        # query validation
        
        row_num = request.args.get('row_num')

        # param validation
        if study_id is None or file_name is None or row_num is None:
            abort(404)

        fname, ext = os.path.splitext(file_name)
        ext = ext.lower()
        if ext not in ('.tsv', '.csv', '.txt'):
            abort(400, message="The file " + file_name + " is not a valid TSV or CSV file")

        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = commons.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)
        file_basename = file_name
        file_name = os.path.join(study_location, file_name)
        try:
            file_df = read_tsv(file_name)
        except FileNotFoundError:
            abort(400, message="The file " + file_name + " was not found")

        row_nums = row_num.split(",")
            
        # result_df.columns = file_df.columns
        # Need to remove the highest row number first as the DataFrame dynamically re-orders when one row is removed
        sorted_num_rows = [int(x) for x in row_nums]
        sorted_num_rows.sort(reverse=False)
        count = 0
        result_df = file_df.filter(items=sorted_num_rows, axis=0)



        df_data_dict = totuples(result_df, 'rows')

        # Get an indexed header row
        df_header = get_table_header(file_df)
        df_data_dict, df_header = filter_dataframe(file_basename, file_df, df_data_dict, df_header)
        return {'header': df_header, 'data': df_data_dict, 'message': ""}

class GetTsvFile(Resource):
    @swagger.operation(
        summary="Get TSV table for a study using filename",
        nickname="Get TSV table for a given study",
        notes="Get a given TSV table for a MTBLS Study with in JSON format. Only '.tsv', '.csv' or '.txt' files are allowed",
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
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "obfuscation-code",
                "description": "obfuscation code of study",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False
            },
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
    @metabolights_exception_handler
    def get(self, study_id, file_name):
        # param validation
        if study_id is None or file_name is None:
            logger.info('No study_id and/or TSV file name given')
            abort(404)

        fname, ext = os.path.splitext(file_name)
        ext = ext.lower()
        if ext not in ('.tsv', '.csv', '.txt'):
            abort(400, message="The file " + file_name + " is not a valid TSV or CSV file")

        study_id = study_id.upper()
        file_name_param = file_name  # store the passed filename for simplicity

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
            
        obfuscation_code = None
        if "obfuscation_code" in request.headers:
            obfuscation_code = request.headers["obfuscation_code"]
            
        study_id, obfuscation_code = identify_study_id(study_id, obfuscation_code)
        logger.info('Assay Table: Getting ISA-JSON Study Assay Table: Getting ISA-JSON Study %s', study_id)
        # check for access rights
        if obfuscation_code:
            permission: StudyAccessPermission = get_permission_by_obfuscation_code(user_token, obfuscation_code)
        else:
            permission: StudyAccessPermission = get_permission_by_study_id(study_id, user_token)
        
        # permission.view
        if not permission.view:
            abort(403)
        file_basename = file_name
        if file_name == 'metabolights_zooma.tsv':  # This will edit the MetaboLights Zooma mapping file
            if not permission.userRole != "ROLE_SUPER_USER":
                abort(403)
            file_name = get_settings().file_resources.mtbls_zooma_file
        else:
            study_location = os.path.join(get_settings().study.mounted_paths.study_metadata_files_root_path, study_id)
            file_name = os.path.join(study_location, file_name)
        
        if not os.path.exists(file_name):
            raise MetabolightsException(http_code=404, message=f"{file_basename} does not exist on {study_id} metadata folder.")

        logger.info('Trying to load TSV file (%s) for Study %s', file_name, study_id)
        # Get the Assay table or create a new one if it does not already exist
        maf_file = False
        col_hidden = False
        sample_abundance = False
        if file_basename.startswith("m_") and file_basename.endswith(".tsv"):
            maf_file = True
        try:
            if maf_file:
                col_names = read_tsv(file_name, nrows=0).columns 
                col_length = len(col_names)
                if col_length > 23:
                    selected_columns = []
                    non_default_columns = []
                    for column in col_names:
                        header, ext = os.path.splitext(column)
                        if header in default_maf_columns:
                            selected_columns.append(column)
                        else:
                            non_default_columns.append(column)
                    if len(selected_columns) > 0:
                        file_df = read_tsv(file_name, selected_columns)
                        col_hidden = True
                        non_default_columns_len = len(non_default_columns)
                        if non_default_columns_len > 0:
                            first_five_non_default_columns = non_default_columns[:5]
                            data_df = read_tsv(file_name, first_five_non_default_columns,nrows=10)
                            for i in range(0, len(data_df)):
                                val = data_df.iloc[i][first_five_non_default_columns[0]]
                                if val:
                                    sample_abundance = True
                                    break
                                if non_default_columns_len > 1:
                                    val = data_df.iloc[i][first_five_non_default_columns[1]]
                                    if val:
                                        sample_abundance = True
                                        break
                                if non_default_columns_len > 2:
                                    val = data_df.iloc[i][first_five_non_default_columns[2]]
                                    if val:
                                        sample_abundance = True
                                        break
                                if non_default_columns_len > 3:
                                    val = data_df.iloc[i][first_five_non_default_columns[3]]
                                    if val:
                                        sample_abundance = True
                                        break
                                if non_default_columns_len > 4:
                                    val = data_df.iloc[i][first_five_non_default_columns[4]]
                                    if val:
                                        sample_abundance = True
                                        break
                else:
                    file_df = read_tsv(file_name)
            else:
                file_df = read_tsv(file_name)
        except FileNotFoundError:
            abort(400, message="The file " + file_name + " was not found")

        df_data_dict = totuples(file_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_table_header(file_df, study_id, file_name_param)
        df_data_dict, df_header = filter_dataframe(file_basename, file_df, df_data_dict, df_header)
        return {'header': df_header, 'data': df_data_dict, 'columns_hidden': col_hidden, 'sample_abundance': sample_abundance}


class TsvFileRows(Resource):
    @swagger.operation(
        summary="Get TSV table rows for a study using filename, column names and page number",
        nickname="Get TSV table rows for a study using filename, column names and page number",
        notes="""Get a given TSV table rows and columns for a MTBLS Study with in JSON format. Only '.tsv' and txt files are allowed
        <code><pre>
        {
            "data": {
                "isaFileName": "m_xyz.tsv",
                "columnNames": ["database_identifier", "smiles"],
                "pageNumber": 0,
                "pageSize": 50
            }
        }
        </pre>
        </code>
        """,
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
                "description": "data input",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
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
    @metabolights_exception_handler
    def post(self, study_id):
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            request_data = data_dict['data']
        except KeyError:
            request_data = None
            
        # param validation
        if not study_id or not request_data or "isaFileName" not in request_data or not request_data["isaFileName"]:
            logger.info('No study_id or file name')
            abort(404)
        file_name = request_data["isaFileName"]
        
        _, ext = os.path.splitext(file_name)
        ext = ext.lower()
        if ext not in ('.tsv', '.txt'):
            abort(400, message="The file " + file_name + " is not a valid TSV or CSV file")

        study_id = study_id.upper()
        page_number = 0
        if "pageNumber" in request_data and request_data["pageNumber"]:
            page_number = int(request_data["pageNumber"])

        page_size = 100
        if "pageSize" in request_data and request_data["pageSize"]:
            page_size = int(request_data["pageSize"])
        column_names = []
        if "columnNames" in request_data and request_data["columnNames"]:
            column_names = request_data["columnNames"]
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
            
        logger.info('Assay Table: Getting ISA-JSON Study Assay Table: Getting ISA-JSON Study %s', study_id)
        # check for access rights
        
        permission: StudyAccessPermission = get_permission_by_study_id(study_id, user_token)
        
        # permission.view
        # is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        #     study_status = commons.get_permissions(study_id, user_token, obfuscation_code)
        if not permission.view:
            abort(403)
        metadata = {
            "file": file_name,
            "columnNames": [],
            "pageNumber": 0,
            "pageSize": 0,
            "defaultPageSize": 100,
            "totalSize": 0,
            "columns": [],
            "currentFilters": [],
            "currentSortOptions": []
        }
        try:
            study_location = os.path.join(get_settings().study.mounted_paths.study_metadata_files_root_path, study_id)
            file_path = os.path.join(study_location, file_name)
            
            if not os.path.exists(file_path):
                raise MetabolightsException(http_code=404, message=f"{file_name} does not exist on {study_id} metadata folder.")

            logger.info('Trying to load TSV file (%s) for Study %s', file_path, study_id)
            
            file_column_names = pd.read_csv(file_path, sep="\t", nrows=0).columns

            if len(file_column_names) > 0:
                columns = []
                for idx, name in enumerate(file_column_names):
                    data = { "columnDef": name, "header": name, "sticky": "false", "targetColumnIndex": int(idx), "calculated": False}
                    columns.append(data)
                valid_column_names = file_column_names
                if column_names: 
                    valid_column_names = [x for x in column_names if x in file_column_names]
                    columns = [x for x in columns if x["columnDef"] in valid_column_names]
                    file_df = read_tsv(file_path, col_names=valid_column_names, skiprows=range(1, page_size*page_number + 1), nrows=page_size)
                else:
                    file_df = read_tsv(file_path, col_names=None, skiprows=range(1, page_size*page_number + 1), nrows=page_size)
                df_data_dict = to_tuple_with_index(file_df, skippedRows=page_size*page_number)
                # for row in df_data_dict["rows"]:
                #     row.index = int(row.index)
                file_df_total = read_tsv(file_path, col_names=[valid_column_names[0]])
                metadata["totalSize"] = int(file_df_total.size)
                metadata["columnNames"] = valid_column_names
                metadata["pageNumber"] = int(page_number)
                metadata["pageSize"] = int(file_df.size)
                metadata["columns"] = columns
                
                return jsonify({"content": {'metadata': metadata, 'rows': df_data_dict}})
            return jsonify({"content": {'metadata': metadata, 'rows': []}})
        except FileNotFoundError:
            abort(400, message="The file " + file_path + " was not found or file is not valid.")

        

        # Get an indexed header row
        
def to_tuple_with_index(df: pd.DataFrame, skippedRows=0):
    rows = []
    for index, row in df.iterrows():
        data = dict([
            (colname, row[i])
            for i, colname in enumerate(df.columns)
        ])
        data["index"] = index + skippedRows
        rows.append(data)
    
    return rows

        
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


default_maf_columns = {
    'database_identifier',
    'chemical_formula',
    'smiles',
    'inchi',
    'metabolite_identification',
    'mass_to_charge',
    'fragmentation',
    'modifications',
    'charge',
    'retention_time',
    'taxid',
    'species',
    'database',
    'database_version',
    'reliability',
    'uri',
    'search_engine',
    'search_engine_score',
    'smallmolecule_abundance_sub',
    'smallmolecule_abundance_stdev_sub',
    'smallmolecule_abundance_std_error_sub',
    'chemical_shift',
    'multiplicity'
    }

class GetAssayMaf(Resource):
    @swagger.operation(
        summary="Get MAF data for a given public study and MAF sheet number",
        nickname="Get MAF data for a given study",
        notes="Get a Database Identifier and Metabolite identification. This API is used by EB EYE search cronjob script",
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
                "name": "sheet_number",
                "description": "Sheet order number",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "int"
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The MAF data is returned"
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
    @metabolights_exception_handler
    def get(self, study_id, sheet_number):
        # param validation
        if study_id is None or sheet_number is None:
            logger.info('No study_id and/or sheet_number given')
            abort(404)

        study_id = study_id.upper()

        with DBManager.get_instance().session_maker() as db_session:
            query = db_session.query(Study)
            query = query.filter(Study.status == StudyStatus.PUBLIC.value,
                                 Study.acc == study_id)
            study = query.first()

            if not study:
                raise MetabolightsDBException(f"{study_id} does not exist or is not public")

        logger.info('Trying to load MAF for Study %s, Sheet number %d', study_id, sheet_number)

        study_path = get_settings().study.mounted_paths.study_metadata_files_root_path
        study_location = os.path.join(study_path, study_id)
        maflist = []

        for maf in glob.glob(os.path.join(study_location, "m_*.tsv")):
            maf_file_name = os.path.basename(maf)
            logger.info(' Adding MAF file :- %s', maf_file_name)
            maflist.append(maf_file_name)

        logger.info(' Requested Sheet number :- %d', sheet_number)

        maf_file = maflist[sheet_number-1]
        logger.info(' maf_file path :- %s', maf_file)
        maf_file_path = os.path.join(study_location, maf_file)
        try:
            file_df = read_tsv_with_filter(maf_file_path)
        except FileNotFoundError:
            abort(400, message="The file " + maf_file_path + " was not found")

        df_data_dict = totuples(file_df.reset_index(), 'rows')
        result = {'content': {'metaboliteAssignmentFileName': maf_file, 'data': df_data_dict}, 'message': None, "err": None}
        return result