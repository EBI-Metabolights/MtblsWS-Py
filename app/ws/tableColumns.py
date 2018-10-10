import logging
import pandas as pd
import numpy as np
import json
from flask import request, abort
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import get_table_header, totuples

"""
MTBLS Table Columns manipulator

Manage the CSV/TSV tables in MTBLS studies.
"""

logger = logging.getLogger('wslog')

# MetaboLights (Java-Based) WebService client
wsc = WsClient()


class SimpleColumns(Resource):
    @swagger.operation(
        summary="Add a new column to the given tsv/csv file",
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
                "required": False,
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

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)

        study_path = wsc.get_study_location(study_id, user_token)
        file_name = study_path + "/" + file_name

        table_df = pd.read_csv(file_name, sep="\t", header=0, encoding='utf-8')
        table_df = table_df.replace(np.nan, '', regex=True)  # Remove NaN

        #  Need to add values for each existing row (not header)
        new_col = []
        for row_val in range(table_df.shape[0]):
            new_col.append(new_column_default_value)

        table_df.insert(loc=int(new_column_position), column=new_column_name, value=new_col)  # Add new column to the spreadsheet

        # Write the new row back in the file
        table_df.to_csv(file_name, sep="\t", encoding='utf-8', index=False)

        df_data_dict = totuples(table_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_table_header(table_df)

        return {'tableHeader': df_header, 'tableData': df_data_dict}


class ComplexColumns(Resource):
    @swagger.operation(
        summary="Add new columns to the given tsv/csv file",
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
                "required": False,
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
            new_columns = data_dict['tableData']
        except KeyError:
            new_columns = None

        if new_columns is None:
            abort(404, "Please provide valid key-value pairs for the new columns."
                       "The JSON string has to have a 'tableData' element")

        # param validation
        if study_id is None or file_name is None:
            abort(404, 'Please provide valid parameters for study identifier and/or file name')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)

        study_path = wsc.get_study_location(study_id, user_token)
        file_name = study_path + "/" + file_name

        table_df = pd.read_csv(file_name, sep="\t", header=0, encoding='utf-8')
        table_df = table_df.replace(np.nan, '', regex=True)  # Remove NaN

        for column in new_columns:
            new_column_default_value = column['Value']
            new_column_name = column['Name']
            new_column_position = column['Position']

            #  Need to add values for each existing row (not header)
            new_col = []
            for row_val in range(table_df.shape[0]):
                new_col.append(new_column_default_value)

            table_df.insert(loc=int(new_column_position), column=new_column_name, value=new_col)  # Add new column to the spreadsheet

        # Write the new row back in the file
        table_df.to_csv(file_name, sep="\t", encoding='utf-8', index=False)

        df_data_dict = totuples(table_df.reset_index(), 'rows')

        # Get an indexed header row
        df_header = get_table_header(table_df)

        return {'tableHeader': df_header, 'tableData': df_data_dict}
