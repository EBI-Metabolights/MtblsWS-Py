#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Dec-12
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
import os

from flask import request, abort
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from marshmallow import ValidationError

from app.ws.isaApiClient import IsaApiClient
from app.ws.models import *
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import log_request, add_ontology_to_investigation, read_tsv, write_tsv, \
    update_ontolgies_in_isa_tab_sheets, totuples

logger = logging.getLogger('wslog')
iac = IsaApiClient()
wsc = WsClient()


class Organism(Resource):
    @swagger.operation(
        summary='Update Study Organism and Organism part',
        notes='''Update Study Organism and Organism part.
                  <br>
                  Use existing characteristics name and value as query parameters to update specific 
                  Organism or Organism part.<pre><code>
    {
        "characteristics": [
            {
                "comments": [],
                "characteristicsName": "Organism",
                "characteristicsType": {
                    "comments": [],
                    "annotationValue": "Homo sapiens",
                    "termSource": {
                        "comments": [],
                        "name": "NCBITAXON",
                        "file": "http://www.ontobee.org/ontology/NCBITaxon",
                        "version": "4",
                        "description": "National Center for Biotechnology Information (NCBI) Organismal Classification"
                    },
                    "termAccession": "http://purl.obolibrary.org/obo/NCBITaxon_9606"
                }
            }
        ]
    }</pre></code>''',
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
            },
            {
                "name": "existing_char_name",
                "description": "Existing Characteristics name",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "existing_char_value",
                "description": "Existing Characteristics value",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "data",
                "description": 'Characteristics in ISA-JSON format.',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "save_audit_copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax."
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
                "code": 412,
                "message": "The JSON provided can not be parsed properly."
            }
        ]
    )
    def post(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('existing_char_name', help="Characteristics name")
        parser.add_argument('existing_char_value', help="Characteristics value")
        args = parser.parse_args()
        existing_characteristics_name = args['existing_char_name']
        existing_characteristics_value = args['existing_char_value']
        if existing_characteristics_name is None or existing_characteristics_value is None:
            abort(404)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            # user token is required
            abort(401)

        # check for keeping copies
        save_audit_copy = False
        save_msg_str = "NOT be"
        if "save_audit_copy" in request.headers and \
                request.headers["save_audit_copy"].lower() == 'true':
            save_audit_copy = True
            save_msg_str = "be"

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

        # body content validation
        updated_characteristics = None
        new_column_name = None
        onto = None
        new_value = None
        new_url = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['characteristics']
            # if partial=True missing fields will be ignored
            try:
                # result = SampleSchema().load(data, many=True, partial=False)
                # We do not have to map the charcaeristics json to a schema a we are using this to directly
                # update the dataframe. The ontology we use more than one time, so map that

                new_column_name = data[0]['characteristicsName']
                char_type = data[0]['characteristicsType']
                new_value = char_type['annotationValue']
                new_url = char_type['termAccession']
                term_source = char_type['termSource']
                onto = OntologySource(
                    name=term_source['name'],
                    version=term_source['version'],
                    file=term_source['file'],
                    description=term_source['description'])

                # Check that the ontology is referenced in the investigation
                add_ontology_to_investigation(isa_inv, onto.name, onto.version, onto.file, onto.description)

            except Exception as e:
                abort(412)

        except (ValidationError, Exception):
            abort(400)

        # update Study Factor details
        logger.info('Updating Study Characteristics details for %s', study_id)

        if existing_characteristics_name != new_column_name:  # update the column header value for characteristics
            update_ontolgies_in_isa_tab_sheets('characteristics', existing_characteristics_name, new_column_name,
                                               study_location, isa_study)
        # Now, it the cell values that needs updating
        update_characteristics_in_sample_sheet(onto.name, new_url, new_column_name, existing_characteristics_value,
                                               new_value, study_location, isa_study)

        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Updated %s', existing_characteristics_value)

        return {"Success": " Sample sheet updated"}

    @swagger.operation(
        summary='Get unique Study Organism and Organism part',
        notes='Get unique Study Organism and Organism part',
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
                "message": "OK."
            },
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax."
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
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            # user token is required
            abort(401)

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

        samples = read_characteristics_from_sample_sheet(study_location, isa_study)
        return totuples(samples, 'organisms')


def update_characteristics_in_sample_sheet(onto_name, new_url, header, old_value, new_value, study_location, isa_study):
    try:
        """ 
        Update column values in sample file(s). The column header looks like 'Characteristics[<characteristics name>']
        """
        sample_file_name = os.path.join(study_location, isa_study.filename)  # Sample sheet
        header = 'Characteristics[' + header + ']'

        if sample_file_name:
            df = read_tsv(sample_file_name)
            ''' 
            This is slightly complicated in a DF, identical columns are separated with .n. "Organism part" should 
            always be the 2nd group of columns, but to be sure we should use the column position (col_pos)
            '''
            col_pos = df.columns.get_loc(header)  # Use this to determine the location of the additional columns
            header_source_ref = df.columns[col_pos + 1]  # 'Term Source REF' (+.n)
            header_acc_number = df.columns[col_pos + 2]  # 'Term Accession Number' (+.n)

            try:

                if old_value != new_value:  # Do we need to change the cell values?
                    df.loc[df[header] == old_value, header_source_ref] = onto_name  # Term Source REF(.n) changed
                    df.loc[df[header] == old_value, header_acc_number] = new_url  # Term Accession Number(.n) changed
                    df.loc[df[header] == old_value, header] = new_value  # Characteristics name changed
                    write_tsv(df, sample_file_name)
                    logger.info(old_value + " " + new_value + " has been renamed in " + sample_file_name)
            except Exception as e:
                logger.warning(old_value + " " + new_value +
                               " was not used in the sheet or we failed updating " + sample_file_name +
                               ". Error: " + str(e))

    except Exception as e:
        logger.error("Could not update the ontology value " + old_value + " in " + sample_file_name)


def read_characteristics_from_sample_sheet(study_location, isa_study):
    sample_orgs = []
    try:
        sample_file_name = os.path.join(study_location, isa_study.filename)  # Sample sheet

        if sample_file_name:
            df = read_tsv(sample_file_name)
            ''' 
            This is slightly complicated in a DF, identical columns are separated with .n. "Organism part" should 
            always be the 2nd group of columns, but to be sure we should use the column position (col_pos)
            '''
            col_pos1 = df.columns.get_loc(
                'Characteristics[Organism]')  # Use this to determine the location of the additional columns
            header_source_ref1 = df.columns[col_pos1 + 1]  # 'Term Source REF'
            header_acc_number1 = df.columns[col_pos1 + 2]  # 'Term Accession Number'

            col_pos2 = df.columns.get_loc('Characteristics[Organism part]')
            header_source_ref2 = df.columns[col_pos2 + 1]  # 'Term Source REF' (+.n)
            header_acc_number2 = df.columns[col_pos2 + 2]  # 'Term Accession Number' (+.n)

            new_df = df[['Characteristics[Organism]', header_source_ref1, header_acc_number1,
                         'Characteristics[Organism part]', header_source_ref2, header_acc_number2]].copy()

            new_df.columns = ['Characteristics[Organism]', 'Term Source REF', 'Term Accession Number',
                              'Characteristics[Organism part]', 'Term Source REF.1', 'Term Accession Number.1']

            return new_df.drop_duplicates()

    except Exception as e:
        logger.error("Could not read 'Characteristics[Organism]' and/or 'Characteristics[Organism part]' in " +
                     sample_file_name)

        abort(400)
