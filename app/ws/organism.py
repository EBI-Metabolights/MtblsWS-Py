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

from flask import request, abort
from flask_restful import Resource, reqparse
from marshmallow import ValidationError
from app.ws.isaApiClient import IsaApiClient
from app.ws.mm_models import *
from app.ws.mtblsWSclient import WsClient
from app.ws.models import *
from flask_restful_swagger import swagger
from app.ws.utils import log_request, add_ontology_to_investigation, read_tsv, \
    update_ontolgies_in_isa_tab_sheets, update_characteristics_in_sample_sheet
import logging
import os

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


def get_sample_details(study_id, user_token, study_location):
    isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                     skip_load_tables=True,
                                                     study_location=study_location)
    try:
        if isa_study:
            sample_file = isa_study.filename
            sample_df = read_tsv(os.path.join(study_location, sample_file))
    except FileNotFoundError:
        abort(500, 'Error: Could not find ISA-Tab metadata files')

    try:
        organism_pos = sample_df.columns.get_loc('Characteristics[Organism]')
    except:
        organism_pos = None

    try:
        organism_part_pos = sample_df.columns.get_loc('Characteristics[Organism part]')
    except:
        organism_part_pos = None

    try:
        variant_pos = sample_df.columns.get_loc('Characteristics[Variant]')
    except:
        variant_pos = None

    unique_org_count = 0
    all_orgs = []
    all_orgs_with_index = []
    for idx, sample in sample_df.iterrows():
        try:
            org_term = sample[organism_pos + 2]
        except:
            org = ""

        try:
            org_part_term = sample[organism_part_pos + 2]
        except:
            org_part = ""

        complete_org = org + "|" + org_part

        if complete_org not in all_orgs:
            unique_org_count += 1
            all_orgs.append(complete_org)
            all_orgs_with_index.append(complete_org + '|' + str(unique_org_count))

    return all_orgs_with_index  # all_orgs
