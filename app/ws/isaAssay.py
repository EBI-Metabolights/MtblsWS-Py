#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-May-23
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
from app.ws.mm_models import *
from flask_restful_swagger import swagger
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from flask import current_app as app
from app.ws.utils import *
import logging
import json
import os.path
import csv

logger = logging.getLogger('wslog')
iac = IsaApiClient()
wsc = WsClient()


# Allow for a more detailed logging when on DEBUG mode
def log_request(request_obj):
    if app.config.get('DEBUG'):
        if app.config.get('DEBUG_LOG_HEADERS'):
            logger.debug('REQUEST HEADERS -> %s', request_obj.headers)
        if app.config.get('DEBUG_LOG_BODY'):
            logger.debug('REQUEST BODY    -> %s', request_obj.data)
        if app.config.get('DEBUG_LOG_JSON'):
            logger.debug('REQUEST JSON    -> %s', request_obj.json)


def extended_response(data=None, errs=None, warns=None):
    ext_resp = {"data": data if data else list(),
                "errors": errs if errs else list(),
                "warnings": warns if warns else list()}
    return ext_resp


def get_assay(assay_list, filename):
    for index, assay in enumerate(assay_list):
        if assay.filename.lower() == filename:
            return assay


def get_source(source_list, source_name):
    for source in source_list:
        if source.name.lower() == source_name.lower():
            return source
    return None


def get_sample(sample_list, sample_name):
    for sample in sample_list:
        if sample.name.lower() == sample_name.lower():
            return sample
    return None


def get_protocol(protocol_list, protocol_name):
    for protocol in protocol_list:
        if protocol.name.lower() == protocol_name.lower():
            return protocol
    return None


class StudyAssayDelete(Resource):
    @swagger.operation(
        summary='Delete an assay',
        notes='''Remove an assay from your study. Use the full assay file name, 
        like this: "a_MTBLS123_LC-MS_positive_hilic_metabolite_profiling.txt"''',
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
                "name": "assay_file_name",
                "description": 'Assay definition',
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "force",
                "description": "Remove related protocols",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": False,
                "default": True
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
                "code": 409,
                "message": "Conflict. The request could not be completed due to a conflict"
                           " with the current state of study. This is usually issued to prevent duplications."
            }
        ]
    )
    def delete(self, study_id, assay_file_name):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)

        if assay_file_name is None:
            abort(404)
        assay_file_name = assay_file_name.strip()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            # user token is required
            abort(401)

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('force', help='Remove related protocols')

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        # check if we should be keeping copies of the metadata
        save_audit_copy = True
        save_msg_str = "be"
        if "save_audit_copy" in request.headers:
            if request.headers["save_audit_copy"].lower() == 'true':
                save_audit_copy = True
                save_msg_str = "be"
            else:
                save_audit_copy = False
                save_msg_str = "NOT be"

        remove_protocols = False
        if request.args:
            args = parser.parse_args(req=request)
            remove_protocols = False if args['force'].lower() != 'true' else True

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id=study_id, api_key=user_token,
                                                         skip_load_tables=True, study_location=study_location)
        # Remove the assay from the study
        for assay in isa_study.assays:  # ToDo, check if we can delete the correct assay if the file is missing
            a_file = assay.filename
            a_file = a_file.strip().rstrip('\n')

            if assay_file_name == a_file:
                logger.info("Removing assay " + assay_file_name + " from study " + study_id)

                if remove_protocols:  # remove protocols *only* used by this assay
                    # Get all unique protocols for the study, ie. any protocol that is only used once
                    unique_protocols = get_all_unique_protocols_from_study_assays(study_id, isa_study.assays)
                    assay_type = get_assay_type_from_file_name(study_id, assay.filename)
                    tidy_header_row, tidy_data_row, protocols, assay_desc, assay_data_type, assay_mandatory_type = \
                        get_assay_headers_and_protcols(assay_type)

                    for protcol in protocols:
                        prot_name = protcol[1]
                        for uprot_name in unique_protocols:
                            if prot_name == uprot_name:
                                obj = isa_study.get_prot(prot_name)
                                if not obj:
                                    abort(404)
                                # remove object
                                isa_study.protocols.remove(obj)

                isa_study.assays.remove(assay)
                maf_name = get_maf_name_from_assay_name(a_file)
                logger.info("A copy of the previous files will %s saved", save_msg_str)
                iac.write_isa_study(isa_inv, user_token, std_path,
                                    save_investigation_copy=save_audit_copy, save_assays_copy=save_audit_copy,
                                    save_samples_copy=save_audit_copy)
                try:
                    remove_file(study_location, a_file, allways_remove=True)  # We have to remove active metadata files
                    if maf_name is not None:
                        remove_file(study_location, maf_name, allways_remove=True)
                except:
                    logger.error("Failed to remove assay file " + a_file + " from study " + study_id)

        return {"success": "The assay was removed from study " + study_id}


class StudyAssay(Resource):
    @swagger.operation(
        summary="Get Study Assay",
        notes="""Get Study Assay.""",
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
                "name": "filename",
                "description": "Assay filename",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "list_only",
                "description": "List filenames only",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True
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
        # param validation
        if study_id is None:
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('filename', help='Assay filename')
        filename = None
        parser.add_argument('list_only', help='List names only')
        list_only = True
        if request.args:
            args = parser.parse_args(req=request)
            filename = args['filename'].lower() if args['filename'] else None
            list_only = True if args['list_only'].lower() == 'true' else False

        logger.info('Getting Assay %s for %s', filename, study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False,
                                                         study_location=study_location)

        obj_list = isa_study.assays
        found = list()
        if not filename:
            found = obj_list
        else:
            assay = get_assay(obj_list, filename)
            if assay:
                found.append(assay)
        if not found:
            abort(404)
        logger.info('Found %d assays', len(found))

        sch = AssaySchema(many=True)
        if list_only:
            sch = AssaySchema(only=('filename',), many=True)
        return extended_response(data={'assays': sch.dump(found).data})

    @swagger.operation(
        summary='Add a new assay',
        notes='''Add a new assay to a study<pre><code>
{ 
 "assay": {        
    "type": "LC-MS",
    "columns": [
            {
                "name"  : "polarity",
                "value" : "positive"
            },
            {
                "name"  : "column type",
                "value" : "hilic"
            },
            {
                "name"  : "Parameter Value[Instrument]",
                "value" : "My instrument make and model"
            }
        ]
    }
}</pre></code> </p>
Accepted values for:</br>
- <b>(mandatory)</b> "type" - "LC-MS", "LC-DAD", "GC-MS", "GCxGC-MS", "GC-FID", "DI-MS", "FIA-MS", "CE-MS", "MALDI-MS", "MSImaging", "NMR"</br>
- <b>(optional)</b> "polarity" - "positive", "negative" or "alternating"</br>
- <b>(optional)</b> "column type"  - "hilic", "reverse phase" or "direct infusion"</br>
</br>
<b>Acronyms:</b>  Diode array detection (LC-DAD), Tandem MS (GCxGC-MS), Flame ionisation detector (GC-FID), 
Direct infusion (DI-MS), Flow injection analysis (FIA-MS), Capillary electrophoresis (CE-MS), 
Matrix-assisted laser desorption-ionisation imaging mass spectrometry (MALDI-MS), Nuclear magnetic resonance (NMR),
Mass spec spectrometry (MSImaging)
</p>
Other columns, like "Parameter Value[Instrument]" must be matches exactly like the header in the assay file''',
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
                "name": "assay",
                "description": 'Assay definition',
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
                "code": 409,
                "message": "Conflict. The request could not be completed due to a conflict"
                           " with the current state of study. This is usually issued to prevent duplications."
            }
        ]
    )
    def post(self, study_id):
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

        # check if we should be keeping copies of the metadata
        save_audit_copy = False
        save_msg_str = "NOT be"
        if "save_audit_copy" in request.headers and \
                request.headers["save_audit_copy"].lower() == 'true':
            save_audit_copy = True
            save_msg_str = "be"

        # body content validation
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['assay']
            assay_type = data['type']
            # platform = data['platform']
            try:
                columns = data['columns']
            except:
                columns = []  # If not provided, ignore

            if assay_type is None:
                abort(412)

        except (ValidationError, Exception):
            abort(400, 'Incorrect JSON provided')

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id=study_id, api_key=user_token,
                                                         skip_load_tables=True, study_location=study_location)

        # Also make sure the sample file is in the standard format of 's_MTBLSnnnn.txt'
        # isa_study, sample_file_name = update_correct_sample_file_name(isa_study, study_location, study_id)

        isa_inv, obi = add_ontology_to_investigation(isa_inv, 'OBI', '29', 'http://data.bioontology.org/ontologies/OBI',
                                                     'Ontology for Biomedical Investigations')

        # Add the new assay to the investigation file
        assay_file_name, assay, protocol_params, overall_technology = create_assay(assay_type, columns, study_id, obi)

        # add the assay to the study
        isa_study.assays.append(assay)

        maf_name = ""
        try:
            maf_name = get_maf_name_from_assay_name(assay_file_name)
            maf_df, annotation_file_name, new_column_counter = create_maf(overall_technology, study_location, assay_file_name, maf_name)
        except:
            logger.error('Could not create MAF for study ' + study_id + ' under assay ' + assay_file_name)

        message = update_assay_column_values(columns, assay_file_name, maf_file_name=maf_name)

        logger.info("A copy of the previous files will %s saved", save_msg_str)
        isa_study = add_new_protocols_from_assay(assay_type, protocol_params, assay_file_name, study_id, isa_study)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)

        protocol_names = ''
        for prot in protocol_params:
            protocol_names = protocol_names + prot[1] + ','

        json_assay = AssaySchema().dump(assay)

        return {"success": "The assay was added to study "+study_id,
                "protocols": protocol_names.rstrip(','),
                "filename": assay.filename,
                "maf": maf_name,
                "assay": json_assay[0]}


def get_all_unique_protocols_from_study_assays(study_id, assays):
    all_protocols = []
    unique_protocols = []
    all_names = []
    short_list = []

    try:
        for assay in assays:
            assay_type = get_assay_type_from_file_name(study_id, assay.filename)
            tidy_header_row, tidy_data_row, protocols, assay_desc, assay_data_type, assay_mandatory_type = \
                get_assay_headers_and_protcols(assay_type)
            all_protocols = all_protocols + protocols
    except:
        return []

    for protocol in all_protocols:
        all_names.append(protocol[1])

    for prot_name in all_names:
        unique_protocols.append([prot_name, all_names.count(prot_name)])

    unique_protocols = list(map(list, set(map(lambda i: tuple(i), unique_protocols))))

    for i in unique_protocols:
        if i[1] == 1:
            short_list.append(i[0])

    return short_list


def create_assay(assay_type, columns, study_id, ontology):
    profiling = 'metabolite_profiling'
    studies_path = app.config.get('STUDY_PATH')  # Root folder for all studies
    study_path = os.path.join(studies_path, study_id)  # This particular study

    polarity = ''
    column = ''
    for key_val in columns:
        if key_val['name'].lower() == 'polarity':
            polarity = key_val['value']

        if key_val['name'].lower() == 'column type':
            column = key_val['value']

    tidy_header_row, tidy_data_row, protocols, assay_desc, assay_data_types, assay_data_mandatory \
        = get_assay_headers_and_protcols(assay_type)

    assay_platform = assay_desc + ' - ' + polarity
    if column != '':
        assay_platform = assay_platform + ' - ' + column

    # this will be the final name for the copied assay template
    file_name = 'a_' + study_id.upper() + '_' + assay_type + '_' + polarity + '_' + column.replace(' ', '-').lower() \
                + '_' + profiling

    file_name = get_valid_assay_file_name(file_name, study_path)
    assay, overall_technology = get_new_assay(file_name, assay_platform, assay_type, ontology)

    file_name = os.path.join(study_path, file_name)

    try:
        file = open(file_name, 'w', encoding="utf-8")
        writer = csv.writer(file, delimiter="\t", quotechar='\"')
        writer.writerow(tidy_header_row)
        writer.writerow(tidy_data_row)
        file.close()
    except (FileNotFoundError, Exception):
        abort(500, 'Could not write the assay file')

    return file_name, assay, protocols, overall_technology


def get_valid_assay_file_name(file_name, study_path):
    # Has the filename has already been used in another assay?
    file_counter = 0
    assay_file = os.path.join(study_path, file_name + '.txt')
    file_exists = os.path.isfile(assay_file)
    while file_exists:
        file_counter += 1
        new_file = file_name + '-' + str(file_counter)
        if not os.path.isfile(os.path.join(study_path, new_file + '.txt')):
            file_name = new_file
            break

    return file_name + '.txt'


def get_new_assay(file_name, assay_platform, assay_type, ontology):
    assay = Assay(filename=file_name, technology_platform=assay_platform)

    # technologyType
    technology = OntologyAnnotation(
        term_accession='http://purl.obolibrary.org/obo/OBI_0000366',
        term='metabolite profiling',
        term_source='OBI')
    # measurementType
    measurement = assay.measurement_type
    overall_technology = ""

    if assay_type in ['NMR', 'MRImaging']:
        technology.term = 'NMR spectroscopy'
        technology.term_accession = 'http://purl.obolibrary.org/obo/OBI_0000623'
        overall_technology = "NMR"
    else:
        technology.term = 'mass spectrometry'
        technology.term_accession = 'http://purl.obolibrary.org/obo/OBI_0000470'
        overall_technology = "MS"

    # Add the termSource to the technologyType
    technology.term_source = ontology
    # Add the ontology term to the assay.technologyType
    assay.technology_type = technology
    # Add the measurementType to the assay.measurementType
    measurement.term_source = ontology
    measurement.term = "metabolite profiling"
    measurement.term_accession = "http://purl.obolibrary.org/obo/OBI_0000366"
    assay.measurement_type = measurement

    try:
        result = AssaySchema().load(assay, partial=True)
    except (ValidationError, Exception):
        abort(400)

    return assay, overall_technology


def update_assay_column_values(columns, assay_file_name, maf_file_name=None):

    # These are the real column headers from the assay file
    assay_col_type = 'Parameter Value[Column type]'
    assay_scan_pol = 'Parameter Value[Scan polarity]'
    assay_sample_name = 'Sample Name'
    maf_column_name = 'Metabolite Assignment File'

    try:
        table_df = read_tsv(assay_file_name)
    except FileNotFoundError:
        abort(400, "The file " + assay_file_name + " was not found")

    for key_val in columns:  # These are the values from the JSON passed
        column_header = key_val['name']
        cell_value = key_val['value']

        try:

            if column_header.lower() == 'polarity':
                column_index = table_df.columns.get_loc(assay_scan_pol)
            elif column_header.lower() == 'column type':
                column_index = table_df.columns.get_loc(assay_col_type)
            else:
                column_index = table_df.columns.get_loc(column_header)

            update_cell(table_df, column_index, cell_value)
        except:
            logger.warning('Could not find %s in the assay file', column_header)

    # Also update the default sample name, this will trigger validation errors
    update_cell(table_df, table_df.columns.get_loc(assay_sample_name), '')

    # Replace the default MAF file_name with the correct one
    if maf_file_name is not None:
        update_cell(table_df, table_df.columns.get_loc(maf_column_name), maf_file_name)

    # Write the updated row back in the file
    message = write_tsv(table_df, assay_file_name)

    return message


def update_cell(table_df, column_index, cell_value):
    try:
        for row_val in range(table_df.shape[0]):
            table_df.iloc[int(0), int(column_index)] = cell_value
    except ValueError:
        abort(417, "Unable to find the required 'value', 'row' and 'column' values")


class AssayProcesses(Resource):

    @swagger.operation(
        summary="Get Assay Process Sequence",
        notes="""Get Assay Process Sequence.
                  <br>
                  Use assay filename, process or protocol name to filter results.""",
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
                "name": "assay_filename",
                "description": "Assay filename",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "process_name",
                "description": "Process name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "protocol_name",
                "description": "Protocol name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "list_only",
                "description": "List names only",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True
            },
            {
                "name": "use_default_values",
                "description": "Provide default values when empty",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": False,
                "default": True
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
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('assay_filename', help='Assay filename')
        assay_filename = None
        parser.add_argument('process_name', help='Assay Processes name')
        process_name = None
        parser.add_argument('protocol_name', help='Protocol name')
        protocol_name = None
        parser.add_argument('list_only', help='List names only')
        list_only = True
        parser.add_argument('use_default_values', help='Provide default values when empty')
        use_default_values = False
        if request.args:
            args = parser.parse_args(req=request)
            assay_filename = args['assay_filename'].lower() if args['assay_filename'] else None
            process_name = args['process_name'].lower() if args['process_name'] else None
            protocol_name = args['protocol_name'].lower() if args['protocol_name'] else None
            list_only = True if args['list_only'].lower() == 'true' else False
            use_default_values = True if args['use_default_values'].lower() == 'true' else False

        logger.info('Getting Processes for Assay %s in %s', assay_filename, study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=False,
                                                         study_location=study_location)

        assay_list = list()
        warns = []
        if not assay_filename:
            assay_list = isa_study.assays
            warns.append({'message': 'No Assay filename provided, so merging ProcessSequence for all assays.'})
        else:
            assay = get_assay(isa_study.assays, assay_filename)
            if assay:
                assay_list.append(assay)
        if not assay_list:
            abort(404)

        found = list()
        for assay in assay_list:
            process_list = assay.process_sequence
            if not process_name and not protocol_name:
                found = process_list
            else:
                for index, proto in enumerate(process_list):
                    if proto.name.lower() == process_name or \
                            proto.executes_protocol.name.lower() == protocol_name:
                        found.append(proto)
            if not found:
                abort(404)
            logger.info('Found %d protocols', len(assay_list))

            # use default values
            if use_default_values:
                set_default_proc_name(process_list, warns)

                proc_list = get_first_process(process_list)
                set_default_output(assay, proc_list, warns)

        sch = ProcessSchema(many=True)
        if list_only:
            sch = ProcessSchema(only=('name', 'executes_protocol.name',
                                      'prev_process.executes_protocol.name',
                                      'next_process.executes_protocol.name'), many=True)
        return extended_response(data={'processSequence': sch.dump(found).data},
                                 warns=warns)


def set_default_output(isa_assay, proc_list, warns):
    for i, proc in enumerate(proc_list):
        # check Extraction outputs
        if proc.executes_protocol.name == 'Extraction':
            if not proc.outputs:
                # take inputs from next process
                if proc.next_process.inputs:
                    proc.outputs = proc.next_process.inputs
                    warns.append({'message': 'Using ' + (proc.next_process.name if proc.next_process.name else proc.next_process.executes_protocol.name) + ' inputs' + ' as outputs for ' + proc.name})
                # create from self inputs
                elif proc.inputs:
                    # create output
                    for input in proc.inputs:
                        if isinstance(input, Sample):
                            extract = Extract(name=input.name + '_' + 'Extract',
                                              comments=[{'name': 'Inferred',
                                                         'value': 'Value was missing in ISA-Tab, '
                                                                  'so building from Sample name.'}])
                            proc.outputs.append(extract)
                            isa_assay.other_material.append(extract)
                            warns.append({'message': 'Created new Extract ' + extract.name})


def set_default_proc_name(obj_list, warns):
    for i, proc in enumerate(obj_list):
        if not proc.name:
            proc.name = 'Process' + '_' + proc.executes_protocol.name
            warns.append({'message': 'Added name to Process ' + proc.name})


def get_first_process(proc_list):
    procs = list()
    for i, proc in enumerate(proc_list):
        if not proc.prev_process:
            procs.append(proc)
    return procs


class AssaySamples(Resource):

    @swagger.operation(
        summary="Get Assay Samples",
        notes="""Get Assay Samples.
                  <br>
                  Use assay filename or sample name to filter results.""",
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
                "name": "assay_filename",
                "description": "Assay filename",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "name",
                "description": "Assay Sample name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "list_only",
                "description": "List names only",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True
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
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('assay_filename', help='Assay filename')
        assay_filename = None
        parser.add_argument('name', help='Assay Sample name')
        sample_name = None
        parser.add_argument('list_only', help='List names only')
        list_only = True
        if request.args:
            args = parser.parse_args(req=request)
            assay_filename = args['assay_filename'].lower() if args['assay_filename'] else None
            sample_name = args['name'].lower() if args['name'] else None
            list_only = True if args['list_only'].lower() == 'true' else False

        logger.info('Getting Samples for Assay %s in %s', assay_filename, study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=False,
                                                         study_location=study_location)

        assay_list = list()
        warns = []
        if not assay_filename:
            assay_list = isa_study.assays
            warns.append({'message': 'No Assay filename provided, so merging Samples for all assays.'})
        else:
            assay = get_assay(isa_study.assays, assay_filename)
            if assay:
                assay_list.append(assay)
        if not assay_list:
            abort(404)

        found = list()
        for assay in assay_list:
            obj_list = assay.samples
            if not sample_name:
                found = obj_list
            else:
                for index, obj in enumerate(obj_list):
                    if obj.name.lower() == sample_name:
                        found.append(obj)
            if not found:
                abort(404)
            logger.info('Found %d Materials', len(assay_list))

        sch = SampleSchema(many=True)
        if list_only:
            sch = SampleSchema(only=('name',), many=True)
        return extended_response(data={'samples': sch.dump(found).data}, warns=warns)

    @swagger.operation(
        summary='Update Assay Samples',
        notes="""Update a list of Assay Samples. Only existing Samples will be updated, unknown will be ignored. 
        To change name, only one sample can be processed at a time.""",
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
                "name": "assay_filename",
                "description": "Assay filename",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "name",
                "description": "Assay Sample name. Leave empty if updating more than one sample.",
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
            },
            {
                "name": "samples",
                "description": 'Assay Sample list in ISA-JSON format.',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "list_only",
                "description": "List names only",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True
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
                }
            ]
        )
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('assay_filename', help='Assay filename')
        assay_filename = None
        parser.add_argument('name', help='Assay Sample name')
        sample_name = None
        parser.add_argument('list_only', help='List names only')
        list_only = True
        if request.args:
            args = parser.parse_args(req=request)
            assay_filename = args['assay_filename'].lower() if args['assay_filename'] else None
            sample_name = args['name'].lower() if args['name'] else None
            list_only = True if args['list_only'].lower() == 'true' else False
        if not assay_filename:
            logger.warning("Missing Assay filename.")
            abort(400)

        # header content validation
        save_audit_copy = False
        save_msg_str = "NOT be"
        if "save_audit_copy" in request.headers and \
                request.headers["save_audit_copy"].lower() == 'true':
            save_audit_copy = True
            save_msg_str = "be"

        # body content validation
        sample_list = list()
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['samples']
            # if partial=True missing fields will be ignored
            result = SampleSchema().load(data, many=True, partial=False)
            sample_list = result.data
            if len(sample_list) == 0:
                logger.warning("No valid data provided.")
                abort(400)
        except (ValidationError, Exception) as err:
            logger.warning("Bad format JSON request.", err)
            abort(400, err)

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=False,
                                                         study_location=study_location)

        logger.info('Updating Samples for Assay %s in %s', assay_filename, study_id)
        assay = get_assay(isa_study.assays, assay_filename)
        if not assay:
            abort(404)

        logger.info('Updating Study Samples details for %s in %s,', assay_filename, study_id)
        updated_samples = list()
        if sample_name:
            if len(sample_list) > 1:
                logger.warning("Requesting name update for more than one sample")
                abort(400, "Requesting name update for more than one sample")
            sample = sample_list[0]
            if self.update_sample(isa_study, sample_name, sample):
                updated_samples.append(sample)
        else:
            for i, sample in enumerate(sample_list):
                if self.update_sample(isa_study, sample.name.lower(), sample):
                    updated_samples.append(sample)

        # check if all samples were updated
        warns = ''
        if len(updated_samples) != len(sample_list):
            warns = 'Some of the samples were not updated. ' \
                    'Updated ' + str(len(updated_samples)) + ' out of ' + str(len(sample_list))
            logger.warning(warns)

        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path,
                            save_investigation_copy=save_audit_copy,
                            save_samples_copy=save_audit_copy,
                            save_assays_copy=save_audit_copy)

        sch = SampleSchema(many=True)
        if list_only:
            sch = SampleSchema(only=('name',), many=True)
        return extended_response(data={'samples': sch.dump(updated_samples).data}, warns=warns)

    def update_sample(self, isa_study, sample_name, new_sample):

        for i, sample in enumerate(isa_study.samples):
            if sample.name.lower() == sample_name:
                isa_study.samples[i].name = new_sample.name
                isa_study.samples[i].characteristics = new_sample.characteristics
                isa_study.samples[i].derives_from = new_sample.derives_from
                isa_study.samples[i].factor_values = new_sample.factor_values
                isa_study.samples[i].comments = new_sample.comments

        for i, process in enumerate(isa_study.process_sequence):
            for ii, sample in enumerate(process.outputs):
                if isinstance(sample, Sample) and sample.name.lower() == sample_name:
                    isa_study.process_sequence[i].outputs[ii].name = new_sample.name
                    isa_study.process_sequence[i].outputs[ii].characteristics = new_sample.characteristics
                    isa_study.process_sequence[i].outputs[ii].factor_values = new_sample.factor_values
                    isa_study.process_sequence[i].outputs[ii].derives_from = new_sample.derives_from
                    isa_study.process_sequence[i].outputs[ii].comments = new_sample.comments

        for isa_assay in isa_study.assays:
            for i, sample in enumerate(isa_assay.samples):
                if sample.name.lower() == sample_name:
                    isa_assay.samples[i].name = new_sample.name
                    isa_assay.samples[i].characteristics = new_sample.characteristics
                    isa_assay.samples[i].derives_from = new_sample.derives_from
                    isa_assay.samples[i].factor_values = new_sample.factor_values
                    isa_assay.samples[i].comments = new_sample.comments

        for i, process in enumerate(isa_assay.process_sequence):
            for ii, sample in enumerate(process.inputs):
                if isinstance(sample, Sample) and sample.name.lower() == sample_name:
                    isa_assay.process_sequence[i].inputs[ii].name = new_sample.name
                    isa_assay.process_sequence[i].inputs[ii].characteristics = new_sample.characteristics
                    isa_assay.process_sequence[i].inputs[ii].factor_values = new_sample.factor_values
                    isa_assay.process_sequence[i].inputs[ii].derives_from = new_sample.derives_from
                    isa_assay.process_sequence[i].inputs[ii].comments = new_sample.comments

                    logger.info('Updated sample: %s', new_sample.name)
                    return True
        return False


class AssayOtherMaterials(Resource):

    @swagger.operation(
        summary="Get Assay Other Materials",
        notes="""Get Assay Other Materials.
                  <br>
                  Use assay filename or material name to filter results.""",
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
                "name": "assay_filename",
                "description": "Assay filename",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "name",
                "description": "Assay Material name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "list_only",
                "description": "List names only",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True
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
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('assay_filename', help='Assay filename')
        assay_filename = None
        parser.add_argument('name', help='Assay Other Materials name')
        obj_name = None
        parser.add_argument('list_only', help='List names only')
        list_only = True
        if request.args:
            args = parser.parse_args(req=request)
            assay_filename = args['assay_filename'].lower() if args['assay_filename'] else None
            obj_name = args['name'].lower() if args['name'] else None
            list_only = True if args['list_only'].lower() == 'true' else False

        logger.info('Getting Other Materials for Assay %s in %s', assay_filename, study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=False,
                                                         study_location=study_location)

        assay_list = list()
        warns = []
        if not assay_filename:
            assay_list = isa_study.assays
            warns.append({'message': 'No Assay filename provided, so merging Other Materials for all assays.'})
        else:
            assay = get_assay(isa_study.assays, assay_filename)
            if assay:
                assay_list.append(assay)
        if not assay_list:
            abort(404)

        found = list()
        for assay in assay_list:
            obj_list = assay.other_material
            if not obj_name:
                found = obj_list
            else:
                for index, obj in enumerate(obj_list):
                    if obj.name.lower() == obj_name:
                        found.append(obj)
            if not found:
                abort(404)
            logger.info('Found %d Materials', len(assay_list))

        sch = OtherMaterialSchema(many=True)
        if list_only:
            sch = OtherMaterialSchema(only=('name',), many=True)
        return extended_response(data={'otherMaterials': sch.dump(found).data}, warns=warns)


class AssayDataFiles(Resource):

    @swagger.operation(
        summary="Get Assay Data File",
        notes="""Get Assay Data File.
                  <br>
                  Use filename as query parameter for specific searching.""",
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
                "name": "assay_filename",
                "description": "Assay filename",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "data_filename",
                "description": "Data File name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "list_only",
                "description": "List names only",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True
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
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('assay_filename', help='Assay filename')
        assay_filename = None
        parser.add_argument('data_filename', help='Assay Data File name')
        data_filename = None
        parser.add_argument('list_only', help='List names only')
        list_only = True
        if request.args:
            args = parser.parse_args(req=request)
            assay_filename = args['assay_filename'].lower() if args['assay_filename'] else None
            data_filename = args['data_filename'].lower() if args['data_filename'] else None
            list_only = True if args['list_only'].lower() == 'true' else False

        logger.info('Getting Data Files for Assay %s in %s', assay_filename, study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=False,
                                                         study_location=study_location)

        assay_list = list()
        warns = []
        if not assay_filename:
            assay_list = isa_study.assays
            warns.append({'message': 'No Assay filename provided, so merging Data files for all assays.'})
        else:
            assay = get_assay(isa_study.assays, assay_filename)
            if assay:
                assay_list.append(assay)
        if not assay_list:
            abort(404)

        found = list()
        for assay in assay_list:
            datafile_list = assay.data_files
            if not data_filename:
                found = datafile_list
            else:
                for index, obj in enumerate(datafile_list):
                    if obj.filename.lower() == data_filename :
                        found.append(obj)
            if not found:
                abort(404)
            logger.info('Found %d data files', len(assay_list))

        sch = DataFileSchema(many=True)
        if list_only:
            sch = DataFileSchema(only=('filename',), many=True)
        return extended_response(data={'dataFiles': sch.dump(found).data}, warns=warns)
