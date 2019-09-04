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

import json
import traceback
import requests
from app.ws.study_files import get_all_files_from_filesystem
from flask import request, abort
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import *
from app.ws.isaApiClient import IsaApiClient
from app.ws.db_connection import override_validations

logger = logging.getLogger('wslog')
wsc = WsClient()
iac = IsaApiClient()

incorrect_species = \
    "cat, dog, mouse, horse, flower, man, fish, leave, root, mice, steam, bacteria, value, chemical, food, matix, " \
    "mus, rat, blood, urine, plasma, hair, fur, skin, saliva, fly, unknown"

last_name_black_list = ['last name', 'asdf', 'name', 'unknown']
first_name_black_list = ['first name', 'asdf', 'name', 'unknown']

correct_maf_order = [{0: "database_identifier"}, {1: "chemical_formula"}, {2: "smiles"},
                     {3: "inchi"}, {4: "metabolite_identification"}]

warning = "warning"
error = "error"
success = "success"
info = "info"


def add_msg(validations, section, message, status, meta_file="", value="", descr="", val_sequence=0, log_category=error):
    if log_category == status or log_category == 'all':
        validations.append({"message": message, "section": section, "val_sequence": str(val_sequence), "status": status,
                            "metadata_file": meta_file, "value": value, "description": descr})


def get_basic_validation_rules(validation_schema, part):
    if validation_schema:
        study_val = validation_schema['study']
        val = study_val[part]
        rules = val['rules'][0]
    return rules, val['description']


def get_complex_validation_rules(validation_schema, part, sub_part, sub_set):
    rules = None
    sets = None
    if validation_schema:
        study_val = validation_schema['study']
        val = study_val[part]
        sub = val[sub_part]
        sets = sub[sub_set]
        rules = sets['rules'][0]
    return rules, sets['description']


def get_protocol_assay_rules(validation_schema, protocol_part):
    val_rule = None
    if validation_schema and protocol_part.lower() != 'sample name':
        study_val = validation_schema['study']
        val = study_val['protocols']
        sub = val['default']
        for column in sub:
            column_title = column['title']
            if column_title.lower() != 'sample collection':
                rules = column['columns']
                for rule in rules:
                    if rule == protocol_part:
                        return rules[protocol_part]
    return val_rule


def extract_details(rule):
    try:
        try:
            val = int(rule['value'])
        except:
            val = rule['value']
        val_error = rule['error']
        val_condition = rule['condition']
        val_type = rule['type']
    except:
        return int(0), "n/a", "n/a", "n/a"
    return val, val_error, val_condition, val_type


def return_validations(section, validations, override_list=[]):
    # Add the validation sequence
    for val in validations:
        # idx += 1  # Set the sequence to 1, as this is the section we will override
        val_sequence = section + '_' + val['val_sequence']
        val["val_sequence"] = val_sequence
        val["val_override"] = 'false'
        val["val_message"] = ''
        if len(override_list) > 0:
            try:
                for db_val in override_list:  # These are from the database, ie. already over-ridden
                    val_step = db_val.split(':')[0]
                    val_msg = db_val.split(':')[1]
                    if val_sequence == val_step:
                        val_status = val['status']
                        val["val_override"] = 'true'
                        val["val_message"] = val_msg
                        if val_status == warning or val_status == error or val_status == info:
                            val["status"] = success
                        elif val_status == success:
                            val["status"] = error
            except:
                logger.error('Could not read the validation override list, is the required ":" there?')

    error_found = False
    warning_found = False
    validates = True
    amber_warning = False

    # What is the overall validation status now?
    for val in validations:
        status = val["status"]
        if status == error:
            error_found = True
        elif status == warning:
            warning_found = True

    if error_found:
        validates = False
        ret_list = {"section": section, "details": validations, "message": "Validation failed",
                    "status": error}
    elif warning_found:
        amber_warning = True
        ret_list = {"section": section, "details": validations,
                    "message": "Some optional information is missing for your study",
                    "status": warning}
    else:
        ret_list = {"section": section, "details": validations, "message": "Successful validation",
                    "status": success}

    return validates, amber_warning, ret_list


def remove_nonprintable(text):
    import string
    # Get the difference of all ASCII characters from the set of printable characters
    nonprintable = set([chr(i) for i in range(128)]).difference(string.printable)
    # Use translate to remove all non-printable characters
    return text.translate({ord(character): None for character in nonprintable})


def is_empty_file(full_file_name):
    file_stats = os.stat(full_file_name)
    file_size = file_stats.st_size
    empty_file = file_size == 0
    return empty_file


def get_sample_names(isa_samples):
    all_samples = ""
    for sample in isa_samples.samples:
        all_samples = all_samples + sample + ','
    return all_samples


def check_file(file_name_and_column, study_location, file_name_list, assay_file_list=None):
    file_name = file_name_and_column.split('|')[0]
    column_name = file_name_and_column.split('|')[1]

    fname, ext = os.path.splitext(file_name)
    fname = fname.lower()
    ext = ext.lower()

    fid_file = 'Free Induction Decay Data File'
    raw_file = 'Raw Spectral Data File'
    derived_file = 'Derived Spectral Data File'

    if os.path.isdir(os.path.join(study_location, file_name)) and ext not in ('.raw', '.d'):
        return False, 'folder', file_name + " is a sub-folder, please reference a file"

    if "fid" not in file_name and file_name not in file_name_list:  # Files may be referenced in sub-folders
        return False, ' - unknown - ', "File " + file_name + " does not exist"

    file_type, status, folder = map_file_type(file_name, study_location, assay_file_list=assay_file_list)
    if is_empty_file(os.path.join(study_location, file_name)):
        return False, file_type, "File " + file_name + " is empty"

    if file_type == 'metadata_maf' and column_name == 'Metabolite Assignment File':
        if file_name.startswith('m_') and file_name.endswith('_v2_maf.tsv'):
            return True, file_type, 'Correct file ' + file_name + ' for column ' + column_name
        else:
            return False, file_type,  "The " + column_name + " must start with 'm_' and end in '_v2_maf.tsv'"

    if (file_type == 'raw' or file_type == 'compressed') and column_name == raw_file:
        return True, file_type, 'Correct file ' + file_name + ' for column ' + column_name
    elif (file_type == 'derived' or file_type == 'raw' or file_type == 'compressed') \
            and (column_name == derived_file or column_name == raw_file):
        return True, file_type, 'Correct file ' + file_name + ' for column ' + column_name
    elif file_type == 'spreadsheet' and column_name == derived_file:
        return True, file_type, 'Correct file ' + file_name + ' for column ' + column_name
    elif file_type != 'derived' and column_name == derived_file:
        return False, file_type, 'Incorrect file ' + file_name + ' or file type for column ' + column_name
    elif file_type == 'compressed' and column_name == fid_file:
        return True, file_type, 'Correct file ' + file_name + ' for column ' + column_name
    elif file_type == 'folder' and column_name == fid_file:
        return True, file_type, 'Correct file ' + file_name + ' for column ' + column_name
    elif file_type == 'compressed' and column_name == 'Acquisition Parameter Data File':
        return True, file_type, 'Correct file ' + file_name + ' for column ' + column_name
    elif file_type == 'fid' and column_name == fid_file:
        return True, file_type, 'Correct file ' + file_name + ' for column ' + column_name
    elif file_type != 'raw' and column_name == raw_file:
        return False, file_type, 'Incorrect file ' + file_name + ' or file type for column ' + column_name

    return status, file_type, 'n/a'


def maf_messages(header, pos, incorrect_pos, maf_header, incorrect_message, validations, file_name):
    try:
        if maf_header[header] != pos:
            incorrect_message = incorrect_message + header + " is not the correct position. "
            incorrect_pos = True
    except Exception as e:
        incorrect_message = incorrect_message + " Column '" + header + "' is missing from " + file_name + ". "
        incorrect_pos = True

    return incorrect_pos, incorrect_message, validations


def validate_maf(validations, file_name, all_assay_names, study_location, study_id,
                 sample_name_list, is_ms=False, log_category=error):
    val_section = "maf"
    maf_name = os.path.join(study_location, file_name)
    maf_df = pd.DataFrame()

    if len(file_name) == 0:
        add_msg(validations, val_section, "Please add a Metabolite Annotation File name '" + file_name + "'",
                error, val_sequence=10, log_category=log_category)

    try:
        maf_df = read_tsv(maf_name)
    except:
        add_msg(validations, val_section, "Could not find or read Metabolite Annotation File '" + file_name + "'",
                error, val_sequence=11, log_category=log_category)

    incorrect_pos = False
    incorrect_message = ""
    maf_order = correct_maf_order.copy()
    if is_ms:
        maf_order.append({5: "mass_to_charge"})
    else:
        maf_order.append({5: "chemical_shift"})

    if not maf_df.empty:
        maf_header = get_table_header(maf_df, study_id, maf_name)

        for idx, col in enumerate(maf_order):
            incorrect_pos, incorrect_message, validations = \
                maf_messages(col[idx], idx, incorrect_pos, maf_header, incorrect_message, validations, file_name)

        if incorrect_pos:
            add_msg(validations, val_section, incorrect_message, error, val_sequence=3, log_category=log_category)
        else:
            add_msg(validations, val_section,
                    "Columns 'database_identifier', 'chemical_formula', 'smiles', 'inchi' and "
                    "'metabolite_identification' found in the correct column position in '" + file_name + "'",
                    success, val_sequence=4, log_category=log_category)

        try:
            if is_ms and maf_header['mass_to_charge']:
                check_maf_rows(validations, val_section, maf_df, 'mass_to_charge', is_ms=is_ms, log_category=log_category)
        except:
            logger.info("No mass_to_charge column found in the MS MAF")

        # NMR/MS Assay Names OR Sample Names are added to the sheet
        if all_assay_names:
            for assay_name in all_assay_names:
                try:
                    maf_header[assay_name]
                    add_msg(validations, val_section, "MS/NMR Assay Name '" + assay_name + "' found in the MAF",
                            success, val_sequence=5, log_category=log_category)
                    check_maf_rows(validations, val_section, maf_df, assay_name, is_ms=is_ms, log_category=log_category)
                except KeyError as e:
                    add_msg(validations, val_section, "MS/NMR Assay Name '" + assay_name + "' not found in the MAF",
                            warning, val_sequence=6, log_category=log_category)

        if not all_assay_names and sample_name_list:
            for sample_name in sample_name_list:
                try:
                    maf_header[sample_name]
                    add_msg(validations, val_section, "Sample Name '" + str(sample_name) + "' found in the MAF",
                            success, val_sequence=7, log_category=log_category)
                    check_maf_rows(validations, val_section, maf_df, sample_name, is_ms=is_ms, log_category=log_category)
                except:
                    add_msg(validations, val_section, "Sample Name '" + str(sample_name) + "' not found in the MAF",
                            warning, val_sequence=8, log_category=log_category)
    else:
        add_msg(validations, val_section, "MAF '" + file_name + "' is empty. Please add metabolite annotation details",
                error, val_sequence=8, log_category=log_category)


def check_maf_rows(validations, val_section, maf_df, column_name, is_ms=False, log_category=error):
    all_rows = maf_df.shape[0]
    col_rows = 0
    # Are all relevant rows filled in?
    for row in maf_df[column_name]:
        if row:
            col_rows += 1

    if col_rows == all_rows:
        add_msg(validations, val_section, "All values for '" + column_name + "' found in the MAF",
                success, val_sequence=9, log_category=log_category)
    else:
        # For MS we should have m/z values, for NMR the chemical shift is equally important.
        if (is_ms and column_name == 'mass_to_charge') or (not is_ms and column_name == 'chemical_shift'):
            add_msg(validations, val_section, "Missing values for '" + column_name + "' in the MAF. " +
                    str(col_rows) + " rows found, but there should be " + str(all_rows),
                    warning, val_sequence=10, log_category=log_category)
        else:
            add_msg(validations, val_section, "Missing values for sample '" + column_name + "' in the MAF. " +
                    str(col_rows) + " rows found, but there should be " + str(all_rows),
                    info, val_sequence=11, log_category=log_category)


class Validation(Resource):
    @swagger.operation(
        summary="Validate study",
        notes='''Validating the overall study. 
        This method will validate the study metadata and check some of the files study folder''',
        parameters=[
            {
                "name": "study_id",
                "description": "Study to validate",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "section",
                "description": "Specify which validations to run, default is all: "
                               "isa-tab, publication, protocols, people, samples, assays, maf, files",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "level",
                "description": "Specify which success-errors levels to report, default is all: "
                               "error, warning, info, success",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
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
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication. "
                           "Please provide a study id and a valid user token"
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token"
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self, study_id):

        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None or study_id is None:
            abort(401)

        study_id = study_id.upper()

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('section', help="Validation section", location="args")
        parser.add_argument('level', help="Validation message levels", location="args")
        args = parser.parse_args()
        section = args['section']
        log_category = args['level']

        if section is None:
            section = 'all'

        if log_category is None:
            log_category = 'all'

        return validate_study(study_id, study_location, user_token, obfuscation_code, section, log_category)


def validate_study(study_id, study_location, user_token, obfuscation_code, validation_section='all', log_category='all'):
    all_validations = []
    validation_schema = None
    error_found = False
    warning_found = False
    validation_section = validation_section.lower()

    try:
        validation_schema_file = app.config.get('VALIDATIONS_FILE')

        if validation_schema_file.startswith('http'):
            response = requests.get(validation_schema_file)
            validation_schema = json.loads(response.content)
        else:
            with open(validation_schema_file, 'r', encoding='utf-8') as json_file:
                validation_schema = json.load(json_file)

    except Exception as e:
        all_validations.append({"info": "Could not find the validation schema, only basic validation will take place",
                                "status": success})
        logger.error(str(e))

    override_list = []
    try:
        query_list = override_validations(study_id, 'query')
        if query_list and query_list[0]:
            for val in query_list[0].split('|'):
                override_list.append(val)
    except Exception as e:
        logger.error('Can not query overridden validations from the database')

    # Validate basic ISA-Tab structure
    isa_study, isa_inv, isa_samples, std_path, status, amber_warning, isa_validation, inv_file, s_file, assay_files = \
        validate_basic_isa_tab(study_id, user_token, study_location, override_list, log_category=log_category)
    all_validations.append(isa_validation)
    if not status:
        error_found = True
    if amber_warning:
        warning_found = True

    # We can now run the rest of the validation checks

    # Validate publications reported on the study
    val_section = "publication"
    if isa_study and validation_section == 'all' or val_section in validation_section:
        status, amber_warning, pub_validation = validate_publication(
            isa_study, validation_schema, inv_file, override_list, val_section, log_category=log_category)
        all_validations.append(pub_validation)

    if not status:
        error_found = True
    if amber_warning:
        warning_found = True

    # Validate detailed metadata in ISA-Tab structure
    val_section = "isa-tab"
    if validation_section == 'all' or val_section in validation_section:
        status, amber_warning, isa_meta_validation = validate_isa_tab_metadata(
            isa_inv, isa_study, validation_schema, inv_file, override_list, val_section, log_category=log_category)
        all_validations.append(isa_meta_validation)

    if not status:
        error_found = True
    if amber_warning:
        warning_found = True

    # Validate Person (authors)
    val_section = "person"
    if isa_study and validation_section == 'all' or val_section in validation_section:
        status, amber_warning, isa_person_validation = validate_contacts(
            isa_study, validation_schema, inv_file, override_list, val_section, log_category=log_category)
        all_validations.append(isa_person_validation)

    if not status:
        error_found = True
    if amber_warning:
        warning_found = True

    # Validate Protocols
    val_section = "protocols"
    if isa_study and validation_section == 'all' or val_section in validation_section:
        status, amber_warning, isa_protocol_validation = validate_protocols(
            isa_study, validation_schema, inv_file, override_list, val_section, log_category=log_category)
        all_validations.append(isa_protocol_validation)

    if not status:
        error_found = True
    if amber_warning:
        warning_found = True

    # Validate Samples
    val_section = "samples"
    sample_name_list = []
    if isa_study and validation_section == 'all' or val_section in validation_section:
        status, amber_warning, isa_sample_validation = \
            validate_samples(isa_study, isa_samples, validation_schema, s_file, override_list,
                             sample_name_list, val_section, log_category=log_category)
        all_validations.append(isa_sample_validation)

    if not status:
        error_found = True
    if amber_warning:
        warning_found = True

    # Validate files
    val_section = "files"
    file_name_list = []
    if isa_study and validation_section == 'all' or val_section in validation_section:
        status, amber_warning, files_validation = validate_files(
            study_id, study_location, obfuscation_code, override_list,
            file_name_list, val_section, log_category=log_category)
        all_validations.append(files_validation)

    if not status:
        error_found = True
    if amber_warning:
        warning_found = True

    # Validate assays
    val_section = "assays"
    if isa_study and validation_section == 'all' or val_section in validation_section or 'maf' in validation_section:
        status, amber_warning, assay_validation = \
            validate_assays(isa_study, study_location, validation_schema, override_list, sample_name_list,
                            file_name_list, val_section, log_category=log_category)
        all_validations.append(assay_validation)

    if not status:
        error_found = True
    if amber_warning:
        warning_found = True

    if error_found:
        return {"validation": {"status": error, "validations": all_validations}}

    if warning_found:
        return {"validation": {"status": warning, "validations": all_validations}}

    return {"validation": {"status": success, "validations": all_validations}}


def get_assay_column_validations(validation_schema, a_header):
    validation_schema = get_protocol_assay_rules(validation_schema, a_header)
    validate_column = False
    required_column = False
    check_schema = True
    val_descr = None

    if a_header.lower() == 'sample name' \
            or a_header.lower() == 'parameter value[scan polarity]' \
            or a_header.lower() == 'parameter value[scan m/z range]' \
            or a_header.lower() == 'parameter value[instrument]':
        validate_column = True
        required_column = True
        check_schema = False
    elif a_header.lower() == 'metabolite assignment file':
        validate_column = True
        required_column = False
        check_schema = False
    elif ' assay name' in a_header.lower():  # NMR and MS assay names are not always present, maybe change?
        validate_column = True
        required_column = False
        check_schema = False
    elif ' data file' in a_header.lower():  # Files are checked separately
        validate_column = True
        required_column = False
        check_schema = False

    if validation_schema and check_schema:
        validate_column = validation_schema['is-hidden']
        if validate_column == 'false':
            validate_column = False
        else:
            validate_column = True
        required_column = validation_schema['is-required']
        if required_column == 'false':
            required_column = False
        else:
            required_column = True
        val_descr = validation_schema['description']

    return validate_column, required_column, val_descr


def check_assay_columns(a_header, all_samples, row, validations, val_section, assay, unique_file_names,
                        all_assay_names, sample_name_list, log_category=error):
    # Correct sample names?
    if a_header.lower() == 'sample name':
        all_samples.append(row)
        if row in sample_name_list:
            add_msg(validations, val_section, "Sample name '" + row + "' found in sample sheet",
                    success, assay.filename, val_sequence=7, log_category=log_category)
        else:
            if len(row) == 0:
                add_msg(validations, val_section, "Sample name '" + row + "' can not be empty",
                        error, meta_file=assay.filename, descr="Please add a valid sample name",
                        val_sequence=8, log_category=log_category)
            else:
                add_msg(validations, val_section, "Sample name '" + row + "' not found in sample sheet",
                        error, meta_file=assay.filename,
                        descr="Please create the sample in the sample sheet first",
                        val_sequence=9, log_category=log_category)
    elif a_header.endswith(' File'):  # files exist?
        file_and_column = row + '|' + a_header
        if file_and_column not in unique_file_names:
            if row != "":  # Do not add a section if a column does not list files
                unique_file_names.append(file_and_column)
    elif a_header.endswith(' Assay Name'):  # MS or NMR assay names are used in the assay
        row = str(row)
        if row not in all_assay_names:
            if len(row) >= 1:
                all_assay_names.append(row)

    return all_samples, all_assay_names, validations, unique_file_names


def check_all_file_rows(assays, assay_df, validations, val_section, filename, all_rows, log_category=error):
    all_file_columns = []
    missing_all_rows = []
    all_assay_raw_files = []
    for assay_header in assays:
        assay_header = str(assay_header)
        if assay_header.endswith(' Data File'):
            all_file_columns.append(assay_header)
            empty_rows = (assay_df[assay_header].values == '').sum()
            if empty_rows == all_rows:
                missing_all_rows.append(assay_header)

    if 'Raw Spectral Data File' in missing_all_rows:
        if 'Derived Spectral Data File' in missing_all_rows:
            # OK, all raw/derived files are missing, no point in looking at these anymore
            all_file_columns.remove('Raw Spectral Data File')
            all_file_columns.remove('Derived Spectral Data File')
            missing_all_rows.remove('Raw Spectral Data File')
            missing_all_rows.remove('Derived Spectral Data File')
            add_msg(validations, val_section,
                    "All Raw and Derived Spectral Data Files are missing from assay",
                    error, filename, val_sequence=7.6, log_category=log_category)

    if all_file_columns:
        short_df = assay_df[assay_df.columns.intersection(all_file_columns)]
        for idx, row in short_df.iterrows():
            row_idx = str(idx + 1)
            raw_found = False
            raw_tested = False
            derived_found = False
            derived_tested = False

            for header, value in row.iteritems():
                if header == 'Raw Spectral Data File':
                    raw_tested = True
                    if value:
                        all_assay_raw_files.append(value)
                        raw_found = True
                elif header == 'Derived Spectral Data File':
                    derived_tested = True
                    if value:
                        all_assay_raw_files.append(value)
                        derived_found = True
                else:
                    if value:
                        add_msg(validations, val_section, header + " was referenced in assay row " + row_idx,
                                success, filename, val_sequence=7.5, log_category=log_category)
                    else:
                        val_type = error
                        if 'Acquisition Parameter Data File' in header or 'Free Induction Decay Data File' in header:
                            val_type = warning

                        add_msg(validations, val_section, header + " was not referenced in assay row " + row_idx,
                                val_type, filename, val_sequence=7.5, log_category=log_category)

            if derived_tested and raw_tested:
                if not raw_found and not derived_found:
                    add_msg(validations, val_section,
                            "Both Raw and Derived Spectral Data Files are missing from assay row " + row_idx,
                            error, filename, val_sequence=7.1, log_category=log_category)
                elif raw_found:
                    add_msg(validations, val_section,
                            "Raw Spectral Data File is referenced in assay row " + row_idx,
                            success, filename, value=value,  val_sequence=7.2, log_category=log_category)
                elif derived_found:
                    add_msg(validations, val_section,
                            "Derived Spectral Data File is referenced in assay row " + row_idx,
                            success, filename, value=value, val_sequence=7.3, log_category=log_category)

    return validations, all_assay_raw_files


def validate_assays(isa_study, study_location, validation_schema, override_list, sample_name_list,
                    file_name_list, val_section="assays", log_category=error):
    validations = []
    all_assays = []
    unique_file_names = []

    study_id = isa_study.identifier

    if isa_study.assays:
        add_msg(validations, val_section, "Found assay(s) for this study", success, val_section,
                val_sequence=1, log_category=log_category)
    else:
        add_msg(validations, val_section, "Could not find any assays", error, descr="Add assay(s) to the study",
                val_sequence=2, log_category=log_category)

    for assay in isa_study.assays:
        is_ms = False
        assays = []
        all_assay_names = []
        all_assay_raw_files = []
        unique_file_names = []
        assay_file_name = os.path.join(study_location, assay.filename)
        assay_df = None
        try:
            assay_df = read_tsv(assay_file_name)
        except FileNotFoundError:
            add_msg(validations, val_section,
                    "The file " + assay_file_name + " was not found",
                    error, assay.filename, val_sequence=2.1, log_category=log_category)
            continue

        assay_type_onto = assay.technology_type
        if assay_type_onto.term == 'mass spectrometry':
            is_ms = True

        assay_header = get_table_header(assay_df, study_id, assay_file_name)
        for header in assay_header:
            if len(header) == 0:
                add_msg(validations, val_section,
                        "Assay sheet '" + assay.filename + "' has empty column header(s)",
                        error, assay.filename, val_sequence=2.1, log_category=log_category)

            if 'Term ' not in header and 'Protocol REF' not in header and 'Unit' not in header:
                assays.append(header)

        # Are the template headers present in the assay
        assay_type = get_assay_type_from_file_name(study_id, assay.filename)
        if assay_type != 'a':  # Not created from the online editor, so we have to skip this validation
            tidy_header_row, tidy_data_row, protocols, assay_desc, assay_data_type, assay_mandatory_type = \
                get_assay_headers_and_protcols(assay_type)
            for idx, template_header in enumerate(tidy_header_row):

                assay_header_pos = None
                for idx, key in enumerate(assay_header):
                    if key == template_header:
                        assay_header_pos = idx  # template_header[idx]
                        break

                if idx != assay_header_pos:
                    add_msg(validations, val_section,
                            "Assay sheet '" + assay.filename + "' column '" + template_header + "' is not in the correct position",
                            info, assay.filename, val_sequence=2.2, log_category=log_category)
                else:
                    add_msg(validations, val_section,
                            "Assay sheet '" + assay.filename + "' column '" + template_header + "' is in the correct position",
                            success, assay.filename, val_sequence=2.2, log_category=log_category)

                if template_header not in assay_header:
                    msg_type = error
                    if template_header in ('Parameter Value[Guard column]', 'Parameter Value[Autosampler model]'):
                        msg_type = info

                    add_msg(validations, val_section,
                            "Assay sheet '" + assay.filename + "' is missing column '" + template_header + "'",
                            msg_type, assay.filename, val_sequence=3, log_category=log_category)

        # Are all relevant rows filled in?
        if not assay_df.empty:
            all_rows = assay_df.shape[0]
            for a_header in assays:
                a_header = str(a_header)  # Names like '1' and '2', gets interpreted as '1.0' and '2.0'
                validate_column, required_column, val_descr = get_assay_column_validations(validation_schema, a_header)
                col_rows = 0  # col_rows = isa_samples[s_header].count()
                try:
                    if validate_column:
                        for row in assay_df[a_header]:
                            if row:
                                col_rows += 1

                            all_sample_names, all_assay_names, validations, unique_file_names = \
                                check_assay_columns(a_header, all_assays, row, validations, val_section,
                                                    assay, unique_file_names, all_assay_names,
                                                    sample_name_list, log_category=log_category)

                        if col_rows < all_rows:

                            if required_column:
                                val_type = error
                            else:
                                val_type = warning

                            if col_rows == 0:
                                add_msg(validations, val_section,
                                        "Assay sheet '" + assay.filename + "' column '" + a_header + "' is empty",
                                        val_type, assay.filename, val_sequence=4, log_category=log_category)
                            else:
                                add_msg(validations,
                                        val_section, "Assay sheet '" + assay.filename + "' column '" + a_header + "' is missing some values. " +
                                        str(col_rows) + " rows found, but there should be " + str(all_rows),
                                        val_type, assay.filename, val_sequence=4, log_category=log_category)
                        else:
                            add_msg(validations, val_section,
                                    "Assay sheet '" + assay.filename + "' column '" + a_header + "' has correct number of rows",
                                    success, assay.filename, val_sequence=5, log_category=log_category)

                except Exception as e:
                    add_msg(validations, val_section,
                            "Assay sheet '" + assay.filename + "' is missing rows for column '" + a_header + "'",
                            error, assay.filename, val_sequence=6, log_category=log_category)

            # We validate all file columns separately here
            validations, all_assay_raw_files = check_all_file_rows(assays, assay_df, validations, val_section,
                                                                   assay.filename, all_rows, log_category=log_category)

            if all_assay_names:
                if len(all_assay_names) < all_rows:
                    add_msg(validations, val_section, "MS/NMR Assay name column should only contain unique values",
                            warning, assay.filename, val_sequence=4, log_category=log_category)
                else:
                    add_msg(validations, val_section, "MS/NMR Assay name column only contains unique values",
                            success, assay.filename, val_sequence=4, log_category=log_category)

        # Correct MAF?
        if header.lower() == 'metabolite assignment file':
            file_name = None
            for row in assay_df[header].unique():
                file_name = row
                break  # We only need one row

            if file_name:
                validate_maf(validations, file_name, all_assay_names, study_location, isa_study.identifier,
                             sample_name_list, is_ms=is_ms, log_category=log_category)
            else:
                add_msg(validations, val_section, "No MAF file referenced for assay sheet " + assay.filename, warning,
                        val_sequence=7.4, log_category=log_category)

    for sample_name in sample_name_list:
        if sample_name not in all_assays:
            add_msg(validations, val_section, "Sample name '" + str(sample_name) + "' is not used in any assay",
                    info, val_sequence=7, log_category=log_category)

    for files in unique_file_names:
        file_name = files.split('|')[0]
        column_name = files.split('|')[1]
        status, file_type, file_description = check_file(files, study_location, file_name_list,
                                                         assay_file_list=all_assay_raw_files)
        if status:
            add_msg(validations, val_section, "File '" + file_name + "' found and appears to be correct for column '"
                    + column_name + "'", success, descr=file_description, val_sequence=8, log_category=log_category)
        else:
            add_msg(validations, val_section, "File '" + file_name + "' of type '" + file_type +
                    "' is missing or not correct for column '" + column_name + "'", error, descr=file_description,
                    val_sequence=9, log_category=log_category)

    return return_validations(val_section, validations, override_list)


def validate_files(study_id, study_location, obfuscation_code, override_list, file_name_list,
                   val_section="files", log_category=error):
    # check for Publication
    validations = []
    assay_file_list = get_assay_file_list(study_location)
    study_files, upload_files, upload_diff, upload_location = \
        get_all_files_from_filesystem(study_id, obfuscation_code, study_location,
                                      directory=None, include_raw_data=True, validation_only=True,
                                      include_upload_folder=False, assay_file_list=assay_file_list)
    sample_cnt = 0
    raw_file_found = False
    derived_file_found = False
    compressed_found = False
    for file in study_files:
        file_name = file['file']
        file_name = str(file_name)
        file_type = file['type']
        file_status = file['status']
        isa_tab_warning = False

        full_file_name = os.path.join(study_location, file_name)

        if file_name != 'audit' and file_name != 'chebi_pipeline_annotations':  # Don't check our internal folders
            if os.path.isdir(os.path.join(full_file_name)):
                for sub_file_name in os.listdir(full_file_name):
                    if is_empty_file(os.path.join(full_file_name, sub_file_name)):
                        add_msg(validations, val_section, "Empty files found is sub-directory", info, val_section,
                                value=os.path.join(file_name, sub_file_name), val_sequence=1, log_category=log_category)

                    # warning for sub folders with ISA tab
                    if sub_file_name.startswith(('i_', 'a_', 's_', 'm_')) and not isa_tab_warning:
                        add_msg(validations, val_section,
                                "Sub-directory " + file_name + " contains ISA-Tab metadata documents",
                                warning, val_section, value=file_name, val_sequence=2, log_category=log_category)
                        isa_tab_warning = True

        if file_name.startswith('Icon') or file_name.lower() == 'desktop.ini' or file_name.lower() == '.ds_store' \
                or '~' in file_name or file_name.startswith('.'):  # "or '+' in file_name" taken out
            add_msg(validations, val_section, "Special files should be removed from the study folder",
                    warning, val_section, value=file_name, val_sequence=3, log_category=log_category)
            continue

        if file_name.startswith(('i_', 'a_', 's_', 'm_')):
            if file_name.startswith('s_') and file_status == 'active':
                sample_cnt += 1

            if sample_cnt > 1:
                add_msg(validations, val_section, "Only one active sample sheet per study is allowed", error,
                        val_section, value='Number of active sample sheets ' + str(sample_cnt),
                        val_sequence=4, log_category=log_category)

            if file_status == 'old':
                add_msg(validations, val_section, "Old metadata file should be removed", warning,
                        val_section, value=file_name, val_sequence=5, log_category=log_category)

        if is_empty_file(full_file_name):
            if file_name not in 'metexplore_mapping.json':
                add_msg(validations, val_section, "Empty files are not allowed: '" + file_name + "'",
                        error, val_section,
                        value=file_name, val_sequence=6, log_category=log_category)

        if file_type == 'raw':
            raw_file_found = True

        if file_type == 'derived':
            derived_file_found = True

        if file_type == 'compressed':
            compressed_found = True

        file_name_list.append(file_name)

    if not raw_file_found and not derived_file_found:
        if compressed_found:
            add_msg(validations, val_section, "No raw or derived files, but compressed files found", warning,
                    val_section, value="", val_sequence=7, log_category=log_category)
        else:
            add_msg(validations, val_section, "No raw or derived files found", error, val_section,
                    value="", val_sequence=7, log_category=log_category)
    elif not raw_file_found and derived_file_found:
        add_msg(validations, val_section, "No raw files found, but there are derived files", warning, val_section,
                value="", descr="Ideally you should provide both raw and derived files",
                val_sequence=8, log_category=log_category)
    elif not derived_file_found:
        add_msg(validations, val_section, "No derived files found", warning, val_section,
                value="", val_sequence=9, log_category=log_category)
    elif not raw_file_found:
        add_msg(validations, val_section, "No raw files found", error, val_section,
                value="", val_sequence=10, log_category=log_category)

    return return_validations(val_section, validations, override_list)


def validate_samples(isa_study, isa_samples, validation_schema, file_name, override_list, sample_name_list,
                     val_section="samples", log_category=error):
    # check for Publication
    validations = []
    samples = []

    if validation_schema:
        study_val = validation_schema['study']
        val = study_val['samples']
        all_val = val['default_order']

    # Get an indexed header row
    s_file_name = isa_study.filename
    sample_header = get_table_header(isa_samples, isa_study.identifier, s_file_name)
    for h_sample in sample_header:
        if 'Term ' not in h_sample:
            samples.append(h_sample)

    for idx, sample in enumerate(all_val):
        sample_val_name = sample['header']
        if sample_val_name in samples:
            if sample_val_name == 'Protocol REF':  # Don't need to output this column name
                continue
            add_msg(validations, val_section, "Sample column '" + sample_val_name + "' found in the sample file",
                    success, file_name, val_sequence=1, log_category=log_category)
        else:
            if sample_val_name == 'Characteristics[Variant]' or sample_val_name == 'Characteristics[Sample type]':  # Not all studies have these present
                add_msg(validations, val_section, "Sample column '" + sample_val_name + "' was not found", info,
                        file_name, val_sequence=2, log_category=log_category)
                continue
            add_msg(validations, val_section, "Sample column '" + sample_val_name + "' was not found", error,
                    file_name, val_sequence=3, log_category=log_category)
    # Has the submitter use the term 'Human' and not 'Homo sapiens'?
    human_found = False
    too_short = False

    # Are all relevant rows filled in?
    if not isa_samples.empty:
        # isa_samples.replace({"", np.NaN}, inplace=True)
        all_rows = isa_samples.shape[0]
        for s_header in samples:
            col_rows = 0  # col_rows = isa_samples[s_header].count()
            for row in isa_samples[s_header]:
                if str(row):  # Float values with 0.0 are not counted, so convert to string first
                    col_rows += 1
                if s_header == 'Characteristics[Organism]':
                    if 'human' == row.lower() or 'man' == row.lower():
                        human_found = True
                    elif len(row) < 5:  # ToDo, read from all_val[idx][ontology-details][rules][0][value]
                        too_short = True

                    if row.lower() in incorrect_species:
                        add_msg(validations, val_section,
                                "Organism can not be '" + row + "', choose the appropriate taxonomy term",
                                error, file_name, val_sequence=4, log_category=log_category)

                    if ':' in row:
                        add_msg(validations, val_section,
                                "Organism should not contain the actual ontology/taxonomy name, "
                                "please include just the appropriate taxonomy term",
                                warning, file_name, val_sequence=5, log_category=log_category)

                elif s_header.lower() == 'sample name':
                    if row:
                        row = str(row)
                        if row not in sample_name_list:
                            sample_name_list.append(row)

            if col_rows < all_rows:
                val_stat = error

                if s_header == 'Characteristics[Variant]':  # This is a new column we like to see, but not mandatory
                    val_stat = info

                if 'Factor Value' in s_header:  # User defined factors may not all have data in all rows
                    val_stat = info

                add_msg(validations, val_section, "Sample sheet column '" + s_header + "' is missing values. " +
                        str(col_rows) + " rows found, but there should be " + str(all_rows), val_stat, file_name,
                        val_sequence=6, log_category=log_category)
            else:
                add_msg(validations, val_section, "Sample sheet column '" + s_header + "' has correct number of rows",
                        success, file_name, val_sequence=7, log_category=log_category)

        if sample_name_list:
            if len(sample_name_list) != all_rows:
                add_msg(validations, val_section, "Sample name column must only contain unique values",
                        error, file_name, val_sequence=4, log_category=log_category)
            else:
                add_msg(validations, val_section, "Sample name column contains unique values",
                        success, file_name, val_sequence=4, log_category=log_category)

    if human_found:
        add_msg(validations, val_section,
                "Organism can not be 'human' or 'man', please choose the 'Homo sapiens' taxonomy term",
                error, file_name, val_sequence=8, log_category=log_category)
    if too_short:
        add_msg(validations, val_section, "Organism name is missing or too short (<5 characters)", error, file_name,
                val_sequence=9, log_category=log_category)

    return return_validations(val_section, validations, override_list)


def validate_protocols(isa_study, validation_schema, file_name, override_list, val_section="protocols",
                       log_category=error):
    # check for Publication
    validations = []
    protocol_order_list = None
    is_nmr = False
    is_ms = False
    default_prots = []

    if isa_study.assays:
        for assay in isa_study.assays:
            assay_type_onto = assay.technology_type
            if assay_type_onto.term == 'mass spectrometry':
                is_ms = True
            elif assay_type_onto.term == 'NMR spectroscopy':
                is_nmr = True

    # List if standard protocols that should be present
    if validation_schema:
        study_val = validation_schema['study']
        # maf_name = os.path.join(study_location, file_name)
        val = study_val['protocols']
        protocol_order_list = val['default']
        for prot in protocol_order_list:
            if is_nmr:
                if 'NMR ' in prot['technique']:
                    default_prots.append(prot)
            elif is_ms:
                if 'mass ' in prot['technique']:
                    default_prots.append(prot)

    # protocol order
    for idx, protocol in enumerate(default_prots):
        prot_val_name = protocol['title']
        try:
            isa_prot = isa_study.protocols[idx]
            isa_prot_name = isa_prot.name
            isa_prot_type = isa_prot.protocol_type
            isa_prot_type_name = isa_prot_type.term

            if isa_prot_name != isa_prot_type_name:
                add_msg(validations, val_section, "Protocol '" + isa_prot_name +
                        "' does not match the protocol type name '" + isa_prot_type_name + "'",
                        warning, file_name, val_sequence=1, log_category=log_category)
            else:
                add_msg(validations, val_section, "Protocol '" + isa_prot_name + "' match the protocol type",
                        success, file_name, val_sequence=1, log_category=log_category)

            if prot_val_name != isa_prot_name:
                add_msg(validations, val_section, "Protocol '" + isa_prot_name +
                        "' is not in the correct position or name has different case/spelling",
                        warning, file_name, val_sequence=1, log_category=log_category)
            else:
                add_msg(validations, val_section, "Protocol '" + isa_prot_name +
                        "' is in the correct position and name has correct case/spelling",
                        success, file_name, val_sequence=2, log_category=log_category)
        except:
            add_msg(validations, val_section, "Protocol '" + prot_val_name + "' was not found", error, file_name,
                    val_sequence=3, log_category=log_category)

    name_rules, name_val_description = get_complex_validation_rules(
        validation_schema, part='protocols', sub_part='protocol', sub_set='name')
    name_val_len, name_val_error, name_val_condition, name_val_type = extract_details(name_rules)

    desc_rules, desc_val_description = get_complex_validation_rules(
        validation_schema, part='protocols', sub_part='protocol', sub_set='description')
    desc_val_len, desc_val_error, desc_val_condition, desc_val_type = extract_details(desc_rules)

    param_rules, param_val_description = get_complex_validation_rules(
        validation_schema, part='protocols', sub_part='protocol', sub_set='parameterName')
    param_val_len, param_val_error, param_val_condition, param_val_type = extract_details(param_rules)

    if isa_study.protocols:
        for protocol in isa_study.protocols:
            prot_name = protocol.name
            prot_desc = protocol.description
            clean_prot_desc = remove_nonprintable(prot_desc)
            prot_params = protocol.protocol_type

            # non printable characters
            if prot_desc != clean_prot_desc:
                add_msg(validations, val_section, "Protocol description contains non printable characters",
                        error, file_name, value=prot_desc, val_sequence=4, log_category=log_category)
            else:
                add_msg(validations, val_section, "Protocol description only contains printable characters",
                        success, file_name, value=prot_desc, val_sequence=5, log_category=log_category)

            if len(prot_name) >= name_val_len:
                add_msg(validations, val_section, "Protocol name '" + prot_name + "' validates", success, file_name,
                        value=prot_name, val_sequence=6, log_category=log_category)
            else:
                add_msg(validations, val_section, prot_name + ": " + name_val_error, error, file_name, value=prot_name,
                        descr=name_val_description, val_sequence=7, log_category=log_category)

            if len(prot_desc) >= desc_val_len:
                if prot_desc == 'Please update this protocol description':
                    add_msg(validations, "Protocol", prot_name + ": " + desc_val_error, warning, file_name,
                            value=prot_desc, descr='Please update this protocol description',
                            val_sequence=8, log_category=log_category)
                add_msg(validations, val_section, "Protocol description validates", success, file_name,
                        value=prot_desc, val_sequence=9, log_category=log_category)
            else:
                if prot_desc.lower().rstrip('.') in('no metabolites', 'not applicable',
                                                    'no metabolites were identified',
                                                    'no data transformation was required'):
                    add_msg(validations, val_section, "Protocol description validates", success, file_name,
                            value=prot_desc, val_sequence=10, log_category=log_category)
                else:
                    add_msg(validations, val_section, prot_name + ": " + desc_val_error, error, file_name,
                            value=prot_desc, descr=desc_val_description, val_sequence=11, log_category=log_category)

            if prot_params and len(prot_params.term) >= param_val_len:
                add_msg(validations, val_section, "Protocol parameter(s) validates", success, file_name,
                        value=prot_params.term, val_sequence=12, log_category=log_category)
            else:
                if prot_params:
                    add_msg(validations, val_section, prot_name + ": " + param_val_error, error, file_name,
                            value=prot_params.term, descr=param_val_description,
                            val_sequence=13, log_category=log_category)
                else:
                    add_msg(validations, val_section, prot_name + ": " + param_val_error, error, file_name,
                            value="", descr=param_val_description, val_sequence=14, log_category=log_category)

    return return_validations(val_section, validations, override_list)


def validate_contacts(isa_study, validation_schema, file_name, override_list, val_section="person", log_category=error):
    # check for People ie. authors
    validations = []

    last_name_rules, last_name_val_description = get_complex_validation_rules(
        validation_schema, part='people', sub_part='person', sub_set='lastName')
    last_name_val_len, last_name_val_error, last_name_val_condition, last_name_val_type = \
        extract_details(last_name_rules)

    first_name_rules, first_name_val_description = get_complex_validation_rules(
        validation_schema, part='people', sub_part='person', sub_set='firstName')
    first_name_val_len, first_name_val_error, first_name_val_condition, first_name_val_type = \
        extract_details(first_name_rules)

    email_rules, email_val_description = get_complex_validation_rules(
        validation_schema, part='people', sub_part='person', sub_set='email')
    email_val_len, email_val_error, email_val_condition, email_val_type = extract_details(email_rules)

    affiliation_rules, affiliation_val_description = get_complex_validation_rules(
        validation_schema, part='people', sub_part='person', sub_set='affiliation')
    affiliation_val_len, affiliation_val_error, affiliation_val_condition, affiliation_val_type = \
        extract_details(affiliation_rules)

    if isa_study.contacts:
        for person in isa_study.contacts:
            last_name = person.last_name
            first_name = person.first_name
            email = person.email
            affiliation = person.affiliation

            if last_name:
                if len(last_name) >= last_name_val_len and validate_name(last_name, 'last_name'):
                    add_msg(validations, val_section, "Person last name '" + last_name + "' validates ", success,
                            file_name, val_sequence=1, log_category=log_category)
                else:
                    add_msg(validations, val_section, last_name_val_error, error, file_name, value=last_name,
                            descr=last_name_val_description, val_sequence=2, log_category=log_category)

                # if not validate_name(last_name, 'last_name'):
                #     add_msg(validations, val_section, "Person last name '" + last_name + "' is not valid ", error,
                #             file_name, val_sequence=1.1, log_category=log_category)

            if first_name:
                if len(first_name) >= first_name_val_len and validate_name(first_name, 'first_name'):
                    add_msg(validations, val_section, "Person first name '" + first_name + "' validates", success,
                            file_name, val_sequence=3, log_category=log_category)
                else:
                    add_msg(validations, val_section, first_name_val_error, error, file_name, value=first_name,
                            descr=first_name_val_description, val_sequence=4, log_category=log_category)

                # if not validate_name(first_name, 'first_name'):
                #     add_msg(validations, val_section, "Person first name '" + last_name + "' is not valid ", error,
                #             file_name, val_sequence=3.1, log_category=log_category)

            if email:
                if len(email) >= 7:
                    add_msg(validations, val_section, "Person email '" + email + "' validates", success, file_name,
                            val_sequence=5, log_category=log_category)
                else:
                    add_msg(validations, val_section, email_val_error, info, file_name, value=email,
                            descr=email_val_error, val_sequence=6, log_category=log_category)

            if affiliation:
                if len(affiliation) >= affiliation_val_len:
                    add_msg(validations, val_section, "Person affiliation '" + affiliation + "' validates", success,
                            file_name, val_sequence=7, log_category=log_category)
                else:
                    add_msg(validations, val_section, affiliation_val_error, info, file_name, value=affiliation,
                            descr=affiliation_val_error, val_sequence=8, log_category=log_category)
    else:
        add_msg(validations, val_section, "No study persons/authors found", error, file_name,
                val_sequence=9, log_category=log_category)

    return return_validations(val_section, validations, override_list)


def check_doi(pub_doi, doi_val):
    # doi = pub_doi
    # doi_pattern = re.compile(doi_val) # Todo, fix pattern
    # doi_check = doi_pattern.match(doi)
    # retun doi_check

    if 'http' in pub_doi or 'doi.org' in pub_doi:
        return False

    return True


def validate_name(name, name_type):
    validates = True
    name = name.lower()

    if name_type == 'last_name':
        if name in last_name_black_list:
            return False
    elif name_type == 'first_name':
        if name in first_name_black_list:
            return False

    return validates


def validate_publication(isa_study, validation_schema, file_name, override_list, val_section="publication",
                         log_category=error):
    # check for Publication
    validations = []

    title_rules, title_val_description = get_complex_validation_rules(
        validation_schema, part='publications', sub_part='publication', sub_set='title')
    title_val_len, title_val_error, title_val_condition, title_val_type = extract_details(title_rules)

    if isa_study.publications:
        pmid = False
        doi = False

        pmid_rules, pmid_val_description = get_complex_validation_rules(
            validation_schema, part='publications', sub_part='publication', sub_set='pubMedID')
        pmid_val_len, pmid_val_error, pmid_val_condition, pmid_val_type = extract_details(pmid_rules)

        doi_rules, doi_val_description = get_complex_validation_rules(
            validation_schema, part='publications', sub_part='publication', sub_set='doi')
        doi_val, doi_val_error, doi_val_condition, doi_val_type = extract_details(doi_rules)

        author_rules, author_val_description = get_complex_validation_rules(
            validation_schema, part='publications', sub_part='publication', sub_set='authorList')
        author_val_len, author_val_error, author_val_condition, author_val_type = extract_details(author_rules)

        status_rules, status_val_description = get_complex_validation_rules(
            validation_schema, part='publications', sub_part='publication', sub_set='status')
        status_val_len, status_val_error, status_val_condition, status_val_type = extract_details(status_rules)

        for index, publication in enumerate(isa_study.publications):

            if not publication:
                add_msg(validations, val_section, title_val_error, error, file_name,
                        val_sequence=1, log_category=log_category)
            else:
                add_msg(validations, val_section, "Found a publication", success, file_name,
                        val_sequence=2, log_category=log_category)

            if not publication.title:
                add_msg(validations, val_section, title_val_error, error, file_name,
                        val_sequence=3, log_category=log_category)
            elif publication.title:
                if len(publication.title) >= title_val_len:
                    add_msg(validations, val_section, "Found the title of the publication '" + publication.title + "'",
                            success, file_name, val_sequence=4, log_category=log_category)
                else:
                    add_msg(validations, val_section, title_val_error, warning,
                            file_name, value=publication.title, descr=title_val_description,
                            val_sequence=5, log_category=log_category)

            if not publication.doi:
                add_msg(validations, val_section, doi_val_description, warning, file_name,
                        val_sequence=6, log_category=log_category)
                doi = False
            elif publication.doi:
                if check_doi(publication.doi, doi_val):
                    add_msg(validations, val_section, "Found the doi '" + publication.doi + "' for the publication",
                            success, file_name, val_sequence=7, log_category=log_category)
                    doi = True
                else:
                    add_msg(validations, val_section, doi_val_error + ": " + publication.doi, warning, file_name, doi,
                            descr=publication.doi, val_sequence=8, log_category=log_category)
                    doi = False

            if not publication.pubmed_id:
                add_msg(validations, val_section, pmid_val_description, warning, file_name,
                        val_sequence=9, log_category=log_category)
                pmid = False
            elif publication.pubmed_id:
                try:
                    int(publication.pubmed_id)
                except ValueError:
                    if publication.pubmed_id != 'none':
                        add_msg(validations, val_section, pmid_val_error, error, file_name,
                                value=publication.pubmed_id, descr=pmid_val_description,
                                val_sequence=10, log_category=log_category)

                if len(publication.pubmed_id) >= int(pmid_val_len):
                    add_msg(validations, val_section,
                            "Found pmid '" + publication.pubmed_id + "' for the publication", success, file_name,
                            val_sequence=11, log_category=log_category)
                    pmid = True
                else:
                    add_msg(validations, val_section, pmid_val_error, error, file_name,
                            value=publication.pubmed_id, descr=pmid_val_description,
                            val_sequence=12, log_category=log_category)
                    pmid = False

            if not doi or not pmid:
                add_msg(validations, val_section,
                        "Please provide both a valid doi and pmid for the publication",
                        warning, file_name, val_sequence=13, log_category=log_category)
            elif doi and pmid:
                add_msg(validations, val_section,
                        "Found both doi and pmid for the publication", success, file_name,
                        val_sequence=14, log_category=log_category)

            if not publication.author_list:
                add_msg(validations, val_section, author_val_error, error, file_name,
                        val_sequence=15, log_category=log_category)
            elif publication.author_list:
                if len(publication.author_list) >= author_val_len:
                    add_msg(validations, val_section, "Found the author list for the publication",
                            success, file_name, val_sequence=16, log_category=log_category)
                else:
                    add_msg(validations, val_section, author_val_error, error, file_name,
                            value=publication.author_list, descr=author_val_description,
                            val_sequence=17, log_category=log_category)

            if not publication.status:
                add_msg(validations, val_section, "Please provide the publication status",
                        error, file_name, val_sequence=18, log_category=log_category)
            elif publication.status:
                pub_status = publication.status
                if len(pub_status.term) >= status_val_len:
                    add_msg(validations, val_section, "Found the publication status",
                            success, file_name, val_sequence=19, log_category=log_category)
                else:
                    add_msg(validations, val_section, status_val_error, success, file_name,
                            value=pub_status.title, descr=status_val_description,
                            val_sequence=20, log_category=log_category)
    else:
        add_msg(validations, val_section, title_val_error, error, val_sequence=21, log_category=log_category)

    return return_validations(val_section, validations, override_list)


def validate_basic_isa_tab(study_id, user_token, study_location, override_list, log_category=error):
    validates = True
    amber_warning = False
    validations = []
    val_section = "basic"
    inv_file_name = 'i_Investigation.txt'
    isa_inv = None
    isa_study = None
    isa_sample_df = None
    std_path = None
    s_file = None
    assay_files = None

    try:

        if os.path.isfile(os.path.join(study_location, inv_file_name)):
            isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                             skip_load_tables=True,
                                                             study_location=study_location)
            try:
                file_name = isa_study.filename
                isa_sample_df = read_tsv(os.path.join(study_location, file_name))
            except FileNotFoundError:
                add_msg(validations, val_section, "The file " + file_name + " was not found", error,
                        inv_file_name, val_sequence=1.1, log_category=log_category)
            except Exception as e:
                add_msg(validations, val_section, "Could not load the minimum ISA-Tab files", error,
                        inv_file_name, val_sequence=1.2, log_category=log_category)
        else:
            add_msg(validations, val_section, "Could not load the minimum ISA-Tab files", error,
                    inv_file_name, val_sequence=1.2, log_category=log_category)

    except ValueError:
        err = traceback.format_exc()
        add_msg(validations, val_section,
                "Loading ISA-Tab without sample and assay tables. "
                "Protocol parameters does not match the protocol definition",
                warning, file_name, val_sequence=1, log_category=log_category)
        logger.error("Cannot load ISA-Tab with sample and assay tables due to critical error: " + err)

    if isa_inv:
        add_msg(validations, val_section, "Successfully read the investigation file", success,
                'i_Investigation.txt', val_sequence=2, log_category=log_category)

        if isa_study:
            add_msg(validations, val_section, "Successfully read the study section of the investigation file", success,
                    'i_Investigation.txt', val_sequence=3, log_category=log_category)
        else:
            add_msg(validations, val_section, "Can not read the study section of the investigation file", error,
                    'i_Investigation.txt', val_sequence=4, log_category=log_category)
            validates = False

        if isa_study.filename:
            add_msg(validations, val_section, "Successfully found the reference to the sample sheet filename", success,
                    'i_Investigation.txt', val_sequence=5, log_category=log_category)
        else:
            add_msg(validations, val_section, "Could not find the reference to the sample sheet filename", error,
                    file_name, val_sequence=6, log_category=log_category)
            validates = False

        # isaconfig
        if isa_inv.get_comment('Created With Configuration'):
            create_config = isa_inv.get_comment('Created With Configuration')
            open_config = None
            if isa_inv.get_comment('Last Opened With Configuration'):
                open_config = isa_inv.get_comment('Last Opened With Configuration')

            if 'isaconfig' in create_config.value:
                add_msg(validations, val_section, "Incorrect configuration files used to create the study ("
                        + create_config.value + "). The study may not contain required fields",
                        warning, file_name, val_sequence=7, log_category=log_category)
                amber_warning = True
            if 'isaconfig' in open_config.value:
                add_msg(validations, val_section, "Incorrect configuration files used to edit the study ("
                        + open_config.value + "). The study may not contain required fields",
                        warning, file_name, val_sequence=8, log_category=log_category)
                amber_warning = True

        if validates:  # Have to have a basic investigation and sample file before we can continue
            if isa_study.samples:
                add_msg(validations, val_section, "Successfully found one or more samples", success, file_name,
                        val_sequence=9, log_category=log_category)
            elif not isa_sample_df.empty:
                add_msg(validations, val_section, "Successfully found one or more samples", success, file_name,
                        val_sequence=10, log_category=log_category)
            else:
                add_msg(validations, val_section, "Could not find any samples",
                        error, file_name, val_sequence=11, log_category=log_category)

            if isa_study.assays:
                add_msg(validations, val_section, "Successfully found one or more assays", success, file_name,
                        val_sequence=12, log_category=log_category)
            else:
                add_msg(validations, val_section, "Could not find any assays",
                        error, file_name, val_sequence=13, log_category=log_category)

            if isa_study.factors:
                add_msg(validations, val_section, "Successfully found one or more factors", success, file_name,
                        val_sequence=14, log_category=log_category)
            else:
                add_msg(validations, val_section, "Could not find any factors",
                        warning, file_name, val_sequence=15, log_category=log_category)

            if isa_study.design_descriptors:
                add_msg(validations, val_section, "Successfully found one or more descriptors", success, file_name,
                        val_sequence=16, log_category=log_category)
            else:
                add_msg(validations, val_section, "Could not find any study design descriptors",
                        error, file_name, val_sequence=17, log_category=log_category)

    else:
        add_msg(validations, "ISA-Tab", "Can not find or read the investigation file",
                error, inv_file_name, val_sequence=18, log_category=log_category)

    validates, amber_warning, ret_list = return_validations(val_section, validations, override_list)

    inv_file = 'i_Investigation.txt'

    if isa_study:
        s_file = isa_study.filename
        assays = isa_study.assays
        assay_files = []
        for assay in assays:
            assay_files.append(assay.filename)

    return isa_study, isa_inv, isa_sample_df, std_path, validates, amber_warning, \
           ret_list, inv_file, s_file, assay_files


def validate_isa_tab_metadata(isa_inv, isa_study, validation_schema, file_name, override_list,
                              val_section="isa-tab", log_category=error):
    validations = []

    if validation_schema:
        title_rules, title_descr = get_basic_validation_rules(validation_schema, 'title')
        desc_rules, desc_desrc = get_basic_validation_rules(validation_schema, 'description')

    if isa_inv:

        # Title
        val_len, val_error, val_condition, val_type = extract_details(title_rules)
        try:
            title_len = len(isa_study.title)
        except:
            title_len = 0

        if title_len >= val_len:
            add_msg(validations, val_section, "The title length validates", success, file_name,
                    val_sequence=1, log_category=log_category)
        else:
            add_msg(validations, val_section, val_error, error, file_name,
                    value=isa_study.title, descr=title_descr, val_sequence=2, log_category=log_category)

        # Description
        val_len, val_error, val_condition, val_type = extract_details(desc_rules)
        try:
            descr_len = len(isa_study.description)
        except:
            descr_len = 0

        if descr_len >= val_len:
            add_msg(validations, val_section, "The length of the description validates", success, file_name,
                    val_sequence=3, log_category=log_category)
        else:
            add_msg(validations, val_section, val_error, error, file_name,
                    value=isa_study.description, descr=desc_desrc, val_sequence=4, log_category=log_category)

    else:
        add_msg(validations, val_section, "Can not find or read the investigation file", error,
                val_sequence=5, log_category=log_category)

    return return_validations(val_section, validations, override_list)


class OverrideValidation(Resource):
    @swagger.operation(
        summary="Approve or reject a specific validation rule (curator only)",
        notes='''For EBI curatiors to manually approve or fail a validation step.</br>
        <pre><code>
    { 
      "validations": [
        {
          "publication_3": "The PubChem id is for a different paper",
          "people_3": "The contact has given an incorrect email address",
          "files_1": ""
        } 
      ]
    }
    </code></pre>''',
        parameters=[
            {
                "name": "study_id",
                "description": "Study to override validations",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "validations",
                "description": 'which validation rules to override.',
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
                "message": "OK."
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication. "
                           "Please provide a study id and a valid user token"
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token"
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def post(self, study_id):

        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None or study_id is None:
            abort(401)

        study_id = study_id.upper()

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not is_curator:
            abort(403)

        override_list = []
        # First, get all existing validations from the database
        try:
            query_list = override_validations(study_id, 'query')
            if query_list:
                for val in query_list[0].split('|'):
                    override_list.append(val)
        except Exception as e:
            logger.error('Can not query existing overridden validations from the database')

        # Get the new validations submitted
        data_dict = json.loads(request.data.decode('utf-8'))
        validation_data = data_dict['validations']

        # only add unique validations to the update statement
        for val, val_message in validation_data[0].items():
            val_found = False
            for existing_val in override_list:
                if val in existing_val:
                    val_found = True

            if not val_found:
                override_list.append(val + ':' + val_message)

        db_update_string = ""
        for existing_val in override_list:
            db_update_string = db_update_string + existing_val + '|'
        db_update_string = db_update_string[:-1]  # Remove trailing pipeline

        try:
            query_list = override_validations(study_id, 'update', override=db_update_string)
        except Exception as e:
            logger.error('Can not store overridden validations on the database')

        return {"success": "Validations stored in the database"}
