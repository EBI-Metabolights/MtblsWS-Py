import json
import traceback
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

correct_maf_order = [{0: "database_identifier"}, {1: "chemical_formula"}, {2: "smiles"}, {3: "inchi"},
                     {4: "metabolite_identification"}, {5: "mass_to_charge"}]

warning = "warning"
error = "error"
success = "success"
info = "info"


def add_msg(validations, section, message, status, meta_file="", value="", desrc=""):
    validations.append({section: message, "status": status, "metadata_file": meta_file,
                        "value": value, "desciption": desrc})


def get_basic_validation_rules(validation_schema, part):
    if validation_schema:
        study_val = validation_schema['study']
        val = study_val[part]
        rules = val['rules'][0]
    return rules, val['description']


def get_complex_validation_rules(validation_schema, part, sub_part, sub_set):
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
    for idx, val in enumerate(validations):
        idx += 1  # Set the sequence to 1, as this is the section we will override
        val_sequence = section + '_' + str(idx)
        val["val_sequence"] = val_sequence
        val["val_override"] = 'false'
        val["val_message"] = ''
        if len(override_list) > 1:
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
    for idx, val in enumerate(validations):
        status = val["status"]
        if status == error:
            error_found = True
        elif status == warning:
            warning_found = True

    if error_found:
        validates = False
        ret_list = {section: validations, "status_message": "Validation failed",
                    "overall_status": error}
    elif warning_found:
        amber_warning = True
        ret_list = {section: validations,
                    "status_message": "Some optional information is missing for your study",
                    "overall_status": warning}
    else:
        ret_list = {section: validations, "status_message": "Successful validation",
                    "overall_status": success}

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


def check_file(file_name_and_column, study_location, file_name_list):
    file_name = file_name_and_column.split('|')[0]
    column_name = file_name_and_column.split('|')[1]
    if file_name not in file_name_list:
        return False, 'missing file', "File " + file_name + " does not exist"

    file_type, status, folder = map_file_type(file_name, study_location)
    if is_empty_file(os.path.join(study_location, file_name)):
        return False, file_type, "File " + file_name + " is empty"

    if file_type == 'metadata_maf' and column_name == 'Metabolite Assignment File':
        if file_name.startswith('m_') and file_name.endswith('_v2_maf.tsv'):
            return True, file_type, 'Correct file ' + file_name + ' for column ' + column_name
        else:
            return False, file_type,  "The " + column_name + " must start with 'm_' and end in '_v2_maf.tsv'"

    if file_type == 'raw' and column_name == 'Raw Spectral Data File':
        return True, file_type, 'Correct file ' + file_name + ' for column ' + column_name
    elif file_type == 'derived' and (column_name == 'Derived Spectral Data File'
                                     or column_name == 'Raw Spectral Data File'):
        return True, file_type, 'Correct file ' + file_name + ' for column ' + column_name
    elif file_type != 'derived' and column_name == 'Derived Spectral Data File':
        return False, file_type, 'Incorrect file ' + file_name + ' or file type for column ' + column_name
    elif file_type == 'spreadsheet' and column_name == 'Derived Spectral Data File':
        return True, file_type, 'Correct file ' + file_name + ' for column ' + column_name
    elif file_type == 'compressed' and column_name == 'Free Induction Decay Data File':
        return True, file_type, 'Correct file ' + file_name + ' for column ' + column_name
    elif file_type != 'raw' and column_name == 'Raw Spectral Data File':
        return False, file_type, 'Incorrect file ' + file_name + ' or file type for column ' + column_name

    return status, file_type, 'n/a'


def maf_messages(header, pos, incorrect_pos, maf_header, incorrect_message, validations, file_name):
    try:
        if maf_header[header] != pos:
            incorrect_message = incorrect_message + header + " is not the correct position. "
            incorrect_pos = True
    except:
        incorrect_message = incorrect_message + " Column '" + header + "' is missing from " + file_name + ". "
        incorrect_pos = True

    return incorrect_pos, incorrect_message, validations


def validate_maf(validations, file_name, all_assay_names, study_location, study_id, is_ms, sample_name_list):
    val_section = "maf"
    maf_name = os.path.join(study_location, file_name)
    maf_df = None
    try:
        maf_df = read_tsv(maf_name)
    except:
        add_msg(validations, val_section, "Could not find or read file '" + file_name + "'", error)

    incorrect_pos = False
    incorrect_message = ""

    if not maf_df.empty:
        maf_header = get_table_header(maf_df, study_id, maf_name)

        for idx, col in enumerate(correct_maf_order):
            incorrect_pos, incorrect_message, validations = \
                maf_messages(col[idx], idx, incorrect_pos, maf_header, incorrect_message, validations, file_name)

        if incorrect_pos:
            add_msg(validations, val_section, incorrect_message, error)
        else:
            add_msg(validations, val_section,
                    "Columns 'database_identifier', 'chemical_formula', 'smiles', 'inchi' and "
                    "'metabolite_identification' found in the correct column position", success)

        try:
            if maf_header['mass_to_charge']:
                check_maf_rows(validations, val_section, maf_df, 'mass_to_charge', is_ms)
        except:
            logger.info("No mass_to_charge column found in the MAF")

        # NMR/MS Assay Names OR Sample Names are added to the sheet
        if all_assay_names:
            for assay_name in all_assay_names:
                try:
                    maf_header[assay_name]
                    add_msg(validations, val_section, "MS/NMR Assay Name '" + assay_name + "' found in the MAF", success)
                    check_maf_rows(validations, val_section, maf_df, assay_name, is_ms)
                except:
                    add_msg(validations, val_section, "MS/NMR Assay Name '" + assay_name + "' not found in the MAF", error)

        if not all_assay_names and sample_name_list:
            for sample_name in sample_name_list:
                try:
                    maf_header[sample_name]
                    add_msg(validations, val_section, "Sample Name '" + sample_name + "' found in the MAF", success)
                    check_maf_rows(validations, val_section, maf_df, sample_name, is_ms)
                except:
                    add_msg(validations, val_section, "Sample Name '" + sample_name + "' not found in the MAF", error)


def check_maf_rows(validations, val_section, maf_df, column_name, is_ms):
    all_rows = maf_df.shape[0]
    col_rows = 0
    # Are all relevant rows filled in?
    for row in maf_df[column_name]:
        if row:
            col_rows += 1

    if col_rows == all_rows:
        add_msg(validations, val_section, "All values for '" + column_name + "' found in the MAF", success)
    else:
        # For MS we should have m/z values, for NMR the chemical shift is equally important.
        if (is_ms and column_name == 'mass_to_charge') or (not is_ms and column_name == 'chemical_shift'):
            add_msg(validations, val_section, "Missing values for '" + column_name + "' in the MAF. " +
                    str(col_rows) + " rows found, but there should be " + str(all_rows), warning)
        else:
            add_msg(validations, val_section, "Missing values for '" + column_name + "' in the MAF. " +
                    str(col_rows) + " rows found, but there should be " + str(all_rows), info)


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
                "name": "val_section",
                "description": "Specify which validations to run, default is all: "
                               "isa-tab_metadata,publication,protocols,people,samples,assays,maf,files",
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
        if not write_access:
            abort(403)

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('val_section', help="Validation section", location="args")
        args = parser.parse_args()
        val_section = args['val_section']
        if val_section is None:
            val_section = 'all'  # All

        return validate_study(study_id, study_location, user_token, obfuscation_code, val_section)


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


def validate_study(study_id, study_location, user_token, obfuscation_code, validation_section='all'):
    all_validations = []
    validation_schema = None
    error_found = False
    warning_found = False
    validation_section = validation_section.lower()

    try:
        validation_schema_file = app.config.get('VALIDATIONS_FILE')
        with open(validation_schema_file, 'r') as json_file:
            validation_schema = json.load(json_file)
    except:
        all_validations.append({"info": "Could not find the validation schema, only basic validation will take place",
                                "status": success})

    override_list = []
    try:
        query_list = override_validations(study_id, 'query')
        for val in query_list[0].split('|'):
            override_list.append(val)
    except Exception as e:
        logger.error('Can not query overridden validations from the database')

    # Validate basic ISA-Tab structure
    isa_study, isa_inv, isa_samples, std_path, status, amber_warning, isa_validation, inv_file, s_file, assay_files = \
        validate_basic_isa_tab(study_id, user_token, study_location, override_list)
    all_validations.append(isa_validation)
    if not status:
        error_found = True
    if amber_warning:
        warning_found = True

    # We can now run the rest of the validation checks

    # Validate publications reported on the study
    val_section = "publication"
    if validation_section == 'all' or val_section in validation_section:
        status, amber_warning, pub_validation = validate_publication(
            isa_study, validation_schema, inv_file, override_list, val_section)
        all_validations.append(pub_validation)

    # Validate detailed metadata in ISA-Tab structure
    val_section = "isa-tab_metadata"
    if validation_section == 'all' or val_section in validation_section:
        status, amber_warning, isa_meta_validation = validate_isa_tab_metadata(
            isa_inv, isa_study, validation_schema, inv_file, override_list, val_section)
        all_validations.append(isa_meta_validation)

    # Validate Person (authors)
    val_section = "people"
    if validation_section == 'all' or val_section in validation_section:
        status, amber_warning, isa_person_validation = validate_contacts(
            isa_study, validation_schema, inv_file, override_list, val_section)
        all_validations.append(isa_person_validation)

    # Validate Protocols
    val_section = "protocols"
    if validation_section == 'all' or val_section in validation_section:
        status, amber_warning, isa_protocol_validation = validate_protocols(
            isa_study, validation_schema, inv_file, override_list, val_section)
        all_validations.append(isa_protocol_validation)

    # Validate Samples
    val_section = "samples"
    sample_name_list = []
    if validation_section == 'all' or val_section in validation_section:
        status, amber_warning, isa_sample_validation = \
            validate_samples(isa_study, isa_samples, validation_schema, s_file, override_list,
                             sample_name_list, val_section)
        all_validations.append(isa_sample_validation)

    # Validate files
    val_section = "files"
    file_name_list = []
    if validation_section == 'all' or val_section in validation_section:
        status, amber_warning, files_validation = validate_files(
            study_id, study_location, obfuscation_code, override_list, file_name_list, val_section)
        all_validations.append(files_validation)

    # Validate assays
    val_section = "assays"
    if validation_section == 'all' or val_section in validation_section or 'maf' in validation_section:
        status, amber_warning, assay_validation = \
            validate_assays(isa_study, study_location, validation_schema, override_list, sample_name_list,
                            file_name_list, val_section)
        all_validations.append(assay_validation)

    if not status:
        error_found = True
    if amber_warning:
        warning_found = True

    if error_found:
        return {"validation": {"study_validation_status": error, "validations": all_validations}}

    if warning_found:
        return {"validation": {"study_validation_status": warning, "validations": all_validations}}

    return {"validation": {"study_validation_status": success, "validations": all_validations}}


def get_assay_column_validations(validation_schema, a_header):
    validation_schema = get_protocol_assay_rules(validation_schema, a_header)
    validate_column = False
    required_column = False
    val_descr = None

    if a_header.lower() == 'sample name':
        validate_column = True
        required_column = True

    if validation_schema and a_header.lower() != 'sample name':
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


def check_assay_columns(a_header, all_assays, row, validations, val_section, assay, unique_file_names, all_assay_names):
    # Correct sample names?
    if a_header.lower() == 'sample name':
        all_assays.append(row)
        if row in sample_name_list:
            add_msg(validations, val_section, "Sample name '" + row + "' found in sample sheet",
                    success, assay.filename)
        else:
            add_msg(validations, val_section, "Sample name '" + row + "' not found in sample sheet",
                    success, meta_file=assay.filename,
                    desrc="Please create the sample in the sample sheet first")
    elif a_header.endswith(' File'):  # files exists?
        file_and_column = row + '|' + a_header
        if file_and_column not in unique_file_names:
            if row != "":  # Do not add a section if a column does not list files
                unique_file_names.append(file_and_column)
    elif a_header.endswith(' Assay Name'):  # MS or NMR assay names are used in the MAF
        if row not in all_assay_names:
            if len(row) >= 1:
                all_assay_names.append(row)

    return all_assays, all_assay_names, validations, unique_file_names


def validate_assays(isa_study, study_location, validation_schema, override_list, sample_name_list, file_name_list, val_section="assays"):
    validations = []
    assays = []
    all_assays = []
    all_assay_names = []
    unique_file_names = []

    study_id = isa_study.identifier

    if isa_study.assays:
        add_msg(validations, val_section, "Found assay(s) for this study", success, val_section)
    else:
        add_msg(validations, val_section, "Could not find any assays", error, desrc="Add assay(s) to the study")

    for assay in isa_study.assays:
        is_ms = False

        unique_file_names = []
        assay_file_name = os.path.join(study_location, assay.filename)
        try:
            assay_df = read_tsv(assay_file_name)
        except FileNotFoundError:
            abort(400, "The file " + assay_file_name + " was not found")

        assay_type_onto = assay.technology_type
        if assay_type_onto.term == 'mass spectrometry':
            is_ms = True

        assay_header = get_table_header(assay_df, study_id, assay_file_name)
        for header in assay_header:
            if 'Term ' not in header and 'Protocol REF' not in header and 'Unit' not in header:
                assays.append(header)

        # Are the template headers present in the assay
        assay_type = get_assay_type_from_file_name(study_id, assay.filename)
        if assay_type != 'a':  # Not created from the online editor, so we have to skip this validation
            tidy_header_row, tidy_data_row, protocols, assay_desc, assay_data_type, assay_mandatory_type = \
                get_assay_headers_and_protcols(assay_type)
            for template_header in tidy_header_row:
                if template_header not in assay_header:
                    add_msg(validations, val_section,
                            "Assay sheet is missing column '" + template_header + "'", error, assay.filename)

        # Are all relevant rows filled in?
        if not assay_df.empty:
            all_rows = assay_df.shape[0]
            for a_header in assays:
                validate_column, required_column, val_descr = get_assay_column_validations(validation_schema, a_header)
                col_rows = 0  # col_rows = isa_samples[s_header].count()
                try:
                    for row in assay_df[a_header]:
                        validate_column = False
                        if row:
                            col_rows += 1
                        all_assays, all_assay_names, validations, unique_file_names = \
                            check_assay_columns(a_header, all_assays, row, validations, val_section,
                                                assay, unique_file_names, all_assay_names)

                    if (col_rows < all_rows) and validate_column:
                        add_msg(validations, val_section, "Assay sheet column '" + a_header + "' is missing values. " +
                                str(col_rows) + " rows found, but there should be " + str(all_rows),
                                warning, assay.filename)
                    else:
                        add_msg(validations, val_section,
                                "Assay sheet column '" + a_header + "' has correct number of rows",
                                success, assay.filename)
                except:
                    add_msg(validations, val_section,
                            "Assay sheet is missing rows for column '" + a_header + "'", error, assay.filename)

    for sample_name in sample_name_list:
        if sample_name not in all_assays:
            add_msg(validations, val_section, "Sample name '" + sample_name + "' is not used in any assay", info)

    for files in unique_file_names:
        file_name = files.split('|')[0]
        column_name = files.split('|')[1]
        status, file_type, file_description = check_file(files, study_location, file_name_list)
        if status:
            add_msg(validations, val_section, "File '" + file_name + "' found and appears to be correct for column '"
                    + column_name + "'", success, desrc=file_description)
        else:
            add_msg(validations, val_section, "File '" + file_name + "' of type '" + file_type +
                    "' is missing or not correct for column '" + column_name + "'", error, desrc=file_description)

        # Correct MAF?
        if column_name.lower() == 'metabolite assignment file':
            validate_maf(validations, file_name, all_assay_names, study_location, isa_study.identifier, is_ms, sample_name_list)

    return return_validations(val_section, validations, override_list)


def validate_files(study_id, study_location, obfuscation_code, override_list, file_name_list, val_section="files"):
    # check for Publication
    validations = []

    study_files, upload_files, upload_diff, upload_location = \
        get_all_files_from_filesystem(study_id, obfuscation_code, study_location,
                                      directory=None, include_raw_data=True)
    sample_cnt = 0
    for file in study_files:
        file_name = file['file']
        file_type = file['type']
        file_status = file['status']
        isa_tab_warning = False

        full_file_name = os.path.join(study_location, file_name)

        if file_name != 'audit':
            if os.path.isdir(os.path.join(full_file_name)):
                for sub_file_name in os.listdir(full_file_name):
                    if is_empty_file(os.path.join(full_file_name, sub_file_name)):
                        add_msg(validations, val_section, "Empty files found is sub-directory", info, val_section,
                                value=os.path.join(file_name, sub_file_name))

                    # warning for sub folders with ISA tab
                    if sub_file_name.startswith(('i_', 'a_', 's_', 'm_')) and not isa_tab_warning:
                        add_msg(validations, val_section,
                                "Sub-directory " + file_name + " contains ISA-Tab metadata documents",
                                warning, val_section, value=file_name)
                        isa_tab_warning = True

        if file_name.startswith('Icon') or file_name.lower() == 'desktop.ini' or file_name.lower() == '.ds_store' \
                or '~' in file_name or '+' in file_name or file_name.startswith('.'):
            add_msg(validations, val_section, "Special files should be removed from the study folder",
                    warning, val_section, value=file_name)
            continue

        if file_name.startswith(('i_', 'a_', 's_', 'm_')):
            if file_name.startswith('s_') and file_status == 'active':
                sample_cnt += 1

            if sample_cnt > 1:
                add_msg(validations, val_section, "Only one active sample sheet per study is allowed", error,
                        val_section, value='Number of active sample sheets ' + sample_cnt)

            if file_status == 'old':
                add_msg(validations, val_section, "Old metadata file should be removed", warning,
                        val_section, value=file_name)

        if is_empty_file(full_file_name):
            if file_name not in 'metexplore_mapping.json':
                add_msg(validations, val_section, "Empty files are not allowed", error, val_section, value=file_name)

        file_name_list.append(file_name)

    return return_validations(val_section, validations, override_list)


def validate_samples(isa_study, isa_samples, validation_schema, file_name, override_list, sample_name_list, val_section="samples"):
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
                    success, file_name)
        else:
            if sample_val_name == 'Characteristics[Variant]':  # Not all studies have this present
                add_msg(validations, val_section, "Sample column '" + sample_val_name + "' was not found", info,
                        file_name)
                continue
            add_msg(validations, val_section, "Sample column '" + sample_val_name + "' was not found", error, file_name)
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
                if row:
                    col_rows += 1
                if s_header == 'Characteristics[Organism]':
                    if 'human' == row.lower() or 'man' == row.lower():
                        human_found = True
                    elif len(row) <= 5:  # ToDo, read from all_val[idx][ontology-details][rules][0][value]
                        too_short = True

                    if row.lower() in incorrect_species:
                        add_msg(validations, val_section,
                                "Organism can not be '" + row + "', choose the appropriate taxonomy term",
                                error, file_name)

                    if ':' in row:
                        add_msg(validations, val_section,
                                "Organism should not contain the actual ontology/taxonomy name, "
                                "please include just the appropriate taxonomy term",
                                warning, file_name)

                elif s_header.lower() == 'sample name':
                    if len(row) >= 1:
                        sample_name_list.append(row)

            if col_rows < all_rows:
                add_msg(validations, val_section, "Sample sheet column '" + s_header + "' is missing values. " +
                        str(col_rows) + " rows found, but there should be " + str(all_rows), error, file_name)
            else:
                add_msg(validations, val_section, "Sample sheet column '" + s_header + "' has correct number of rows",
                        success, file_name)

    if human_found:
        add_msg(validations, val_section,
                "Organism can not be 'human' or 'man', please choose the 'Homo sapiens' taxonomy term",
                error, file_name)
    if too_short:
        add_msg(validations, val_section, "Organism name is too short (>=5 characters)", error, file_name)

    return return_validations(val_section, validations, override_list)


def validate_protocols(isa_study, validation_schema, file_name, override_list, val_section="protocols"):
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
        isa_prot_name = ""
        try:
            isa_prot = isa_study.protocols[idx]
            isa_prot_name = isa_prot.name

            if prot_val_name != isa_prot_name:
                add_msg(validations, val_section, "Protocol '" + isa_prot_name + "' is not in the correct position",
                        warning, file_name)
            else:
                add_msg(validations, val_section, "Protocol '" + isa_prot_name + "' is in the correct position",
                        success, file_name)
        except:
            add_msg(validations, val_section, "Protocol '" + prot_val_name + "' was not found", error, file_name)

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
                        error, file_name, value=prot_desc)
            else:
                add_msg(validations, val_section, "Protocol description only contains printable characters",
                        success, file_name, value=prot_desc)

            if len(prot_name) >= name_val_len:
                add_msg(validations, val_section, "Protocol name validates", success, file_name, value=prot_name)
            else:
                add_msg(validations, val_section, prot_name + ": " + name_val_error, error, file_name, value=prot_name,
                        desrc=name_val_description)

            if len(prot_desc) >= desc_val_len:
                if prot_desc == 'Please update this protocol description':
                    add_msg(validations, "Protocol", prot_name + ": " + desc_val_error, warning, file_name,
                            value=prot_desc, desrc='Please update this protocol description')
                add_msg(validations, val_section, "Protocol description validates", success, file_name, value=prot_desc)
            else:
                if 'no metabolites' in prot_desc.lower():
                    add_msg(validations, val_section, "Protocol description validates", success, file_name,
                            value=prot_desc)
                else:
                    add_msg(validations, val_section, prot_name + ": " + desc_val_error, error, file_name,
                            value=prot_desc, desrc=desc_val_description)

            if len(prot_params.term) >= param_val_len:
                add_msg(validations, val_section, "Protocol parameter validates", success, file_name,
                        value=prot_params.term)
            else:
                add_msg(validations, val_section, prot_name + ": " + param_val_error, error, file_name,
                        value=prot_params.term, desrc=param_val_description)

    return return_validations(val_section, validations, override_list)


def validate_contacts(isa_study, validation_schema, file_name, override_list, val_section="people"):
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
                if len(last_name) >= last_name_val_len:
                    add_msg(validations, val_section, "Persons last name validates", success, file_name)
                else:
                    add_msg(validations, val_section, last_name_val_error, error, file_name, value=last_name,
                            desrc=last_name_val_description)

            if first_name:
                if len(first_name) >= first_name_val_len:
                    add_msg(validations, val_section, "Persons first name validates", success, file_name)
                else:
                    add_msg(validations, val_section, first_name_val_error, error, file_name, value=first_name,
                            desrc=first_name_val_description)

            if email:
                if len(email) >= email_val_len:
                    add_msg(validations, val_section, "Persons email validates", success, file_name)
                else:
                    add_msg(validations, val_section, email_val_error, error, file_name, value=email,
                            desrc=email_val_error)

            if affiliation:
                if len(affiliation) >= affiliation_val_len:
                    add_msg(validations, val_section, "Persons affiliation validates", success, file_name)
                else:
                    add_msg(validations, val_section, affiliation_val_error, error, file_name, value=affiliation,
                            desrc=affiliation_val_error)

    return return_validations(val_section, validations, override_list)


def check_doi(pub_doi, doi_val):
    # doi = pub_doi
    # doi_pattern = re.compile(doi_val) # Todo, fix pattern
    # doi_check = doi_pattern.match(doi)
    # retun doi_check

    if 'http' in pub_doi or 'doi.org' in pub_doi:
        return False

    return True


def validate_publication(isa_study, validation_schema, file_name, override_list, val_section="publication"):
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
                add_msg(validations, val_section, title_val_error, error, file_name)
            else:
                add_msg(validations, val_section, "Found a publication", success, file_name)

            if not publication.title:
                add_msg(validations, val_section, title_val_error, error, file_name)
            elif publication.title:
                if len(publication.title) >= title_val_len:
                    add_msg(validations, val_section, "Found the title of the publication", success, file_name)
                else:
                    add_msg(validations, val_section, title_val_error, warning,
                            file_name, value=publication.title, desrc=title_val_description)

            if not publication.doi:
                add_msg(validations, val_section, doi_val_description, warning, file_name)
                doi = False
            elif publication.doi:
                if check_doi(publication.doi, doi_val):
                    add_msg(validations, val_section, "Found the doi for the publication", success, file_name)
                    doi = True
                else:
                    add_msg(validations, val_section, doi_val_error, warning, file_name, doi)
                    doi = False

            if not publication.pubmed_id:
                add_msg(validations, val_section, pmid_val_description, warning, file_name)
                pmid = False
            elif publication.pubmed_id:
                try:
                    int(publication.pubmed_id)
                except ValueError:
                    add_msg(validations, val_section, pmid_val_error, error, file_name,
                            value=publication.pubmed_id, desrc=pmid_val_description)

                if len(publication.pubmed_id) >= int(pmid_val_len):
                    add_msg(validations, val_section, "Found the pmid for the publication", success, file_name)
                    pmid = True
                else:
                    add_msg(validations, val_section, pmid_val_error, error, file_name,
                            value=publication.pubmed_id, desrc=pmid_val_description)
                    pmid = False

            if not doi or not pmid:
                add_msg(validations, val_section,
                        "Please provide both a valid doi and pmid for the publication", warning, file_name)
            elif doi and pmid:
                add_msg(validations, val_section,
                        "Found both doi and pmid for the publication", success, file_name)

            if not publication.author_list:
                add_msg(validations, val_section, author_val_error, error, file_name)
            elif publication.author_list:
                if len(publication.author_list) >= author_val_len:
                    add_msg(validations, val_section, "Found the author list for the publication", success, file_name)
                else:
                    add_msg(validations, val_section, author_val_error, error, file_name,
                            value=publication.author_list, desrc=author_val_description)

            if not publication.status:
                add_msg(validations, val_section, "Please provide the publication status", error, file_name)
            elif publication.status:
                pub_status = publication.status
                if len(pub_status.term) >= status_val_len:
                    add_msg(validations, val_section, "Found the publication status", success, file_name)
                else:
                    add_msg(validations, val_section, status_val_error, success, file_name,
                            value=pub_status.title, desrc=status_val_description)
    else:
        add_msg(validations, val_section, title_val_error, error)

    return return_validations(val_section, validations, override_list)


def validate_basic_isa_tab(study_id, user_token, study_location, override_list):
    validates = True
    amber_warning = False
    validations = []
    val_section = "basic"
    file_name = 'i_Investigation.txt'

    try:
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)
        try:
            file_name = isa_study.filename
            isa_sample_df = read_tsv(os.path.join(study_location, file_name))
        except FileNotFoundError:
            abort(400, "The file " + file_name + " was not found")

    except ValueError:
        err = traceback.format_exc()
        add_msg(validations, val_section,
                "Loading ISA-Tab without sample and assay tables. "
                "Protocol parameters does not match the protocol definition", warning, file_name)
        logger.error("Cannot load ISA-Tab with sample and assay tables due to critical error: " + err)

    if isa_inv:
        add_msg(validations, val_section, "Successfully read the i_Investigation.txt files", success, file_name)

        if isa_study:
            add_msg(validations, val_section, "Successfully read the study section of the investigation file", success,
                    file_name)
        else:
            add_msg(validations, val_section, "Can not read the study section of the investigation file", error,
                    file_name)
            validates = False

        if isa_study.filename:
            add_msg(validations, val_section, "Successfully found the reference to the sample sheet filename", success,
                    file_name)
        else:
            add_msg(validations, val_section, "Could not find the reference to the sample sheet filename", error,
                    file_name)
            validates = False

        # isaconfig
        if isa_inv.get_comment('Created With Configuration'):
            create_config = isa_inv.get_comment('Created With Configuration')
            open_config = None
            if isa_inv.get_comment('Last Opened With Configuration'):
                open_config = isa_inv.get_comment('Last Opened With Configuration')

            if 'isaconfig' in create_config.value:
                add_msg(validations, val_section, "Incorrect configuration files used to create the study ("
                        + create_config.value + "). The study may not contain required fields", warning, file_name)
                amber_warning = True
            if 'isaconfig' in open_config.value:
                add_msg(validations, val_section, "Incorrect configuration files used to edit the study ("
                        + open_config.value + "). The study may not contain required fields", warning, file_name)
                amber_warning = True

        if validates:  # Have to have a basic investigation and sample file before we can continue
            if isa_study.samples:
                add_msg(validations, val_section, "Successfully found one or more samples", success, file_name)
            elif not isa_sample_df.empty:
                add_msg(validations, val_section, "Successfully found one or more samples", success, file_name)
            else:
                add_msg(validations, val_section, "Could not find any samples", error, file_name)

            if isa_study.assays:
                add_msg(validations, val_section, "Successfully found one or more assays", success, file_name)
            else:
                add_msg(validations, val_section, "Could not find any assays", error, file_name)

            if isa_study.factors:
                add_msg(validations, val_section, "Successfully found one or more factors", success, file_name)
            else:
                add_msg(validations, val_section, "Could not find any factors", warning, file_name)

            if isa_study.design_descriptors:
                add_msg(validations, val_section, "Successfully found one or more descriptors", success, file_name)
            else:
                add_msg(validations, val_section, "Could not find any study design descriptors", error, file_name)

    else:
        add_msg(validations, "ISA-Tab", "Can not find or read the investigation files", error, file_name)

    validates, amber_warning, ret_list = return_validations(val_section, validations, override_list)

    inv_file = 'i_Investigation.txt'
    s_file = isa_study.filename
    assays = isa_study.assays
    assay_files = []
    for assay in assays:
        assay_files.append(assay.filename)

    return isa_study, isa_inv, isa_sample_df, std_path, validates, amber_warning, \
            ret_list, inv_file, s_file, assay_files


def validate_isa_tab_metadata(isa_inv, isa_study, validation_schema, file_name, override_list,
                              val_section="isa-tab_metadata"):
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
            add_msg(validations, val_section, "The title length validates", success, file_name)
        else:
            add_msg(validations, val_section, val_error, error, file_name, value=isa_study.title, desrc=title_descr)

        # Description
        val_len, val_error, val_condition, val_type = extract_details(desc_rules)
        try:
            descr_len = len(isa_study.description)
        except:
            descr_len = 0

        if descr_len >= val_len:
            add_msg(validations, val_section, "The length of the description validates", success, file_name)
        else:
            add_msg(validations, val_section, val_error, error, file_name,
                    value=isa_study.description, desrc=desc_desrc)

    else:
        add_msg(validations, val_section, "Can not find or read the investigation files", error)

    return return_validations(val_section, validations, override_list)
