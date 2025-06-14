#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Jan-09
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

import datetime
import glob
import json
import logging
import os
import re
import subprocess
import time
import traceback
from typing import List, OrderedDict, Union

import numpy as np
import pandas as pd
import requests
from isatools import model

from app.config import get_settings
from app.config.model.study import StudySettings
from app.utils import current_time
from app.ws.cluster_jobs import submit_job
from app.ws.db.schemes import Study
from app.ws.db_connection import update_validation_status
from app.ws.isaApiClient import IsaApiClient
from app.ws.settings.utils import get_study_settings
from app.ws.study.folder_utils import get_files_for_validation
from app.ws.study.study_service import StudyService
from app.ws.study_folder_utils import FileSearchResult
from app.ws.utils import (find_text_in_isatab_file,
                          get_assay_headers_and_protocols,
                          get_assay_type_from_file_name, get_table_header,
                          map_file_type, read_tsv)
iac = IsaApiClient()

logger = logging.getLogger('wslog')
factor_value_pattern = r"(\bFactor Value\b)\[\b(.+)\b\](\.\d)?"
parameter_value_pattern = r"(\bParameter Value\b)\[\b(.+)\b\](\.\d)?"

incorrect_species = \
    "cat, dog, mouse, horse, flower, man, fish, leave, root, mice, steam, bacteria, value, food, matix, " \
    "mus, rat, blood, urine, plasma, hair, fur, skin, saliva, fly, unknown"

last_name_black_list = ['last name', 'asdf', 'name', 'unknown']
first_name_black_list = ['first name', 'asdf', 'name', 'unknown']

correct_maf_order = [{0: "database_identifier"}, {1: "chemical_formula"}, {2: "smiles"},
                     {3: "inchi"}, {4: "metabolite_identification"}]

warning = "warning"
error = "error"
success = "success"
info = "info"

unknown_file = ' - unknown - '

fid_file = 'Free Induction Decay Data File'
acq_file = 'Acquisition Parameter Data File'
raw_file = 'Raw Spectral Data File'
derived_file = 'Derived Spectral Data File'
valid_acq_raw_files = ["acqu", "acqus", "acqu2", "acqu2s", "acqus.txt", "acqu2s.txt"]

def add_msg(validations, section, message, status, meta_file="", value="", descr="", val_sequence=0,
            log_category=error):
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


def return_validations(section, validations, override_list=None, comment_list=None):
    # Add the validation sequence
    if not override_list:
        override_list = []

    if not comment_list:
        comment_list = []

    for val in validations:
        # idx += 1  # Set the sequence to 1, as this is the section we will override
        val_sequence = section + '_' + val['val_sequence']
        val["val_sequence"] = val_sequence
        val["val_override"] = 'false'
        val["val_message"] = ''
        val["comment"] = ''

        comment_list = [string for string in comment_list if val_sequence in string]

        if len(override_list) > 0:  # These are from the database, ie. already over-ridden
            try:
                for db_val in override_list:
                    val_step = db_val.split(':')[0]
                    val_msg = db_val.split(':')[1]
                    if val_sequence == val_step or val_step == '*':  # "*" overrides all errors/warning/info etc
                        val_status = val['status']
                        val["val_override"] = 'true'
                        val["val_message"] = val_msg
                        if val_status == warning or val_status == error or val_status == info:
                            val["status"] = success
                        elif val_status == success and val_step != '*':
                            val["status"] = error
            except:
                logger.error('Could not read the validation override list, is the required ":" there?')
        if len(comment_list) > 0:
            for comment in comment_list:
                # we can't assume that we will get only one result, as the list comp would catch both 7.11 and 7.1
                comment_validation_seq = comment.split(':')[0]
                text = comment.split(':')[0]
                if comment_validation_seq == val_sequence:
                    val['comment'] = 'text'

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


def is_empty_file(full_file_name, study_location=None):
    # This will return False if a filename is not correct, ie. has a space etc.
    empty_file = True

    ignore_file_list = get_settings().file_filters.ignore_file_list
    short_file_name = os.path.basename(full_file_name).lower()
    for ignore in ignore_file_list:  # Now there are a bunch of files we want to ignore regardless if they are empty
        if ignore in short_file_name:
            return False

    # The file in the assay may be locally referenced, so check full path. Only for files, not folders
    if not os.path.isfile(full_file_name) and not os.path.isdir(full_file_name):
        f_file = os.path.join(study_location, full_file_name.lstrip('/'))
        if os.path.isfile(f_file):
            full_file_name = f_file
    try:
        file_stats = os.stat(full_file_name)
        file_size = file_stats.st_size
        empty_file = file_size == 0
    except Exception as e:
        logger.error("File '" + full_file_name + "' can not be checked/found. " + str(e))
        return empty_file
    return empty_file


def get_sample_names(isa_samples):
    all_samples = ""
    for sample in isa_samples.samples:
        all_samples = all_samples + sample + ','
    return all_samples


def check_file(
        file_name_and_column, study_location, file_name_list, validations, assay_file_list=None, assay_file_name=None):
    """
    Check an individual file. Performs various checks in sequence:
    1 Whether the given filename is a directory or a file
    2 The type of file it is IE derived, raw etc
    3 Whether the filename given in the assay sheet actually exists in the filesystem. This check will fail in the case
    of trailing whitespace in the assay sheet filename.
    4 Whether the type of file is correct for the given column. IE a raw file won't be accepted in the Derived column.

    :param file_name_and_column: The given filename and assay sheet column it originates from.
    :param study_location: The location in the filesystem of the study folder.
    :param file_name_list: List of all files in the study
    :param assay_file_list: A list of raw files referenced in the assay sheet
    :param assay_file_name: The filename of the parent assay sheet we're checking the file for.
    :return: Status indicating validity as a boolean, the type of file it is, the description of the given file.
    """
    file_name = file_name_and_column.split('|')[0]
    column_name = file_name_and_column.split('|')[1]
    full_file = os.path.join(study_location, file_name)
    short_name, ext = os.path.splitext(file_name)

    ext = ext.lower()

    if assay_file_name:
        assay_file_name = ' (' + assay_file_name + ')'
    else:
        assay_file_name = ''

    if os.path.isdir(full_file) and ext not in ('.raw', '.d'):
        return False, 'folder', file_name + " is a sub-folder, please reference a file" + assay_file_name

    file_type, status, folder = map_file_type(file_name, study_location, assay_file_list=assay_file_list)

    # define some generic return validation messages
    valid_message = True, file_type, 'Correct file ' + file_name + ' for column ' + column_name
    invalid_message = False, file_type, 'Incorrect file "' + file_name + '" or file type for column ' + column_name + assay_file_name

    # if not folder and "fid" not in file_name and final_filename.lstrip('/') not in file_name_list:  # Files may be referenced in sub-folders
    if not folder and file_name not in file_name_list and file_name.lstrip(
            '/') not in file_name_list:  # was final_filename
        msg = "File '" + file_name + "' does not exist" + assay_file_name
        if file_name != file_name.rstrip(' '):
            msg = msg + ". Trailing space in file name?"
        return False, unknown_file, msg

    if is_empty_file(full_file, study_location=study_location):
        return False, file_type, "File '" + file_name + "' is empty or incorrect" + assay_file_name

    if file_type == 'metadata_maf' and column_name == 'Metabolite Assignment File':
        if file_name.startswith('m_') and file_name.endswith('_v2_maf.tsv'):
            return valid_message
        else:
            return False, file_type, "The " + column_name + \
                   " must start with 'm_' and end in '_v2_maf.tsv'" + assay_file_name

    if (file_type == 'raw' or file_type == 'compressed') and column_name == raw_file:
        return valid_message
    elif file_type in ('derived', 'raw', 'compressed') and (column_name == derived_file or column_name == raw_file):
        return valid_message
    elif file_type == 'text' and column_name == derived_file:
        if textfile_valid_for_derived_column(validations, file_name):
            return valid_message
        else:
            return invalid_message
    elif file_type == 'spreadsheet' and column_name == derived_file:
        return valid_message
    elif file_type != 'derived' and column_name == derived_file:
        return invalid_message
    elif file_type == 'compressed' and column_name == fid_file:
        return valid_message
    elif file_type == 'folder' and column_name == fid_file:
        return valid_message
    elif file_type in ('compressed', 'acqus') and column_name == acq_file:
        return valid_message
    elif file_type == 'fid' and column_name == fid_file:
        return valid_message
    elif file_type != 'raw' and column_name == raw_file:
        return invalid_message

    return status, file_type, 'n/a'


def textfile_valid_for_derived_column(validations, filename) -> bool:
    """
    Check whether we want to allow a text file in the derived column for a given instance.
    We only want to allow this if there is a *valid* raw spectral data file entry in the same row.
    There is a success validation message that indicates that such a situation has occurred, and to what raw file and
    derived file pairing. We look for such a message that has our derived filename in it, and if we find it, return true
    and if we don't, return false.
    :param validations: List of dicts that represent validation messages.
    :param filename: Filename to check as a string.
    :return: bool value indicating validity.
    """
    for validation in validations:
        if validation['val_sequence'] == "7.11" and validation["message"].endswith(filename):
            return True

    return False


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
                 sample_name_list, is_ms=False, log_category=error, assay_type=None):
    val_section = "maf"

    if not file_name.startswith('m_') or not file_name.endswith('_v2_maf.tsv'):
        add_msg(validations, val_section,
                "The Metabolite Annotation File name must start with 'm_' and end in '_v2_maf.tsv'",
                error, val_sequence=12, log_category=log_category)

    maf_name = os.path.join(study_location, file_name)
    maf_df = pd.DataFrame()

    if len(file_name) == 0:
        add_msg(validations, val_section, "Please add a Metabolite Annotation File name '" + file_name + "'",
                error, val_sequence=1, log_category=log_category)

    try:
        maf_df = read_tsv(maf_name)
    except:
        add_msg(validations, val_section, "Could not find or read Metabolite Annotation File '" + file_name + "'",
                error, val_sequence=2, log_category=log_category)

    maf_shape = maf_df.shape
    all_rows = maf_shape[0]
    all_columns = maf_shape[1]
    if all_rows == 1:
        for index, row in maf_df.iterrows():
            db_id_value = row["database_identifier"]
            cf_value = row["chemical_formula"]
            if row["database_identifier"] == '0' and not row["chemical_formula"]:
                add_msg(validations, val_section,
                        "Incomplete Metabolite Annotation File '" + file_name + "'",
                        error, descr="Please complete the MAF", val_sequence=3, log_category=log_category)

    empty_maf_rows = False
    # replace field that's entirely space (or empty) with NaN for isnull() function
    temp_df = maf_df.replace(r'^\s*$', np.nan, regex=True)
    for i in range(len(temp_df.index)):
        empty_cells_per_row = temp_df.iloc[i].isnull().sum()
        if all_columns == empty_cells_per_row:
            empty_maf_rows = True
            break
    temp_df = None  # No need to keep this copy in memory any more

    if empty_maf_rows:
        add_msg(validations, val_section, "MAF '" + file_name + "' contains empty rows",
                error, val_sequence=13, log_category=log_category)

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
            add_msg(validations, val_section, incorrect_message, error, val_sequence=4, log_category=log_category)
        else:
            add_msg(validations, val_section,
                    "Columns 'database_identifier', 'chemical_formula', 'smiles', 'inchi' and "
                    "'metabolite_identification' found in the correct column position in '" + file_name + "'",
                    success, val_sequence=4.2, log_category=log_category)

        try:
            if is_ms and ("mass_to_charge" in maf_header):
                check_maf_rows(validations, val_section, maf_df, 'mass_to_charge', is_ms=is_ms,
                               log_category=log_category, file_name=file_name)
        except:
            logger.info("No mass_to_charge column found in the MS MAF")
        try:
            if assay_type in {"LC-MS", "GC-MS"} and "retention_time" in maf_header:
                check_maf_rows(validations, val_section, maf_df, 'retention_time', is_ms=is_ms,
                               log_category=log_category, file_name=file_name)
        except:
            logger.info("No retention_time column found in the MS MAF")
            
        try:
            if not is_ms and "chemical_shift" in maf_header:
                check_maf_rows(validations, val_section, maf_df, 'chemical_shift', is_ms=is_ms,
                               log_category=log_category, file_name=file_name)
        except:
            logger.info("No chemical_shift column found in the NMR MAF")

        # NMR/MS Assay Names OR Sample Names are added to the sheet
        missing_maf_columns = []
        if all_assay_names:
            for assay_name in all_assay_names:
                try:
                    maf_header[assay_name]
                    # add_msg(validations, val_section, "MS/NMR Assay Name '" + assay_name + "' found in the MAF",
                    #         success, val_sequence=5, log_category=log_category)
                    check_maf_rows(validations, val_section, maf_df, assay_name, is_ms=is_ms, log_category=log_category)
                except KeyError as e:
                    missing_maf_columns.append(assay_name)
            if missing_maf_columns:
                add_msg(validations, val_section, "MS/NMR Assay Name '" + ','.join(missing_maf_columns) + "' not found in the MAF",
                    warning, val_sequence=6, log_category=log_category)

        if not all_assay_names and sample_name_list:
            missing_maf_columns = []
            for sample_name in sample_name_list:
                try:
                    maf_header[sample_name]
                    # add_msg(validations, val_section, "Sample Name '" + str(sample_name) + "' found in the MAF",
                    #         success, val_sequence=7, log_category=log_category)
                    check_maf_rows(validations, val_section, maf_df, sample_name, is_ms=is_ms,
                                   log_category=log_category)
                except:
                    missing_maf_columns.append(sample_name)
            if missing_maf_columns:
                add_msg(validations, val_section, "Sample Name '" + ', '.join(missing_maf_columns) + "' not found in the MAF",
                    warning, val_sequence=8, log_category=log_category)

    else:
        add_msg(validations, val_section, "MAF '" + file_name + "' is empty. Please add metabolite annotation details",
                error, val_sequence=9, log_category=log_category)


def check_maf_rows(validations, val_section, maf_df, column_name, is_ms=False, log_category=error, file_name=""):
    all_rows = maf_df.shape[0]
    col_rows = 0
    # Are all relevant rows filled in?
    for row in maf_df[column_name]:
        if row and row.strip():
            col_rows += 1

    # if col_rows == all_rows:
    # add_msg(validations, val_section, "All values for column '" + column_name + "' found in the MAF",
    #         success, val_sequence=9.1, log_category=log_category)
    # else:
    if col_rows != all_rows:
        # For MS we should have m/z values, for NMR the chemical shift is equally important.
        if (is_ms and column_name in ('mass_to_charge', 'retention_time')) or (not is_ms and column_name == 'chemical_shift'):
            add_msg(validations, val_section, "Missing values for '" + column_name + "' in the MAF file " + file_name + ". " + 
                    str(col_rows) + " row(s) found, but there should be " + str(all_rows),
                    error, val_sequence=10, log_category=log_category)
        else:
            add_msg(validations, val_section, "Missing values for sample '" + column_name + "' in the MAF file " + file_name + ". " + 
                    str(col_rows) + " row(s) found, but there should be " + str(all_rows),
                    info, val_sequence=11, log_category=log_category)




def update_validation_schema_files(validation_file, study_id, user_token, obfuscation_code,
                            log_category='all', study_settings: Union[None, StudySettings] = None):
    settings = get_settings()
    if not study_settings:
        study_settings = settings.study
        
    internal_files_folder = os.path.join(study_settings.mounted_paths.study_internal_files_root_path, study_id)
    readonly_files_folder = os.path.join(study_settings.mounted_paths.study_readonly_files_actual_root_path, study_id)
    metadata_files_folder = os.path.join(study_settings.mounted_paths.study_metadata_files_root_path, study_id)
    # Tidy up old files first
    validation_files_path = os.path.join(internal_files_folder, study_settings.validation_files_json_name)
    # if os.path.isfile(validation_files_path):
    #     os.remove(validation_files_path)

    # if os.path.isfile(validation_file):
    #     os.remove(validation_file)
    # try:
    #     with open(validation_file, 'w', encoding='utf-8') as f:
    #         # json.dump(validation_schema, f, ensure_ascii=False, indent=4)
    #         json.dump(ValidationReportFile(), f, ensure_ascii=False)
    # except Exception as e:
    #     logger.error('Error writing validation schema file: ' + str(e))

    f_start_time = time.time()
    
    search_result = get_files_for_validation(study_id, metadata_files_folder)
    for item in search_result.study:
        item.file = item.relative_path
    result = search_result.model_dump(include={"study"})
    try:
        with open(validation_files_path, 'w', encoding='utf-8') as f1:
            json.dump(result, f1, ensure_ascii=False)
    except Exception as e1:
        logger.error('Error Writing validation file list: ' + str(e1))

    logger.info(study_id + " - Generating validations file list took %s seconds" % round(time.time() - f_start_time, 2))

    v_start_time = time.time()
    validation_schema = validate_study(study_id, metadata_files_folder, user_token, obfuscation_code,
                                       log_category=log_category, static_validation_file=validation_files_path)
    # if log_category == 'all':  # Only write the complete file, not when we have a sub-section only query
    try:
        with open(validation_file, 'w', encoding='utf-8') as f:
            # json.dump(validation_schema, f, ensure_ascii=False, indent=4)
            json.dump(validation_schema, f, ensure_ascii=False)
        # read to ensure 
        with open(validation_file, 'r', encoding='utf-8') as f:
            result = json.load(f)
    except Exception as e:
        logger.error('Error writing validation schema file: ' + str(e))
        return {"status": "failed", "file": validation_file, "message": "File write task was failed"}
    v_end_time = time.time() 
    time_elipsed = round(v_end_time - v_start_time, 2)
    logger.info(study_id + " - Generating validations list took %s seconds" % time_elipsed)

    return {"status": "completed", "file": validation_file, "message": None}


def get_last_update_on_folder(study_location):
    need_validation_update = True
    list_of_files = glob.glob(os.path.join(study_location, '*'))
    latest_file = max(list_of_files, key=os.path.getmtime)
    return os.path.getmtime(latest_file)
    if 'validation_' in latest_file:
        need_validation_update = False  # No files modified since the validation schema files
    return need_validation_update


def validate_study(study_id, study_location_old, user_token, obfuscation_code,
                   validation_section='all', log_category='all', static_validation_file=None, settings: Union[None, StudySettings] = None):
    """
    Entry point method for validating an entire study. Each section is validated in turn, unless a validation section is
    specified in the request parameters.

    :param study_id: ID of the study to be validated. This is the accession number IE MTBLS1234.
    :param study_location: The location in the filesystem of the study folder.
    :param user_token: The users API token, used to ascertain whether the user is permitted to take this action.
    :param obfuscation_code: The obfuscation code, which points to the ftp folder for the study.
    :param validation_section: The section of the study to validate. The distinct sections are: publication, isatab,
    person, protocols, samples, files, assays. The default is 'all', which means all sections will be validated.
    :param log_category: The type of log IE Info, warning or error. Default to 'all'.
    :param static_validation_file: Flag to indicate whether to read the validation report and file list from
     pre generated files. This is often set to true for large studies.
     :return: An object comprised of all validation messages.

    """
    start_time = time.time()
    last_update_time = current_time()
    last_update_timestamp = int(last_update_time.timestamp())
    last_update_time_str = last_update_time.strftime('%Y-%m-%d-%H:%M')
    all_validations = []
    validation_schema = None
    error_found = False
    warning_found = False
    validation_section = validation_section.lower()
    if not settings:
        settings = get_study_settings()
    internal_files_folder = os.path.join(settings.mounted_paths.study_internal_files_root_path, study_id)
    readonly_files_folder = os.path.join(settings.mounted_paths.study_readonly_files_actual_root_path, study_id)
    metadata_files_folder = os.path.join(settings.mounted_paths.study_metadata_files_root_path, study_id)

    
    
    isa_date_format = "%Y-%m-%d"
    study: Study = StudyService.get_instance().get_study_by_acc(study_id)
    db_release_date = study.releasedate.strftime(isa_date_format)
    try:
        validation_schema_file = get_settings().file_resources.validations_file

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
    comment_list = []
    try:
        override = study.override
        if override:
            override_list.extend(override.split('|'))
    except Exception as e:
        logger.error('Could not query overridden validations from the database. ' + str(e))


    try:
        comment = study.comment
        if comment:
            comment_list.extend(comment.split('|'))
    except IndexError as e:
        logger.error(f'Unable to retrieve comment list from database: {str(e)}')

    # Validate basic ISA-Tab structure
    isa_study, isa_inv, isa_samples, std_path, status, amber_warning, isa_validation, inv_file, s_file, assay_files = \
        validate_basic_isa_tab(
            study_id, user_token, metadata_files_folder, db_release_date, override_list, comment_list, log_category=log_category)
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
            isa_study, validation_schema, inv_file, override_list, comment_list, val_section, log_category=log_category)
        all_validations.append(pub_validation)

    if not status:
        error_found = True
    if amber_warning:
        warning_found = True

    # Validate detailed metadata in ISA-Tab structure
    val_section = "isa-tab"
    if validation_section == 'all' or val_section in validation_section:
        status, amber_warning, isa_meta_validation = validate_isa_tab_metadata(
            isa_inv, isa_study, validation_schema, inv_file, override_list, comment_list, val_section, log_category=log_category)
        all_validations.append(isa_meta_validation)

    if not status:
        error_found = True
    if amber_warning:
        warning_found = True

    # Validate Person (authors)
    val_section = "person"
    if isa_study and validation_section == 'all' or val_section in validation_section:
        status, amber_warning, isa_person_validation = validate_contacts(
            isa_study, validation_schema, inv_file, override_list, comment_list, val_section, log_category=log_category)
        all_validations.append(isa_person_validation)

    if not status:
        error_found = True
    if amber_warning:
        warning_found = True

    # Validate Protocols
    val_section = "protocols"
    if isa_study and validation_section == 'all' or val_section in validation_section:
        status, amber_warning, isa_protocol_validation = validate_protocols(
            isa_study, validation_schema, inv_file, override_list, comment_list, val_section, log_category=log_category)
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
            validate_samples(isa_study, isa_samples, validation_schema, s_file, override_list, comment_list,
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
            study_id, readonly_files_folder, metadata_files_folder, obfuscation_code, override_list, comment_list,
            file_name_list, val_section, log_category=log_category, static_validation_file=static_validation_file)
        all_validations.append(files_validation)

    if not status:
        error_found = True
    if amber_warning:
        warning_found = True

    # Validate assays
    val_section = "assays"
    if isa_study and validation_section == 'all' or val_section in validation_section or 'maf' in validation_section:
        status, amber_warning, assay_validation = \
            validate_assays(isa_study, readonly_files_folder, metadata_files_folder, internal_files_folder, validation_schema, override_list, comment_list, sample_name_list,
                            file_name_list, val_section, log_category=log_category)
        all_validations.append(assay_validation)

    if not status:
        error_found = True
    if amber_warning:
        warning_found = True
    
    if error_found:
        update_validation_status(study_id=study_id, validation_status=error)
        end_time = round(time.time() - start_time, 2)
        return {"validation": {"status": error, "timing": end_time, "last_update_time": last_update_time_str, "last_update_timestamp": last_update_timestamp,  "validations": all_validations}}

    if warning_found:
        update_validation_status(study_id=study_id, validation_status=warning)
        end_time = round(time.time() - start_time, 2)
        return {"validation": {"status": warning, "timing": end_time, "last_update_time": last_update_time_str, "last_update_timestamp": last_update_timestamp, "validations": all_validations}}

    update_validation_status(study_id=study_id, validation_status=success)
    end_time = round(time.time() - start_time, 2)
    return {"validation": {"status": success, "timing": end_time, "last_update_time": last_update_time_str, "last_update_timestamp": last_update_timestamp, "validations": all_validations}}


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
                        all_assay_names, sample_name_list, log_category=error, assay_file_name=None):
    # Correct sample names?
    if a_header.lower() == 'sample name':
        if row not in all_samples:
            all_samples.append(row)
        # if row in sample_name_list:
        # add_msg(validations, val_section, "Sample name '" + row + "' found in sample sheet",
        #         success, assay.filename, val_sequence=7, log_category=log_category)
        # else:
        if row not in sample_name_list:
            if len(row) == 0:
                add_msg(validations, val_section, "Sample name '" + row + "' cannot be empty",
                        error, meta_file=assay.filename, descr="Please add a valid sample name",
                        val_sequence=8, log_category=log_category)
            else:
                add_msg(validations, val_section, "Sample name '" + row + "' not found in sample sheet",
                        error, meta_file=assay.filename,
                        descr="Please create the sample in the sample sheet first",
                        val_sequence=9, log_category=log_category)
    elif a_header.endswith(' File'):  # files exist?
        file_and_column = row + '|' + a_header
        if assay_file_name:
            file_and_column = file_and_column + '|' + assay_file_name
        if file_and_column not in unique_file_names:
            if row != "":  # Do not add a section if a column does not list files
                if file_and_column not in unique_file_names:
                    unique_file_names.append(file_and_column)
    elif a_header.endswith(' Assay Name'):  # MS or NMR assay names are used in the assay
        row = str(row)
        if row not in all_assay_names:
            if len(row) >= 1:
                all_assay_names.append(row)

    return all_samples, all_assay_names, validations, unique_file_names


def check_all_file_rows(assays, assay_dataframe, validations, val_section, filename, total_rows, log_category=error):
    """
    Check that the values in the Raw Spectral Data File & Derived Spectral Data Column are valid.
    We want to check the filetypes of each value, as there are restrictions on what kind of file can be present
    in each column.

    :param assays: a list of assay sheet headers present in the file.
    :param assay_dataframe: Two dimensional data structure containing the assay sheet data.
    :param validations: List of pre-existing validation objects and their constituent validation messages.
    :param val_section: Predefined in parent method as 'assays'
    :param filename: String representation of the assay sheet's filename.
    :param total_rows: The total number of rows
    :param log_category: Predefined as 'error'
    :return: Tuple of the updated validations object now containing assay validations and list of all raw files
     referenced in the assay.

    """
    settings = get_settings()
    valid_derived_filetypes = [x.lower() for x in settings.file_filters.derived_files_list]
    valid_raw_filetypes = [x.lower() for x in settings.file_filters.raw_files_list]
    
    all_file_columns = []
    missing_all_rows = []
    all_assay_raw_files = []
    all_assay_derived_files = []
    for assay_header in assays:
        assay_header = str(assay_header)
        if assay_header.endswith(' Data File'):
            all_file_columns.append(assay_header)
            empty_rows = (assay_dataframe[assay_header].values == '').sum()
            if empty_rows == total_rows:
                missing_all_rows.append(assay_header)

    if derived_file in missing_all_rows:
        if raw_file in missing_all_rows:
            # OK, all raw/derived files are missing, no point in looking at these anymore
            all_file_columns.remove(derived_file)
            missing_all_rows.remove(derived_file)
            all_file_columns.remove(raw_file)
            missing_all_rows.remove(raw_file)
            add_msg(validations, val_section,
                    f"'{filename}': All Raw and Derived Spectral Data Files are missing from assay",
                    error, filename, val_sequence=7.6, log_category=log_category)
        if fid_file in missing_all_rows:
            if acq_file in missing_all_rows:
                all_file_columns.remove(acq_file)
                missing_all_rows.remove(acq_file)
                all_file_columns.remove(fid_file)
                missing_all_rows.remove(fid_file)
                add_msg(validations, val_section,
                        f"'{filename}': All " + derived_file + "s, " + fid_file
                        + "s and " + acq_file + "s are missing from assay",
                        error, filename, val_sequence=7.7, log_category=log_category)

    if all_file_columns:
        short_dataframe = assay_dataframe[assay_dataframe.columns.intersection(all_file_columns)]
        for idx, row in short_dataframe.iterrows():
            row_idx = str(idx + 1)
            raw_found = False
            raw_tested = False
            raw_valid = False
            raw_value = None

            derived_found = False
            derived_tested = False
            derived_valid = {
                'value': None,
                'valid': False,
                'is_text_file': False
            }
            fid_file_value = None
            aqc_file_value = None
            nmr_data_row = False
            derived_data_value = None
            for header, value in row.items():  # Check cells
                if header == raw_file:
                    raw_tested = True
                    if value:
                        all_assay_raw_files.append(value)
                        raw_found = True
                        raw_valid = is_valid_raw_file_column_entry(value, valid_filetypes=valid_raw_filetypes)
                        raw_value = value
                elif header == derived_file:
                    derived_tested = True
                    if value:
                        all_assay_derived_files.append(value)
                        derived_data_value= value
                        derived_found = True
                        derived_valid = is_valid_derived_column_entry(value, valid_filetypes=valid_derived_filetypes)
                elif header == fid_file:
                    if value and os.path.basename(value).lower() in ("fid", "ser") or value.endswith(".zip"):
                        fid_file_value = value
                        all_assay_raw_files.append(fid_file_value)
                    elif value:
                        add_msg(validations, val_section,
                            f"'{filename}': {fid_file} value at  {str(row_idx)} row is invalid. " + 
                            f"Current value '{value if value else ''}'. " + 
                            f"Relative path of fid, ser or a compressed raw data (*.zip) file is expected.",
                            error, filename, val_sequence=7.21, log_category=log_category)
                    nmr_data_row = True
                elif header == acq_file:
                    if  value and value.endswith(".zip") or os.path.basename(value) in valid_acq_raw_files:
                        aqc_file_value = value
                        all_assay_raw_files.append(aqc_file_value)
                    else:
                        add_msg(validations, val_section,
                            f"'{filename}': {acq_file} value at  {str(row_idx)} row is invalid. Current value '{value}'. " +
                            f"Relative path of acqu, acqus, acqu2 or acqu2s or a compressed raw data (*.zip) file is expected.",
                            error, filename, val_sequence=7.22, log_category=log_category)
                    nmr_data_row = True
                else:
                    if not value:
                        val_type = error
                        if acq_file in header or fid_file in header:
                            val_type = warning

                        add_msg(validations, val_section, header + f" was not referenced in {filename} assay row {str(row_idx)}",
                                val_type, filename, val_sequence=7.5, log_category=log_category)
            if nmr_data_row:
                if not derived_found or not derived_valid['valid']:
                    if not fid_file_value:
                        add_msg(validations, val_section,
                            f"'{filename}': If there is no valid derived data file in {derived_file} column, " + 
                            f"{fid_file} column value is required and should be valid at {str(row_idx)}. row. " + 
                            f"Current values: {derived_file}:'{derived_data_value if derived_data_value else ''}', " + 
                            f"{fid_file}:'{fid_file_value if fid_file_value else ''}', ",
                            error, filename, val_sequence=7.24, log_category=log_category)                        
            elif derived_tested and raw_tested:
                if not raw_found and not derived_found:
                    add_msg(validations, val_section,
                            f"'{filename}': Both Raw and Derived Spectral Data Files are missing from assay row {str(row_idx)}",
                            error, filename, val_sequence=7.1, log_category=log_category)
                if not raw_valid and derived_valid['is_text_file'] is True:
                    add_msg(validations, val_section,
                            "Valid raw file missing from row {0} where a text file is present in the derived column"
                            .format(row_idx), error, filename, val_sequence=7.8, log_category=log_category)
                if raw_valid and derived_found and derived_valid['valid'] is not True:
                    add_msg(validations, val_section,
                            f"'{filename}': Derived Spectral Data Column entry missing or invalid for row {str(row_idx)}",
                            error, filename, val_sequence=7.9, log_category=log_category)
                if raw_value and derived_valid['value'] and derived_valid['is_text_file']:
                    add_msg(validations, val_section,
                           f"'{filename}' Raw Spectral Data File {str(raw_value)} has a corresponding text file entry in the Derived Spectral Data Column: {derived_valid['value']}", success, filename,
                            val_sequence=7.11,
                            log_category=log_category)

    return validations, all_assay_raw_files


def is_valid_raw_file_column_entry(value: str, valid_filetypes: List[str]=None, exact_match: bool=False) -> bool:
    """
    Checks whether the given value for the raw file is valid. Iterates over the list of valid filetypes, and if the
    value string contains a valid filetype, the loop breaks and returns true.

    :param value: Raw Data File Column entry as a string
    :return: bool value indicating whether the entry is valid.

    """
    if not valid_filetypes:
        valid_filetypes = [x.lower() for x in get_settings().file_filters.raw_files_list]
    for filetype in valid_filetypes:
        if exact_match:
            if value.lower() == filetype.lower():
                return True
        else:
            if value.lower().endswith(filetype) and len(value) > len(filetype):
                return True
    return False


def is_valid_derived_column_entry(value: str, valid_filetypes: List[str]=None) -> dict:
    """
    Checks whether the given value for the Derived Spectral Data File column is valid.Iterates over the list of valid
    filetypes, and if the value string contains a valid filetype, the loop breaks and returns true. It also checks
    whether the file is a text file, as this is only acceptable in certain conditions.

    :param value: The derived spectral data column value to validate.
    :return: dict object indicating validity and whether the value is a text file.
    """
    result_dict = {
        'value': value,
        'valid': False,
        'is_text_file': False
    }
    if not valid_filetypes:
        valid_filetypes = [x.lower() for x in get_settings().file_filters.derived_files_list]
    valid_filetypes.append('.txt')
    for filetype in valid_filetypes:
        if value.lower().endswith(filetype):
            result_dict['valid'] = True
            if filetype == '.txt':
                result_dict['is_text_file'] = True
            break

    return result_dict


def validate_assays(isa_study, readonly_files_folder, metadata_files_folder, internal_files_folder, validation_schema, override_list, comment_list, sample_name_list,
                    file_name_list, val_section="assays", log_category=error):
    validations = []
    all_assay_samples = []
    unique_file_names = []
    missing_or_incorrect_files = []

    study_id = isa_study.identifier
    settings = get_study_settings()
    if isa_study.assays:
        add_msg(validations, val_section, "Found assay(s) for this study", success, val_section,
                val_sequence=1, log_category=log_category)
    else:
        add_msg(validations, val_section, "Could not find any assays", error, descr="Add assay(s) to the study",
                val_sequence=2, log_category=log_category)

    total_assay_rows = 0

    for item in isa_study.assays:
        assay: model.Assay = item
        is_ms = False
        assays = []
        all_assay_names = []
        all_assay_raw_files = []
        assay_file_name = os.path.join(metadata_files_folder, assay.filename)
        assay_dataframe = None
        try:
            assay_dataframe = read_tsv(assay_file_name)
        except FileNotFoundError:
            add_msg(validations, val_section,
                    "The file " + assay_file_name + " was not found",
                    error, assay.filename, val_sequence=2.1, log_category=log_category)
            continue
        
        assay_type_onto = assay.technology_type
        if assay_type_onto.term.lower().startswith('mass spectrometry'):
            is_ms = True

        assay_header = get_table_header(assay_dataframe, study_id, assay_file_name)
        required_columns = {"chromatography instrument", 'column model', "column type"}
        found_columns = {}
        for header in assay_header:
            if len(header) == 0:
                add_msg(validations, val_section,
                        "Assay sheet '" + assay.filename + "' has empty column header(s)",
                        error, assay.filename, val_sequence=2.11, log_category=log_category)

            if 'Term ' not in header and 'Protocol REF' not in header and 'Unit' not in header:
                assays.append(header)
            if len(assay_dataframe) > 1 and header.startswith("Parameter Value["):
                match = re.match(parameter_value_pattern, header)
                if match:
                    param = match.groups()[1]
                    if param.lower() in required_columns:
                        found_columns[header] = param
        if len(found_columns) == len(required_columns):
            for header, param in found_columns.items():
                values = [str(x) for x in assay_dataframe[header] if str(x) and str(x).strip()]
                if len(values) < len(assay_dataframe):
                    add_msg(validations, val_section, 
                            "Assay sheet '" + str(assay.filename) + f" Parameter Value[{param}] has empty rows.", 
                            error, val_sequence=20.1, log_category=log_category)

        if len(assay_dataframe) <= 1:
            add_msg(validations, val_section, "Assay sheet '" + str(
                assay.filename) + " contains Only 1 sample, please ensure you have included all samples and any control, QC, standards etc. If no further samples were used in the study please contact MetaboLights-help.",
                    error, val_sequence=2.12, log_category=log_category)

        # Are the template headers present in the assay
        assay_type = get_assay_type_from_file_name(study_id, assay.filename)
        if assay_type != 'a':  # Not created from the online editor, so we have to skip this validation
            tidy_header_row, tidy_data_row, protocols, assay_desc, assay_data_type, assay_file_type, \
            assay_mandatory_type = get_assay_headers_and_protocols(assay_type)
            for idx, template_header in enumerate(tidy_header_row):

                assay_header_pos = None
                for idx, key in enumerate(assay_header):
                    if key == template_header:
                        assay_header_pos = idx  # template_header[idx]
                        break

                if idx != assay_header_pos:
                    add_msg(validations, val_section,
                            "Assay sheet '" + assay.filename + "' column '" + template_header + "' is not in the correct position for assay type " + assay_type,
                            info, assay.filename, val_sequence=2.2, log_category=log_category)
                else:
                    add_msg(validations, val_section,
                            "Assay sheet '" + assay.filename + "' column '" + template_header + "' is in the correct position for assay type " + assay_type,
                            success, assay.filename, val_sequence=2.21, log_category=log_category)

                if template_header not in assay_header:
                    msg_type = error
                    if template_header in ('Parameter Value[Guard column]', 'Parameter Value[Autosampler model]'):
                        msg_type = info

                    add_msg(validations, val_section,
                            "Assay sheet '" + assay.filename + "' is missing column '" + template_header + "'",
                            msg_type, assay.filename, val_sequence=3, log_category=log_category)

        # Are all relevant rows filled in?
        if not assay_dataframe.empty:
            all_rows = assay_dataframe.shape[0]
            total_assay_rows = total_assay_rows + all_rows
            for a_header in assays:
                a_header = str(a_header)  # Names like '1' and '2', gets interpreted as '1.0' and '2.0'
                validate_column, required_column, val_descr = get_assay_column_validations(validation_schema, a_header)
                col_rows = 0  # col_rows = isa_samples[s_header].count()
                try:
                    if validate_column:
                        for row in assay_dataframe[a_header]:
                            if row:
                                col_rows += 1

                            all_sample_names, all_assay_names, validations, unique_file_names = \
                                check_assay_columns(a_header, all_assay_samples, row, validations, val_section,
                                                    assay, unique_file_names, all_assay_names,
                                                    sample_name_list, log_category=log_category,
                                                    assay_file_name=assay_file_name.replace(metadata_files_folder + '/', ''))

                        if col_rows > len(all_assay_samples):
                            add_msg(validations, val_section,
                                    "Sample names should ideally be unique for assay sheet '" + assay.filename + "'",
                                    warning, assay.filename, val_sequence=10, log_category=log_category)

                        if col_rows < all_rows:

                            if required_column:
                                val_type = error
                            else:
                                val_type = warning

                            if 'factor value' in a_header.lower():  # User defined factors may not all have data in all rows
                                val_type = info

                            if col_rows == 0:
                                add_msg(validations, val_section,
                                        "Assay sheet '" + assay.filename + "' column '" + a_header + "' is empty",
                                        val_type, assay.filename, val_sequence=4, log_category=log_category)
                            else:
                                add_msg(validations,
                                        val_section,
                                        "Assay sheet '" + assay.filename + "' column '" + a_header + "' has missing some values. " +
                                        str(col_rows) + " row(s) found, but there should be " + str(all_rows),
                                        val_type, assay.filename, val_sequence=4.1, log_category=log_category)

                        # Correct MAF?
                        if a_header.lower() == 'metabolite assignment file':
                            maf_file_name = None
                            for row in assay_dataframe[a_header].unique():
                                maf_file_name = row
                                break  # We only need one row

                            if maf_file_name:
                                validate_maf(validations, maf_file_name, all_assay_names, metadata_files_folder,
                                             isa_study.identifier,
                                             sample_name_list, is_ms=is_ms, log_category=log_category, assay_type=assay_type)
                            else:
                                add_msg(validations, val_section,
                                        "No MAF/feature file referenced for assay sheet " + assay.filename + ". Please add the appropriate file reference (m_xxx.tsv) to the last column of the assay table. If the metabolite file is missing or your study does not include metabolite/feature identification information, please contact metabolights-help@ebi.ac.uk",
                                        error,
                                        val_sequence=7.4, log_category=log_category)

                except Exception as e:
                    add_msg(validations, val_section,
                            "Assay sheet '" + assay.filename + "' is missing rows for column '" + a_header + "' " +
                            str(e), error, assay.filename, val_sequence=6, log_category=log_category)

            # We validate all file columns separately here
            validations, all_assay_raw_files = check_all_file_rows(assays, assay_dataframe, validations, val_section,
                                                                   assay.filename, all_rows, log_category=log_category)

            if all_assay_names:
                if len(all_assay_names) < all_rows:
                    add_msg(validations, val_section, "MS/NMR Assay name column should only contain unique values",
                            warning, assay.filename, val_sequence=4.11, log_category=log_category)
                else:
                    add_msg(validations, val_section, "MS/NMR Assay name column only contains unique values",
                            success, assay.filename, val_sequence=4.12, log_category=log_category)

    missing_samples = []
    for sample_name in sample_name_list:  # Loop all unique sample names from sample sheet
        if sample_name not in all_assay_samples:
            missing_samples.append(sample_name)
    if missing_samples:
        sample_names = ",".join(missing_samples)
        if len(missing_samples) == 1:
            add_msg(validations, val_section, "Sample name '" + str(sample_names) + "' is not used in any assay",
                    error, val_sequence=7.1, log_category=log_category)
        else:
            add_msg(validations, val_section, "Sample names '" + str(sample_names) + "' are not used in any assay",
                    error, val_sequence=7.2, log_category=log_category)


    sample_len = len(sample_name_list)
    if total_assay_rows < sample_len:
        add_msg(validations, val_section, "There are more unique sample rows (" + str(sample_len)
                + ") than unique assay rows (" + str(total_assay_rows) + "), must be the same or more",
                error, val_sequence=8, log_category=log_category)

    for files in unique_file_names:
        # Validate each file referenced in the assay sheet individually to make sure it is the correct type /
        # has the correct content.
        file_name = files.split('|')[0]
        column_name = files.split('|')[1]
        try:
            a_file_name = files.split('|')[2]
        except IndexError as e:
            logger.warning('Unable to find assay file name in string {0} : {1}'.format(files, e))
            a_file_name = None
        valid, file_type, file_description = check_file(files, metadata_files_folder, file_name_list, validations,
                                                        assay_file_list=all_assay_raw_files,
                                                        assay_file_name=a_file_name)
        if not valid:
            missing_or_incorrect_files.append(file_name + ' ({0}) [{1}]'
                                              .format(column_name, (a_file_name if a_file_name is not None else '')))
            err_msg = "File '" + file_name + "'"
            if file_type != unknown_file:
                err_msg = err_msg + " of type '" + file_type + "'"

            err_msg = err_msg + " is missing or not correct for column '" + column_name + "'"
            if a_file_name:
                err_msg = err_msg + " (" + a_file_name + ")"
            add_msg(validations, val_section, err_msg, error, descr=file_description,
                    val_sequence=9, log_category=log_category)
        else:
            add_msg(validations, val_section, "File '" + file_name + "' found and appears to be correct for column '"
                    + column_name + "'", success, descr=file_description, val_sequence=8.1, log_category=log_category)
    if len(missing_or_incorrect_files) > 0:
        try:
            with open( os.path.join(internal_files_folder, settings.missing_files_name), 'w', encoding='utf-8') as file:
                file.write("\n".join(missing_or_incorrect_files))
        except Exception as e:
            logger.error(f'Error writing missing file {str(e)}')
    else:
        missing_file_path = os.path.join(internal_files_folder, settings.missing_files_name)
        
        if os.path.exists(missing_file_path):
            os.remove(missing_file_path)
    return return_validations(val_section, validations, override_list, comment_list)


def get_files_in_sub_folders(study_location):
    folder_list = []
    file_list = []
    folder_exclusion_list = get_settings().file_filters.folder_exclusion_list

    for file_name in os.listdir(study_location):
        # for file_name in file_list:
        if os.path.isdir(os.path.join(study_location, file_name)):
            fname, ext = os.path.splitext(file_name)
            ext = ext.lower()
            if ext:
                if ext not in folder_exclusion_list:
                    if file_name not in folder_list:
                        folder_list.append(file_name)
            else:
                if file_name.lower() not in folder_exclusion_list and file_name.lower() not in folder_list:
                    folder_list.append(file_name)

    # file_folder_list = []
    # for folder in folder_list:
    #     full_folder = os.path.join(study_location, folder)
    #     for file_name in os.listdir(full_folder):
    #         file_folder = os.path.join(folder, file_name)
    #         if file_folder not in file_folder_list:
    #             file_folder_list.append(file_folder)

    return folder_list


def validate_files(study_id, readonly_files_folder, metadata_files_folder, obfuscation_code, override_list, comment_list, file_name_list,
                   val_section="files", log_category=error, static_validation_file=None):
    validations = []
    
    settings = get_study_settings()
    # folder_list = get_files_in_sub_folders(study_location)
    search_result: FileSearchResult = get_files_for_validation(study_id, metadata_files_folder)
    study_files = search_result.study 
    sample_cnt = 0
    raw_file_found = False
    derived_file_found = False
    compressed_found = False
    ignore_file_set = set(get_settings().file_filters.ignore_file_list)
    for file in study_files:
        file_name = file.relative_path
        file_type = file.type
        file_status = file.status
        isa_tab_warning = False
        
        full_file_name = os.path.join(metadata_files_folder, file_name)

        # Don't check our internal folders
        if not file_name.startswith(settings.internal_files_symbolic_link_name) and not file_name.startswith(settings.audit_files_symbolic_link_name):
            if file.file != file.relative_path and file.file.startswith(('i_', 'a_', 's_', 'm_')) and not isa_tab_warning:
                add_msg(validations, val_section,
                        "ISA-Tab metadata document " + file_name + " is detected in subfolder.",
                        warning, val_section, value=file_name, val_sequence=2, log_category=log_category)
                isa_tab_warning = True
            if file.directory and file.is_stop_folder and file.status == "active":
                for sub_file_name in os.listdir(full_file_name):
                    if is_empty_file(os.path.join(full_file_name, sub_file_name), study_location=metadata_files_folder):
                        add_msg(validations, val_section, "Empty file is found in a sub-directory", info, val_section,
                                value=os.path.join(file_name, sub_file_name), val_sequence=1, log_category=log_category)

            if file.file not in ignore_file_set  and not file.directory and file.is_empty:
                # if '/' in file_name and file_name.split("/")[1].lower() not in empty_exclusion_list:  # In case the file is in a folder
                add_msg(validations, val_section, "Empty files are not allowed: '" + file_name + "'",
                        error, val_section,
                        value=file_name, val_sequence=6, log_category=log_category)

            if file_name.startswith('Icon') or file_name.lower() == 'desktop.ini' or file_name.lower() == '.ds_store' \
                    or '~' in file_name or file_name.startswith('.'):  # "or '+' in file_name" taken out
                add_msg(validations, val_section, "Special files should be removed from the study folder",
                        warning, val_section, value=file_name, val_sequence=3, log_category=log_category)
                continue

            if file_name.startswith(('i_', 'a_', 's_', 'm_')):
                if file_status != 'active':
                    add_msg(validations, val_section, "Inactive ISA-Tab metadata file should be removed ("
                            + file_name + ")", error, val_section, value=file_name,
                            val_sequence=5.1, log_category=log_category)

                if file_name.startswith(('i_', 'a_', 's_')) and not file_name.endswith('.txt'):
                    add_msg(validations, val_section, "ISA-Tab metadata file should have .txt extension ("
                            + file_name + ")", error, val_section, value=file_name,
                            val_sequence=11, log_category=log_category)

                if file_name.startswith('s_') and file_status == 'active':
                    sample_cnt += 1

                if sample_cnt > 1:
                    add_msg(validations, val_section, "Only one active sample sheet per study is allowed", error,
                            val_section, value='Number of active sample sheets ' + str(sample_cnt),
                            val_sequence=4, log_category=log_category)

                if file_status == 'old':
                    add_msg(validations, val_section, "Old ISA-Tab metadata file should be removed ("
                            + file_name + ")", error, val_section, value=file_name, val_sequence=5,
                            log_category=log_category)

            if file_type == 'aspera-control':
                add_msg(validations, val_section,
                        "Incomplete Aspera transfer? '.partial', '.aspera-ckpt' or '.aspx' Aspera control files "
                        "are present in the study folder: '" + file_name + "'",
                        error, val_section, value=file_name, val_sequence=6.1, log_category=log_category)

        if file_type in ('raw', 'fid', 'acqus', 'd'):  # Raw for MS and fid/acqus for NMR
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
                    value="", val_sequence=7.1, log_category=log_category)
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

    return return_validations(val_section, validations, override_list, comment_list)


def validate_samples(isa_study, isa_samples, validation_schema, file_name, override_list, comment_list, sample_name_list,
                     val_section="samples", log_category=error):
    # check for Publication
    validations = []
    samples = []
    sample_coll_found = True
    prot_ref = 'protocol ref'
    if validation_schema:
        study_val = validation_schema['study']
        val = study_val['samples']
        all_val = val['default_order']
    # check isa_sample frame size - if size is 1 return  below error -    
    if len(isa_samples) <= 1:
        add_msg(validations, val_section,
                "Only 1 sample has been added to your study, please ensure you have included all samples and any control, QC, standards etc. If no further samples were used in the study please contact MetaboLights-help",
                error,
                file_name, val_sequence=1, log_category=log_category)
    # Get an indexed header row
    s_file_name = isa_study.filename
    sample_header = get_table_header(isa_samples, isa_study.identifier, s_file_name)

    study: model.Study = isa_study
    study_factors: List[str] = {x.name for x in study.factors}
    factor_column_pattern = r"^Factor Value\[(.+)\].*$"
    factor_columns = OrderedDict()
    for h_sample in sample_header:
        if 'Term ' not in h_sample:
            samples.append(h_sample)
            factor_match = re.match(factor_column_pattern, h_sample)
            if factor_match:
                factor_columns[factor_match.group(1)] = h_sample
    sample_factor_columns = set(factor_columns.keys())
    missing_columns = study_factors - sample_factor_columns
    if missing_columns:
        if len(missing_columns) == 1:
            add_msg(validations, val_section, "Some factors are defined in study, but " + ", ".join([f"'Factor Value[{x}]'" for x in missing_columns]) +" factor column is missing in sample sheet respectively.",
                    error, file_name, val_sequence=30.1, log_category=log_category)
        else:
            add_msg(validations, val_section, "Some factors are defined in study, but " + ", ".join([f"'Factor Value[{x}]'" for x in missing_columns]) +" factor columns are missing in sample sheet respectively.",
                    error, file_name, val_sequence=30.1, log_category=log_category)            
    for idx, sample in enumerate(all_val):
        sample_val_name = sample['header']
        if sample_val_name in samples:
            if sample_val_name == 'Protocol REF':  # Don't need to output this column name
                continue
            add_msg(validations, val_section, "Sample column '" + sample_val_name + "' found in the sample file",
                    success, file_name, val_sequence=1.1, log_category=log_category)
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
            row_values = set()
            for row in isa_samples[s_header]:
                if str(row):  # Float values with 0.0 are not counted, so convert to string first
                    col_rows += 1
                if row and row.strip() and s_header.lower().startswith('factor value['):
                    row_values.add(row.strip())
                elif s_header == 'Characteristics[Organism]':
                    if 'human' == row.lower() or 'man' == row.lower():
                        human_found = True
                    elif len(row) < 4:  # ToDo, read from all_val[idx][ontology-details][rules][0][value]
                        too_short = True

                    if row.lower() in incorrect_species:
                        add_msg(validations, val_section,
                                "Organism cannot be '" + row + "', choose the appropriate taxonomy term",
                                error, file_name, val_sequence=4.3, log_category=log_category)

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

                elif s_header.lower() == prot_ref:
                    if row:
                        row = str(row)
                        if row != "Sample collection":
                            sample_coll_found = False
            if not row_values and s_header.lower().startswith('factor value['):
                add_msg(validations, val_section, "Sample sheet column '" + s_header + "' is empty. At least one row should be filled.", error, file_name,
                        val_sequence=20.1, log_category=log_category)
            if not sample_coll_found and s_header.lower() == prot_ref:
                add_msg(validations, val_section, "Sample sheet column '" + s_header + "' has missing required values. " 
                        + "All rows must contain the text 'Sample collection'",
                        error, file_name, val_sequence=7.8, log_category=log_category)

            if col_rows < all_rows:
                val_stat = error

                # These are new columns we like to see, but not mandatory yet
                if s_header in ('Characteristics[Variant]', 'Characteristics[Sample type]'):
                    val_stat = info

                if 'factor value' in s_header.lower():  # User defined factors may not all have data in all rows
                    val_stat = info

                add_msg(validations, val_section, "Sample sheet column '" + s_header + "' has missing values. " +
                        str(col_rows) + " row(s) found, but there should be " + str(all_rows), val_stat, file_name,
                        val_sequence=6, log_category=log_category)
            else:
                add_msg(validations, val_section, "Sample sheet column '" + s_header + "' has correct number of rows",
                        success, file_name, val_sequence=7, log_category=log_category)

        if sample_name_list:
            if len(sample_name_list) != all_rows:
                add_msg(validations, val_section, "Sample name column should ideally only contain unique values",
                        info, file_name, val_sequence=4, log_category=log_category)
            else:
                add_msg(validations, val_section, "Sample name column contains unique values",
                        success, file_name, val_sequence=4.1, log_category=log_category)

    if human_found:
        add_msg(validations, val_section,
                "Organism cannot be 'human' or 'man', please choose the 'Homo sapiens' taxonomy term",
                error, file_name, val_sequence=8, log_category=log_category)
    if too_short:
        add_msg(validations, val_section, "Organism name is missing or too short (<4 characters)", error, file_name,
                val_sequence=9, log_category=log_category)

    return return_validations(val_section, validations, override_list, comment_list)


def validate_protocols(isa_study: model.Study, validation_schema, file_name, override_list, comment_list, val_section="protocols",
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
            if not assay_type_onto:
                add_msg(validations, val_section, "Assay has no valid technology type definition",
                            warning, file_name, val_sequence=20, log_category=log_category)
            elif assay_type_onto.term:
                term_type = assay_type_onto.term if assay_type_onto.term else ""
                if not term_type:              
                    add_msg(validations, val_section, "Assay has no valid technology type definition",
                            warning, file_name, val_sequence=20, log_category=log_category)
                if term_type.lower().startswith('mass spectrometry'):
                    is_ms = True
                elif term_type.lower().startswith('nmr spectroscopy'):
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
    all_prots = ""
    for idx, protocol in enumerate(default_prots):
        prot_val_name = protocol['title']
        if all_prots:
            all_prots = all_prots + ", " + prot_val_name
        else:
            all_prots = prot_val_name
        try:
            isa_prot: model.Protocol = isa_study.protocols[idx]
            isa_prot_name = isa_prot.name
            isa_prot_type = isa_prot.protocol_type
            isa_prot_type_name = isa_prot_type.term

            if isa_prot_name != isa_prot_type_name:
                add_msg(validations, val_section, "Protocol '" + isa_prot_name +
                        "' does not match the protocol type name '" + isa_prot_type_name + "'",
                        warning, file_name, val_sequence=1, log_category=log_category)
            else:
                add_msg(validations, val_section, "Protocol '" + isa_prot_name + "' match the protocol type definition",
                        success, file_name, val_sequence=1.1, log_category=log_category)

            if prot_val_name != isa_prot_name:
                add_msg(validations, val_section, "Protocol '" + isa_prot_name +
                        "' is not in the correct position or name has different case/spelling. Expected '" +
                        prot_val_name + "'", warning, file_name, val_sequence=2, log_category=log_category)
            else:
                add_msg(validations, val_section, "Protocol '" + isa_prot_name +
                        "' is in the correct position and name has correct case/spelling",
                        success, file_name, val_sequence=2.1, log_category=log_category)
        except IndexError:
            add_msg(validations, val_section, "Could not find all required protocols '" + all_prots + "' for " +
                    term_type, error, file_name, val_sequence=3, log_category=log_category)

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

                sentence2 = None
                reg_ex = re.search(r"(\.\s\w+)", prot_desc)
                if reg_ex:
                    try:
                        sentence2 = reg_ex[1]  # get the 2nd sentence in the paragraph
                    except IndexError:
                        sentence2 = None
                if not sentence2:  # More than one line in the description?
                    add_msg(validations, "Protocol", prot_name + " description should be more than just one sentence",
                            warning, file_name, value=prot_desc,
                            descr='Please update this protocol description to contain more than one sentence',
                            val_sequence=8.1, log_category=log_category)

                add_msg(validations, val_section, "Protocol description validates", success, file_name,
                        value=prot_desc, val_sequence=9, log_category=log_category)
            else:
                if prot_desc.lower().rstrip('.') in ('no metabolites', 'not applicable',
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

    return return_validations(val_section, validations, override_list, comment_list)


def validate_contacts(isa_study, validation_schema, file_name, override_list, comment_list, val_section="person", log_category=error):
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

    return return_validations(val_section, validations, override_list, comment_list)


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


def validate_publication(isa_study, validation_schema, file_name, override_list, comment_list, val_section="publication",
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

            # if not doi or not pmid:
            #     add_msg(validations, val_section,
            #             "Please provide both a valid DOI and PubMedID for the publication",
            #             warning, file_name, val_sequence=13, log_category=log_category)
            # elif doi and pmid:
            #     add_msg(validations, val_section,
            #             "Found both DOI and PubMedID for the publication", success, file_name,
            #             val_sequence=14, log_category=log_category)

            if not publication.author_list:
                add_msg(validations, val_section, author_val_error, error, file_name,
                        val_sequence=15, log_category=log_category)
            elif publication.author_list:
                if len(publication.author_list) >= 1:  # author_val_len
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
                    add_msg(validations, val_section, status_val_error, error, file_name,
                            value=pub_status.term, descr=status_val_description,
                            val_sequence=20, log_category=log_category)
    else:
        add_msg(validations, val_section, title_val_error, error, val_sequence=21, log_category=log_category)

    return return_validations(val_section, validations, override_list, comment_list)


def validate_basic_isa_tab(study_id, user_token, study_location, release_date, override_list, comment_list, log_category=error):
    validates = True
    amber_warning = False
    validations = []
    val_section = "basic"
    # inv_file_format = re.compile(r'i_(.*?)\.txt')
    isa_inv = None
    isa_study = None
    isa_sample_df = None
    std_path = None
    s_file = None
    assay_files = None
    settings = get_study_settings()
    inv_file_name = settings.investigation_file_name
    try:

        if os.path.isfile(os.path.join(study_location, inv_file_name)):
            try:
                isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                                 skip_load_tables=True,
                                                                 study_location=study_location)

                file_name = isa_study.filename
                isa_sample_df = read_tsv(os.path.join(study_location, file_name))
            except FileNotFoundError:
                add_msg(validations, val_section, "The file '" + file_name + "' was not found", error,
                        inv_file_name, val_sequence=1.1, log_category=log_category)
            except Exception as e:
                add_msg(validations, val_section, "Could not load the minimum ISA-Tab files (generic reading error).",
                        error, inv_file_name, val_sequence=1.11, log_category=log_category)
        else:
            add_msg(validations, val_section, "Could not load the minimum ISA-Tab files. Investigation file missing?",
                    error, inv_file_name, val_sequence=1.2, log_category=log_category)

    except ValueError:
        err = traceback.format_exc()
        add_msg(validations, val_section,
                "Loading ISA-Tab without sample and assay tables. "
                "Protocol parameters does not match the protocol definition",
                warning, file_name, val_sequence=1, log_category=log_category)
        logger.error("Cannot load ISA-Tab with sample and assay tables due to critical error: " + err)

    if isa_inv:
        add_msg(validations, val_section, "Successfully read the investigation file", success,
                inv_file_name, val_sequence=2, log_category=log_category)

        study_num = 0
        if isa_inv.studies:
            study_num = len(isa_inv.studies)
            if study_num > 1:
                add_msg(validations, val_section,
                        "You can only submit one study per submission, this submission has " + str(
                            study_num) + " studies",
                        error, inv_file_name, val_sequence=2.1, log_category=log_category)

        if isa_study and study_num == 1:
            add_msg(validations, val_section, "Successfully read the study section of the investigation file", success,
                    inv_file_name, val_sequence=3, log_category=log_category)
        else:
            add_msg(validations, val_section, "Could not correctly read the study section of the investigation file",
                    error,
                    inv_file_name, val_sequence=4, log_category=log_category)
            validates = False

        if isa_study.filename:
            add_msg(validations, val_section, "Successfully found the reference to the sample sheet filename", success,
                    inv_file_name, val_sequence=5, log_category=log_category)
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
            elif len(isa_sample_df) != 0 and not isa_sample_df.empty:
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
                add_msg(validations, val_section, "Could not find any factor. Please define at least one factor value in sample sheet.",
                        error, file_name, val_sequence=15, log_category=log_category)

            if isa_study.design_descriptors:
                if len(isa_study.design_descriptors) < 3:
                    add_msg(validations, val_section, "Less than 3 Study design descriptors. Please define at least 3 design descriptors.", error, file_name,
                            val_sequence=16.2, log_category=log_category)
                else:
                    add_msg(validations, val_section, "Successfully found 3 or more descriptors", success, file_name,
                            val_sequence=16, log_category=log_category)
            else:
                add_msg(validations, val_section, "Could not find any study design descriptor",
                        error, file_name, val_sequence=17, log_category=log_category)

            if find_text_in_isatab_file(study_location, 'Thesaurus.owl#'):
                # The hash in an ontology URL will cause problems for the ISA-API
                add_msg(validations, val_section,
                        "URL's containing # will not load properly, please change to '%23'",
                        warning, inv_file_name, val_sequence=17.1, log_category=log_category)

            if isa_study.public_release_date:
                public_release_date = isa_study.public_release_date
                if public_release_date != release_date:
                    add_msg(validations, val_section,
                            "The public release date in the investigation file " +
                            public_release_date + " is not the same as the database release date " +
                            release_date if release_date else "", warning, file_name, val_sequence=19.2, log_category=log_category)
            else:
                add_msg(validations, val_section, "Could not find the public release date in the investigation file",
                        warning, file_name, val_sequence=19.1, log_category=log_category)
    else:
        add_msg(validations, "ISA-Tab", "Could not find or read the investigation file",
                error, inv_file_name, val_sequence=18, log_category=log_category)

    validates, amber_warning, ret_list = return_validations(val_section, validations, override_list, comment_list)


    if isa_study:
        s_file = isa_study.filename
        assays = isa_study.assays
        assay_files = []
        for assay in assays:
            assay_files.append(assay.filename)

    return isa_study, isa_inv, isa_sample_df, std_path, validates, amber_warning, \
           ret_list, inv_file_name, s_file, assay_files


def validate_isa_tab_metadata(isa_inv, isa_study, validation_schema, file_name, override_list, comment_list,
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
        add_msg(validations, val_section, "Could not find or read the investigation file", error,
                val_sequence=5, log_category=log_category)

    return return_validations(val_section, validations, override_list, comment_list)


def job_status(job_id):
    cmd = "/usr/bin/ssh ebi-cli bjobs " + str(job_id).strip()
    logger.info(cmd)
    result = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True, check=True)
    logger.info(result)
    result = result.stdout.decode("utf-8")
    index = result.find("tc_cm01")
    result = result[index + 8:].lstrip().split(' ')
    return result[0]


def submitJobToCluser(command, section, study_internal_files_folder):
    logger.info("Starting cluster job for Validation : " + command)
    status, message, job_out, job_err, log_file = submit_job(True, account=None, job_cmd=command, job_params=None, log=False)

    if status:
        start = 'Job <'
        end = '> is'
        cron_job_id = (job_out[job_out.find("Job <") + len(start):job_out.rfind(end)])
        cron_job_file = os.path.join(study_internal_files_folder, "validation_" + section + "_" + cron_job_id + '.json')
        with open(cron_job_file, 'w') as fp:
            pass
        os.chmod(cron_job_file, 0o777)
        return {"success": message, "job_id": cron_job_id, "message": job_out, "errors": job_err}
    else:
        return {"error": message, "message": job_out, "errors": job_err}


def is_newer_timestamp(location, fileToCompare):
    need_validation_update = False
    try:
        list_of_files = glob.glob(os.path.join(location, '*'))
        latest_file = max(list_of_files, key=os.path.getctime)
    except:
        return need_validation_update
    updateTime = os.path.getctime(latest_file)
    if os.path.getctime(fileToCompare) > updateTime:
        need_validation_update = True  # No files modified since the validation schema files
    return need_validation_update
