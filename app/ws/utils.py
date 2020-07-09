#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Feb-13
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

import base64
import datetime
import glob
import io
import logging
import os
import os.path
import random
import re
import shutil
import string
import time
import uuid
from os.path import normpath, basename
import json
import numpy as np
import pandas as pd
import requests
from flask import current_app as app, request, abort
from isatools.model import Protocol, ProtocolParameter, OntologySource
from lxml import etree
from mzml2isa.parsing import convert as isa_convert
import ast

from app.ws.mm_models import OntologyAnnotation

"""
Utils

Misc of utils
"""

logger = logging.getLogger('wslog')

date_format = "%Y%m%d%H%M%S"  # 20180724092134
file_date_format = "%B %d %Y %H:%M:%S"  # 20180724092134
isa_date_format = "%Y-%m-%d"


def check_user_token(user_token):
    if not user_token or user_token is None or len(user_token) < 5:
        return False
    return True


def get_timestamp():
    """
    Get a string with the current date & time as a timestamp i.e. 20170302143045
    :return: %Y%m%d%H%M%S - full year, two digit month, day, 24 hour, minutes and seconds
    """
    return time.strftime(date_format)


def get_year_plus_one(todays_date=False, isa_format=False):
    """
    Get a string with the current date 20170302
    :return: %Y%m%d - full year, two digit month, day
    """
    today = datetime.date.today()
    if todays_date:
        now = datetime.date(today.year, today.month, today.day)
    else:
        now = datetime.date(today.year + 1, today.month, today.day)
    if isa_format:
        return now.strftime(isa_date_format)

    return now.strftime("%Y%m%d")


def new_timestamped_folder(path):
    """
    Create a new folder under 'path', with current timestamp as name
    :param path:
    :return:
    """
    new_folder = os.path.join(path, get_timestamp())
    try:
        os.makedirs(new_folder)
    except FileExistsError:
        logger.info('Audit folder ' + new_folder + ' already exists, will use this folder.')

    return new_folder


def copy_file(source, destination):
    """
    Make a copy of origin to destination. If destination already exists, it will be replaced.
    :param source:  string containing the full path to the source file, including filename
    :param destination: string containing the path to the source file, including filename
    :return:
    """
    try:
        # copy origin to destination
        logger.info("Copying %s to %s", source, destination)
        shutil.copyfile(source, destination)
    except Exception as e:
        logger.error('Could not create a new folder for the study ' + str(e))
        raise


def copytree(src, dst, symlinks=False, ignore=None, include_raw_data=False, include_investigation_file=True):
    try:
        if not os.path.exists(dst):
            logger.info('Creating a new folder for the study, %s', dst)
            os.makedirs(dst, exist_ok=True)

        file_list = {}
        for item in os.listdir(src):
            source = os.path.join(src, item)
            destination = os.path.join(dst, item)

            if not include_investigation_file and item.startswith('i_'):
                logger.info('Do NOT copy any i_Investigation files from the upload folder')
                continue

            if item.startswith('i_') or item.startswith('s_') or item.startswith('a_') or item.startswith('m_'):
                try:
                    source_file_time = int(get_single_file_information(source))
                    desc_file_time = 0
                    if os.path.isfile(destination):
                        desc_file_time = int(get_single_file_information(destination))
                    diff = source_file_time - desc_file_time
                except Exception as e:
                    diff = 1  # if there is no destination file (in the study folder) then copy the file
                    logger.error('Error copying metadata file %s to %s. Error %s', source, destination, str(e))

                if diff > 0:
                    logger.info('Will copy files')
                    copy_file(source, destination)
            else:
                if include_raw_data:
                    try:
                        time_diff = os.stat(source).st_ctime - os.stat(destination).st_ctime
                    except FileNotFoundError as e:
                        time_diff = 1  # Destination folder does not exist
                        logger.error('Error copying file %s to %s. Error %s', source, destination, str(e))

                    # if os.path.isdir(destination):
                    #     pass  # We already have this folder

                    if int(time_diff) >= 1:
                        # ToDo, rsync all (non-metadata) files
                        file_list = {source, destination}

                        if os.path.isdir(source):
                            logger.info(source + ' is a directory')
                            try:
                                shutil.copytree(source, destination, symlinks=symlinks, ignore=ignore)
                            except OSError as e:
                                logger.error('Does the folder already exists? Can not copy %s to %s', source,
                                             destination, str(e))
                            except FileExistsError as e:
                                logger.error('Folder already exists! Can not copy %s to %s', source, destination,
                                             str(e))
                            except Exception as e:
                                logger.error('Other error! Can not copy %s to %s', source, destination,
                                             str(e))
                        else:  # elif not os.path.exists(destination):
                            logger.info(source + ' is not a directory')
                            try:
                                shutil.copy2(source, destination)  # Should retain all file metadata, ie. timestamps
                                logger.info('Copied file %s to %s', source, destination)
                            except OSError as e:
                                logger.error('Does the file already exists? Can not copy %s to %s', source, destination,
                                             str(e))
                            except FileExistsError as e:
                                logger.error('File already exists! Can not copy %s to %s', source, destination, str(e))
                            except Exception as e:
                                logger.error('Other error! Can not copy %s to %s', source, destination, str(e))
                    else:
                        logger.info("Newer file already exists. Will not copy '%s' to '%s'", source, destination)
    except Exception as e:
        logger.error(str(e))
        raise


def copy_files_and_folders(source, destination, include_raw_data=True, include_investigation_file=True):
    """
      Make a copy of files/folders from origin to destination. If destination already exists, it will be replaced.
      :param source:  string containing the full path to the source file, including filename
      :param destination: string containing the path to the source file, including filename
      :param include_raw_data: Copy all files or metadata only, Boolean (default True)
      :param include_investigation_file: Copy the i_Investigation.txt file, Boolean (default True)
      :return:
      """

    if source is None or destination is None:
        return False, 'Study or upload folder is not known, aborting'

    try:
        # copy origin to destination
        logger.info("Copying %s to %s", source, destination)
        copytree(source, destination, include_raw_data=include_raw_data,
                 include_investigation_file=include_investigation_file)
    except FileNotFoundError:
        return False, 'No files found under ' + source
    except IsADirectoryError:
        return False, 'Please give filename(s), not only upload folder ' + source
    except Exception:
        return False, 'Could not copy files from ' + source

    return True, 'Files successfully copied from ' + source + ' to ' + destination


def remove_samples_from_isatab(std_path):
    # dest folder name is a timestamp
    update_path_suffix = app.config.get('UPDATE_PATH_SUFFIX')
    update_path = os.path.join(std_path, update_path_suffix)
    dest_path = new_timestamped_folder(update_path)
    # check for all samples
    for sample_file in glob.glob(os.path.join(std_path, "s_*.txt")):
        src_file = sample_file
        filename = os.path.basename(sample_file)
        dest_file = os.path.join(dest_path, filename)
        logger.info('Moving %s to %s', src_file, dest_file)
        shutil.move(src_file, dest_file)

        # remove tagged lines
        tag = app.config.get('DELETED_SAMPLES_PREFIX_TAG')
        backup_file = dest_file.replace('.txt', '.bak')
        removed_lines = 0
        with open(dest_file, "r") as infile:
            with open(src_file, "w+") as outfile:
                for line in infile:
                    if tag in line:
                        with open(backup_file, "a+") as backupfile:
                            backupfile.write(line)
                            removed_lines += 1
                    else:
                        outfile.write(line)

    return removed_lines


def get_single_file_information(file_name):
    file_time = ''
    try:
        if not file_name.startswith('.'):  # ignore hidden files on Linux/UNIX
            dt = time.gmtime(os.path.getmtime(file_name))
            file_time = time.strftime(date_format, dt)  # 20180724092134
    except:
        logger.info('Could not find file ' + file_name)

    return file_time


def get_assay_headers_and_protcols(assay_type):
    tidy_header_row = ""
    tidy_data_row = ""
    protocols = ""
    assay_desc = ""
    assay_data_type = ""
    assay_file_type = ""
    assay_mandatory_type = ""

    if assay_type is None or assay_type == 'a':
        logger.error('Assay Type is empty or incorrect!')
        return tidy_header_row, tidy_data_row, protocols, assay_desc, assay_data_type, \
               assay_file_type, assay_mandatory_type

    resource_folder = os.path.join(".", "resources")
    logger.info(' - get_assay_headers_and_protcols for assay type ' + assay_type)
    assay_master_template = os.path.join(resource_folder, 'MetaboLightsAssayMaster.tsv')
    master_df = read_tsv(assay_master_template)

    header_row = master_df.loc[master_df['name'] == assay_type + '-header']
    data_row = master_df.loc[master_df['name'] == assay_type + '-data']
    protocol_row = master_df.loc[master_df['name'] == assay_type + '-protocol']
    assay_desc_row = master_df.loc[master_df['name'] == assay_type + '-assay']
    assay_data_type_row = master_df.loc[master_df['name'] == assay_type + '-type']
    assay_file_type_row = master_df.loc[master_df['name'] == assay_type + '-file']
    assay_data_mandatory_row = master_df.loc[master_df['name'] == assay_type + '-mandatory']

    try:
        protocols = get_protocols_for_assay(protocol_row, assay_type)
        assay_desc = get_desc_for_assay(assay_desc_row, assay_type)
        assay_data_type = get_data_type_for_assay(assay_data_type_row, assay_type)
        assay_file_type = get_file_type_for_assay(assay_file_type_row, assay_type)
        assay_mandatory_type = get_mandatory_data_for_assay(assay_data_mandatory_row, assay_type)
        tidy_header_row = tidy_template_row(header_row)  # Remove empty cells after end of column definition
        tidy_data_row = tidy_template_row(data_row)
    except:
        logger.error('Could not retrieve all required template info for this assay type: ' + assay_type)

    return tidy_header_row, tidy_data_row, protocols, assay_desc, assay_data_type, assay_file_type, assay_mandatory_type


def get_table_header(table_df, study_id=None, file_name=None):
    # Get an indexed header row
    df_header = pd.DataFrame(list(table_df))  # Get the header row only
    df_header = df_header.reset_index().to_dict(orient='list')
    mapping = {}
    assay_type = None

    if file_name is not None and file_name.startswith("a_"):
        try:
            assay_type = get_assay_type_from_file_name(study_id, file_name)
        except:
            assay_type = None

    if assay_type is not None and assay_type != "a":
        tidy_header_row, tidy_data_row, protocols, assay_desc, assay_data_type, assay_file_type, \
        assay_data_mandatory = get_assay_headers_and_protcols(assay_type)
        df_header['type'] = assay_data_type
        df_header['file-type'] = assay_file_type
        df_header['mandatory'] = assay_data_mandatory

        try:
            for i in range(0, len(df_header['index'])):
                mapping[df_header[0][i]] = {"index": df_header['index'][i], "data-type": df_header['type'][i],
                                            "file-type": df_header['file-type'][i],
                                            "mandatory": df_header['mandatory'][i]}
        except:  # Using new assay file pattern, but not correct columns, so try the legacy mapping
            mapping = get_legacy_assay_mapping(df_header)

    else:  # This means we have an assay file that not created with the new pattern
        mapping = get_legacy_assay_mapping(df_header)

    return mapping


def get_legacy_assay_mapping(df_header):
    mapping = {}
    for i in range(0, len(df_header['index'])):
        mapping[df_header[0][i]] = df_header['index'][i]
    return mapping


def get_assay_type_from_file_name(study_id, file_name):
    assay_type = None
    file_name = file_name.replace("a_" + study_id + "_", "")  # Remove study_id and assay refs from filename
    for file_part in file_name.split("_"):  # Split string on assay
        assay_type = file_part  # Only interested in the assay type part, ie. first part
        break

    if assay_type == 'a':  # Legacy filename
        if file_name.endswith('metabolite_profiling_NMR_spectroscopy.txt'):
            assay_type = 'NMR'
        elif file_name.endswith('metabolite_profiling_mass_spectrometry.txt'):
            assay_type = 'LC-MS'  # For this purpose LC and GC has the same columns

    return assay_type


def validate_row(table_header_df, row, http_type):
    try:
        row.pop('index', None)  # Remove "index:n" element, this is the original row number
    except TypeError:
        pass  # Don't worry if it's not present

    try:
        if http_type == 'post':
            if row[0].items() is not None:
                a_row = row[0].items()
        elif http_type == 'put':
            if row.items() is not None:
                a_row = row.items()
    except AttributeError:
        return False, 'Could not find the data for the row'

    for key, value in a_row:
        if key in table_header_df.columns:
            pass
        else:
            return False, "'" + key + "' is not a valid column name. The cell value passed was '" + value + "'"
    return True, 'OK. All columns exist in file'


# Convert panda DataFrame to json tuples object
def totuples(df, text):
    d = [
        dict([
            (colname, row[i])
            for i, colname in enumerate(df.columns)
        ])
        for row in df.values
    ]
    return {text: d}


# Allow for a more detailed logging when on DEBUG mode
def log_request(request_obj):
    if app.config.get('DEBUG'):
        if app.config.get('DEBUG_LOG_HEADERS'):
            logger.debug('REQUEST HEADERS -> %s', request_obj.headers)
        if app.config.get('DEBUG_LOG_BODY'):
            logger.debug('REQUEST BODY    -> %s', request_obj.data)
        if app.config.get('DEBUG_LOG_JSON'):
            try:
                logger.debug('REQUEST JSON    -> %s', request_obj.json)
            except:
                logger.debug('REQUEST JSON    -> EMPTY')


def read_tsv(file_name):
    table_df = pd.DataFrame()  # Empty file
    try:
        try:
            if os.path.getsize(file_name) == 0:  # Empty file
                logger.error("Could not read file " + file_name)
            else:
                # Enforce str datatype for all columns we read from ISA-Tab tables
                col_names = pd.read_csv(file_name, sep="\t", nrows=0).columns
                types_dict = {col: str for col in col_names}
                table_df = pd.read_csv(file_name, sep="\t", header=0, encoding='utf-8', dtype=types_dict)
        except Exception as e:  # Todo, should check if the file format is Excel. ie. not in the exception handler
            if os.path.getsize(file_name) > 0:
                table_df = pd.read_csv(file_name, sep="\t", header=0, encoding='ISO-8859-1')  # Excel format
                logger.info("Tried to open as Excel tsv file 'ISO-8859-1' file " + file_name + ". " + str(e))
    except Exception as e:
        logger.error("Could not read file " + file_name + ". " + str(e))

    table_df = table_df.replace(np.nan, '', regex=True)  # Remove NaN
    return table_df


def tidy_template_row(df):
    row = df.iloc[0]
    new_row = []
    cell_count = 0
    for cell in row:
        if cell_count > 0:  # Skip first cell, this is only for our labeling
            if cell != 'row-end':
                new_row.append(cell)
            if cell == 'row-end':
                return new_row  # We have all the columns now
        cell_count += 1
    return new_row


def get_protocols_for_assay(df_row, assay_type):
    row = df_row.iloc[0]
    prot_list = []

    for cell in row:
        if '|' in cell:
            split_cell = cell.split('|')
            prot_name = split_cell[0]
            prot_params = split_cell[1]
            prot_list.append([assay_type, prot_name, prot_params])

    return prot_list


def get_desc_for_assay(df_row, assay_type):
    row = df_row.iloc[0]

    for cell in row:
        if cell != '' and cell != assay_type + '-assay':  # return first cell that is not the label
            return cell


def get_data_type_for_assay(df_row, assay_type):
    row = df_row.iloc[0]
    new_row = []

    for cell in row:
        if cell == assay_type + '-type':
            continue  # skip the label
        else:
            if cell == '':
                cell = 'string'  # 'string' is the default value if we have not defined a value

            if cell != 'row-end':
                new_row.append(cell)
            if cell == 'row-end':
                return new_row  # We have all the columns now
    return new_row


def get_file_type_for_assay(df_row, assay_type):
    row = df_row.iloc[0]
    new_row = []

    for cell in row:
        if cell == assay_type + '-file':
            continue  # skip the label
        else:
            if cell == '':
                cell = 'string'  # 'string' is the default value if we have not defined a value

            if cell != 'row-end':
                new_row.append(cell)
            if cell == 'row-end':
                return new_row  # We have all the columns now
    return new_row


def get_mandatory_data_for_assay(df_row, assay_type):
    row = df_row.iloc[0]
    new_row = []

    for cell in row:
        if cell == assay_type + '-mandatory':
            continue  # skip the label
        else:
            if cell == '' or cell == 'n':
                cell = False  # 'False' is the default value if we have not defined a value
            if cell == 'y':
                cell = True

            if cell != 'row-end':
                new_row.append(cell)
            if cell == 'row-end':
                return new_row  # We have all the columns now
    return new_row


def write_tsv(dataframe, file_name):
    try:
        # Remove all ".n" numbers at the end of duplicated column names
        dataframe.rename(columns=lambda x: re.sub(r'\.[0-9]+$', '', x), inplace=True)

        # Write the new row back in the file
        dataframe.to_csv(file_name, sep="\t", encoding='utf-8', index=False)
    except:
        return 'Error: Could not write/update the file ' + file_name

    return 'Success. Update file ' + file_name


def add_new_protocols_from_assay(assay_type, protocol_params, assay_file_name, study_id, isa_study):
    # Add new protocol
    logger.info('Adding new Protocols from %s for %s', assay_file_name, study_id)
    protocols = isa_study.protocols

    for prot_param in protocol_params:

        prot_type = prot_param[0]
        prot_name = prot_param[1]
        prot_params = prot_param[2]

        if prot_type not in assay_type:  # Is this protocol for MS or NMR?
            continue

        # check for protocol added already
        obj = isa_study.get_prot(prot_name)
        if obj:
            continue

        protocol_type = 'mass spectrometry'
        if assay_type == 'NMR':
            protocol_type = 'nmr spectroscopy'

        protocol = Protocol(
            name=prot_name,
            # protocol_type=OntologyAnnotation(term=protocol_type),
            protocol_type=OntologyAnnotation(term=prot_name),
            description='Please update this protocol description')

        for param in prot_params.split(';'):
            protocol_parameter = ProtocolParameter(parameter_name=OntologyAnnotation(term=param))
            protocol.parameters.append(protocol_parameter)

        # Add the protocol to the protocols list
        protocols.append(protocol)

    return isa_study


def validate_mzml_files(study_id, obfuscation_code, study_location):
    upload_location = app.config.get('MTBLS_FTP_ROOT') + study_id.lower() + "-" + obfuscation_code

    status, result = True, "All mzML files validated in both study and upload folder"

    # Getting xsd schema for validation
    items = app.config.get('MZML_XSD_SCHEMA')
    xsd_name = items[0]
    script_loc = items[1]

    xmlschema_doc = etree.parse(os.path.join(script_loc, xsd_name))
    xmlschema = etree.XMLSchema(xmlschema_doc)

    for file_loc in [study_location, upload_location]:  # Check both study and upload location. Study first!!!!
        if os.path.isdir(file_loc):  # Only check if the folder exists
            files = glob.iglob(os.path.join(file_loc, '*.mzML'))  # Are there mzML files there?
            if files.gi_yieldfrom is None:  # No files, check sub-folders
                logger.info('Could not find any mzML files, checking any sub-folders')
                files = glob.iglob(os.path.join(file_loc, '**/*.mzML'), recursive=True)

            for file in files:
                try:
                    logger.info('Validating mzML file ' + file)
                    status, result = validate_xml(xml=file, xmlschema=xmlschema)
                    if not status:
                        return status, result
                    # Ok, the file validated, so we now copy it file to the study folder
                    if file_loc == upload_location:
                        shutil.copy(file, study_location)
                        # copy_file(file, study_location)
                        try:
                            logger.info('Moving mzML file "' + file + '" into study location ' + study_location)
                            # Rename the file so that we don't have to validate/copy it again
                            shutil.move(file, file + ".MOVED")
                        except Exception:
                            return False, "Could not copy the mzML file " + file
                except Exception:
                    return status

    return status, result


def validate_xml(xml=None, xmlschema=None):
    # parse xml
    try:
        doc = etree.parse(xml)
    except IOError:
        return False, {"Error": "Can not read the file " + xml}
    except etree.XMLSyntaxError:
        return False, {"Error": "File " + xml + " is not a valid XML file"}

    # validate against schema
    try:
        xmlschema.assertValid(doc)
        print('XML valid, schema validation ok: ' + xml)
        return True, "File " + xml + " is a valid XML file"
    except etree.DocumentInvalid:
        print('Schema validation error. ' + xml)
        return False, "Can not validate the file " + xml


def to_isa_tab(study_id, input_folder, output_folder):
    try:
        isa_convert(input_folder, output_folder, study_id, jobs=2)
    except Exception as e:
        return False, "Could not convert mzML to ISA-Tab study " + study_id + ". " + str(e)

    return True, "ISA-Tab files generated for study " + study_id


def convert_to_isa(study_location, study_id):
    input_folder = study_location + "/"
    output_folder = study_location + "/"
    status, message = to_isa_tab(study_id, input_folder, output_folder)
    return status, message


def update_correct_sample_file_name(isa_study, study_location, study_id):
    sample_file_name = isa_study.filename
    sample_file_name = os.path.join(study_location, sample_file_name)
    short_sample_file_name = 's_' + study_id.upper() + '.txt'
    default_sample_file_name = os.path.join(study_location, short_sample_file_name)
    if os.path.isfile(sample_file_name):
        if sample_file_name != default_sample_file_name:
            isa_study.identifier = study_id  # Adding the study identifier
            os.rename(sample_file_name, default_sample_file_name)  # Rename the sample file
            isa_study.filename = short_sample_file_name  # Add the new filename to the investigation

    return isa_study, short_sample_file_name


def get_maf_name_from_assay_name(assay_file_name):
    annotation_file_name = assay_file_name.replace(".txt", "_v2_maf.tsv")
    for file_part in annotation_file_name.split("/a_"):
        maf_name = file_part

    maf_name = maf_name.replace("a_", "")
    maf_name = "m_" + maf_name
    return maf_name


def update_ontolgies_in_isa_tab_sheets(ontology_type, old_value, new_value, study_location, isa_study):
    try:
        """ 
        Update column header in sample and assay file(s). The column will look like 'Factor Value[<factor name>]' or 
        'Characteristics[<characteristics name>']
        """

        prefix = ""
        postfix = "]"
        if ontology_type.lower() == 'factor':
            prefix = 'Factor Value['
        elif ontology_type.lower() == 'characteristics':
            prefix = 'Characteristics['

        file_names = []
        # Sample sheet
        file_names.append(os.path.join(study_location, isa_study.filename))
        #  assay_sheet(s)
        for assay in isa_study.assays:
            file_names.append(os.path.join(study_location, assay.filename))

        if file_names:
            for file in file_names:
                file_df = read_tsv(file)
                try:
                    old = prefix + old_value + postfix
                    new = prefix + new_value + postfix
                    if old != new:  # Do we need to change the column value?
                        file_df.rename(columns={old: new}, inplace=True)
                        write_tsv(file_df, file)
                        logger.info(ontology_type + " " + new_value + " has been renamed in " + file)
                except Exception as e:
                    logger.warning(ontology_type + " " + new_value +
                                   " was not used in the sheet or we failed updating " + file + ". Error: " + str(e))

    except Exception as e:
        logger.error("Could not update the ontology value " + old_value + " in all sheets")


def create_maf(technology, study_location, assay_file_name, annotation_file_name):
    resource_folder = os.path.join(".", "resources")
    update_maf = False

    if technology is None:
        if "nmr" in assay_file_name.lower():
            technology = "NMR"

    # Fixed column headers to look for in the MAF, defaults to MS
    sample_name = 'Sample Name'
    assay_name = 'MS Assay Name'
    annotation_file_template = os.path.join(resource_folder, 'm_metabolite_profiling_mass_spectrometry_v2_maf.tsv')

    # NMR MAF and assay name
    if technology == "NMR":
        annotation_file_template = os.path.join(resource_folder, 'm_metabolite_profiling_NMR_spectroscopy_v2_maf.tsv')
        assay_name = 'NMR Assay Name'

    if annotation_file_name is None or len(annotation_file_name) == 0:
        annotation_file_name = get_maf_name_from_assay_name(assay_file_name)

    full_annotation_file_name = os.path.join(study_location, annotation_file_name)
    assay_file_name = os.path.join(study_location, assay_file_name)

    # Get the MAF table or create a new one if it does not already exist
    try:
        maf_df = pd.read_csv(full_annotation_file_name, sep="\t", header=0, encoding='utf-8')
    except FileNotFoundError:
        update_maf = True
        maf_df = pd.read_csv(annotation_file_template, sep="\t", header=0, encoding='utf-8')
        logger.info("Creating new MAF: " + full_annotation_file_name)
    except UnicodeDecodeError as e:
        if os.path.getsize(full_annotation_file_name) > 0:
            logger.info(
                "Trying to open as Excel tsv file 'ISO-8859-1' file " + full_annotation_file_name + ". " + str(e))
            maf_df = pd.read_csv(full_annotation_file_name, sep="\t", header=0, encoding='ISO-8859-1')  # Excel format

    # Get rid of empty numerical values
    maf_df = maf_df.replace(np.nan, '', regex=True)

    # Read NMR or MS Assay Name first, if that is empty, use Sample Name
    assay_df = read_tsv(assay_file_name)

    assay_names = []
    # Get the MS/NMR Assay Name or Sample names from the assay
    try:
        assay_names_df = assay_df[assay_name]
        if assay_names_df:
            for assay_name in assay_names_df:
                if len(assay_name) != 0:
                    assay_names.append(assay_name)
    except:
        logger.warning('The assay ' + assay_file_name + ' does not have ' + assay_name + ' defined!')

    try:
        sample_names = assay_df[sample_name]
    except:
        logger.warning('The assay ' + assay_file_name + ' does not have ' + sample_name + ' defined!')

    if len(assay_names) == 0:
        assay_names = sample_names

    new_column_counter = 0
    # Does the column already exist?
    for row in assay_names:
        s_name = str(row)
        if s_name != '':
            try:
                in_maf = maf_df.columns.get_loc(s_name)
            except KeyError:  # Key is not found, so add it
                # Add the new columns to the MAF
                maf_df[s_name] = ""
                new_column_counter += 1
                update_maf = True

    # Write the new empty columns back in the file
    if update_maf:
        maf_df.to_csv(full_annotation_file_name, sep="\t", encoding='utf-8', index=False)

    return maf_df, annotation_file_name, new_column_counter


def add_ontology_to_investigation(isa_inv, onto_name, onto_version, onto_file, onto_desc):
    # Check if the OBI ontology has already been referenced
    if not onto_name:
        onto_name = 'N/A'
    onto = OntologySource(
        name=onto_name,
        version=onto_version,
        file=onto_file,
        description=onto_desc)

    onto_exists = isa_inv.get_ontology_source_reference(onto_name)
    if onto_exists is None:  # Add the ontology to the investigation
        ontologies = isa_inv.get_ontology_source_references()
        ontologies.append(onto)

    return isa_inv, onto


def remove_file(file_location, file_name, allways_remove=False):
    # Raw files are sometimes actually folders, so need to check if file or folder before removing
    file_to_delete = os.path.join(file_location, file_name)
    # file_status == 'active' of a file is actively used as metadata
    file_type, file_status, folder = map_file_type(file_name, file_location)

    try:
        if file_type == 'metadata_investigation' or file_type == 'metadata_assay' or file_type == 'metadata_sample' or file_type == 'metadata_maf':
            if file_status == 'active' and not allways_remove:  # If active metadata and "remove anyway" flag if not set
                return False, "Can not delete any active metadata files " + file_name
        if os.path.exists(file_to_delete):  # First, does the file/folder exist?
            if os.path.isfile(file_to_delete):  # is it a file?
                os.remove(file_to_delete)
            elif os.path.isdir(file_to_delete):  # is it a folder
                shutil.rmtree(file_to_delete)
        else:
            return False, "Can not find file " + file_name
    except:
        return False, "Can not delete file " + file_name
    return True, "File " + file_name + " deleted"


def map_file_type(file_name, directory, assay_file_list=None):
    active_status = 'active'
    none_active_status = 'unreferenced'
    folder = False
    final_filename = os.path.basename(file_name)
    # fname, ext = os.path.splitext(file_name)
    fname, ext = os.path.splitext(final_filename)
    fname = fname.lower()
    ext = ext.lower()
    empty_exclusion_list = app.config.get('EMPTY_EXCLUSION_LIST')
    ignore_file_list = app.config.get('IGNORE_FILE_LIST')
    raw_files_list = app.config.get('RAW_FILES_LIST')
    derived_files_list = app.config.get('DERIVED_FILES_LIST')
    compressed_files_list = app.config.get('COMPRESSED_FILES_LIST')
    internal_mapping_list = app.config.get('INTERNAL_MAPPING_LIST')

    # Metadata first, current is if the files are present in the investigation and assay files
    if fname.startswith(('i_', 'a_', 's_', 'm_')) and (ext == '.txt' or ext == '.tsv'):
        if fname.startswith('a_'):
            if is_file_referenced(file_name, directory, 'i_'):
                return 'metadata_assay', active_status, folder
        elif fname.startswith('s_'):
            if is_file_referenced(file_name, directory, 'i_'):
                return 'metadata_sample', active_status, folder
        elif fname.startswith('m_'):
            if is_file_referenced(file_name, directory, 'a_', assay_file_list=assay_file_list):
                return 'metadata_maf', active_status, folder
            else:
                return 'metadata_maf', none_active_status, folder
        elif fname.startswith('i_'):
            investigation = os.path.join(directory, 'i_')
            if os.sep + 'audit' + os.sep in directory:
                return 'metadata_investigation', none_active_status, folder
            for invest_file in glob.glob(investigation + '*'):  # Default investigation file pattern
                if open(invest_file, encoding='utf8', errors="ignore").read():
                    return 'metadata_investigation', active_status, folder
        return 'metadata', none_active_status, folder
    elif final_filename in ('fid', 'fid.txt'):  # NMR data
        return 'fid', active_status, folder
    elif final_filename in ('acqus', 'acqus.txt', 'acqu', 'acqu.txt'):  # NMR data
        return 'acqus', active_status, folder
    elif ext in ('.xls', '.xlsx', '.xlsm', '.csv', '.tsv'):
        return 'spreadsheet', active_status, folder
    elif ext in ('.sdf', '.mol'):
        return 'chemical_structure', active_status, folder
    elif ext in ('.png', '.tiff', '.tif', '.jpeg', '.mpg', '.jpg'):
        return 'image', active_status, folder
    elif ext in ('.result_c', '.mcf', '.mcf_idx', '.hdx', '.u2', '.method', '.unt', '.hss', '.ami', '.baf', '.content',
                 '.baf_idx', '.baf_xtr', '.xmc') or fname == 'synchelper' or fname == 'pulseprogram':
        return 'part_of_raw', active_status, folder
    elif ext in ('.txt', '.text', '.tab', '.html', '.ini'):
        for ignore in ignore_file_list:  # some internal RAW datafiles have these extensions, so ignore
            if ignore in fname:
                return 'part_of_raw', none_active_status, folder
        return 'text', active_status, folder
    elif fname.startswith('~') or ext.endswith('~') or ext in ('.temp', '.tmp'):
        return 'temp', none_active_status, folder
    elif ext in ('.r', '.java', '.py', '.rdata', '.xsd', '.scan') and '.wiff' not in fname:
        return 'programmatic', none_active_status, folder
    elif ext in ('.partial', '.aspera-ckpt', '.aspx'):
        return 'aspera-control', none_active_status, folder
    elif file_name == 'audit':
        return 'audit', none_active_status, True
    elif file_name == '.DS_Store':
        return 'macos_special_file', none_active_status, False
    elif ext in derived_files_list:
        if is_file_referenced(file_name, directory, 'a_', assay_file_list=assay_file_list):
            return 'derived', active_status, folder
        else:
            return 'derived', none_active_status, folder
    elif ext in compressed_files_list:
        if is_file_referenced(file_name, directory, 'a_', assay_file_list=assay_file_list):
            return 'compressed', active_status, folder
        else:
            return 'compressed', none_active_status, folder
    elif fname in internal_mapping_list:
        return 'internal_mapping', active_status, folder
    elif fname.endswith(('.tsv.split', '_pubchem.tsv', '_annotated.tsv')):
        return 'chebi_pipeline_file', active_status, folder
    elif fname in empty_exclusion_list:
        return 'ignored', none_active_status, folder
    else:
        for ignore in ignore_file_list:
            if ignore in fname:
                return 'part_of_raw', none_active_status, folder
        if is_file_referenced(file_name, directory, 'a_', assay_file_list=assay_file_list):
            if os.path.isdir(os.path.join(directory, file_name)):
                return 'raw', active_status, True
            else:
                return 'raw', active_status, folder
        else:
            if ext in raw_files_list:
                if os.path.isdir(os.path.join(directory, file_name)):
                    return 'raw', none_active_status, True
                else:
                    return 'raw', none_active_status, folder

            if os.path.isdir(os.path.join(directory, file_name)):
                return 'directory', none_active_status, True

        return 'unknown', none_active_status, folder


def traverse_subfolders(study_location=None, file_location=None, file_list=None, all_folders=None, full_path=None):
    if not file_list:
        file_list = []
    if not all_folders:
        all_folders = []

    # Check that we have both referenced folders
    if not os.path.isdir(study_location) or not os.path.isdir(file_location):
        return file_list, all_folders

    folder_exclusion_list = app.config.get('FOLDER_EXCLUSION_LIST')

    if file_location not in all_folders:
        for params in os.walk(file_location):
            root = None
            sub_directories = None
            files = None

            if params:
                if params[0]:
                    root = params[0]
                if params[1]:
                    sub_directories = params[1]
                if params[2]:
                    files = params[2]

            if root and basename(normpath(root)) not in folder_exclusion_list:
                if root not in all_folders:
                    all_folders.append(root)
                if files:
                    for file in files:
                        if file:
                            file_name = file
                            if full_path:
                                file_name = os.path.join(root.replace(study_location, ""), file_name)
                            if file_name not in file_list:
                                file_list.append(file_name)

                if sub_directories:
                    for directory in sub_directories:
                        if directory and directory not in folder_exclusion_list:
                            next_folder = os.path.join(root, directory)
                            if next_folder not in all_folders:
                                file_list, all_folders = traverse_subfolders(
                                    study_location=study_location, file_location=next_folder, file_list=file_list,
                                    all_folders=all_folders, full_path=full_path)

    return file_list, all_folders


def is_file_referenced(file_name, directory, isa_tab_file_to_check, assay_file_list=None):
    """ There can be more than one assay, so each MAF must be checked against
    each Assay file. Do not state a MAF as not in use if it's used in the 'other' assay """
    found = False
    start_time = time.time()

    try:  # Submitters using standard ISAcreator (not ours) with a non UFT-8 character set will cause issues
        file_name = file_name.encode('ascii', 'ignore').decode('ascii')
    except Exception as e:
        logger.error(str(e))

    try:

        if 'audit' in directory or 'audit' + os.sep in file_name:
            return False

        if assay_file_list and isa_tab_file_to_check.startswith('a_'):
            if file_name in assay_file_list:
                return True
            else:
                return False

        if file_name.startswith(('i_', 'a_', 's_', 'm_')) and os.sep + 'ftp' in directory:  # FTP metadata
            return False

        isa_tab_file_to_check = isa_tab_file_to_check + '*.txt'
        isa_tab_file = os.path.join(directory, isa_tab_file_to_check)
        for ref_file_name in glob.glob(isa_tab_file):
            """ The filename we pass in is found referenced in the metadata (ref_file_name)
            One possible problem here is of the maf is found in an old assay file, then we will report it as 
            current """
            try:
                logger.info("Checking if file " + file_name + " is referenced in " + ref_file_name)
                if file_name in io.open(ref_file_name, 'r', encoding='utf8', errors="ignore").read():
                    found = True
            except Exception as e:
                logger.error('File Format error? Cannot read or open file ' + file_name)
                logger.error(str(e))

        logger.info("Looking for file name '" + file_name + "' in ISA-Tab files took %s seconds" % round(
            time.time() - start_time, 2))
    except Exception as e:
        logger.error('File Format error? Cannot access file :' + str(file_name))
        logger.error(str(e))
        return False
    return found


def find_text_in_isatab_file(study_folder, text_to_find):
    found = False
    isa_tab_file = os.path.join(study_folder, 'i_*.txt')
    for ref_file in glob.glob(isa_tab_file):
        try:
            logger.info("Checking if text " + text_to_find + " is referenced in " + ref_file)
            if text_to_find in io.open(ref_file, 'r', encoding='utf8', errors="ignore").read():
                found = True
        except Exception as e:
            logger.error('File Format error? Cannot read or open file ' + ref_file)
            logger.error(str(e))

    return found


def get_assay_file_list(study_location):
    assay_files = os.path.join(study_location, 'a_*.txt')
    all_files = []

    for assay_file_name in glob.glob(assay_files):
        assay_cols = []
        assay_df = read_tsv(assay_file_name)
        df_header = get_table_header(assay_df)
        for header, value in df_header.items():
            if ' File' in header:
                assay_cols.append(value)

        for col_pos in assay_cols:
            unique_files = np.unique(assay_df.iloc[:, col_pos].values).tolist()
            for a_file in unique_files:
                if a_file not in all_files and len(str(a_file)) > 0:
                    all_files.append(a_file)

    return all_files


def track_ga_event(category, action, tracking_id=None, label=None, value=0):
    data = {
        'v': '1',  # API Version.
        'tid': tracking_id,  # Tracking ID / Property ID.
        # Anonymous Client Identifier. Ideally, this should be a UUID that
        # is associated with particular user, device, or browser instance.
        'cid': '555',
        't': 'event',  # Event hit type.
        'ec': category,  # Event category.
        'ea': action,  # Event action.
        'el': label,  # Event label.
        'ev': value,  # Event value, must be an integer
    }

    response = requests.post('https://www.google-analytics.com/collect', data=data)

    print("Calling Google Analytics with tracking id: " + tracking_id +
          " returned response code: " + str(response.status_code))


def google_analytics():
    tracking_id = app.config.get('GA_TRACKING_ID')
    if tracking_id:
        environ = request.headers.environ
        url = environ['REQUEST_URI']
        track_ga_event(
            tracking_id=tracking_id,
            category='MetaboLights-WS',
            action=url)


def safe_str(obj):
    if not obj:
        return ""
    try:
        return obj.encode('ascii', 'ignore').decode('ascii')
    except UnicodeEncodeError:
        return ""
    except Exception as e:
        logger.error(str(e))
        return ""


def val_email(email=None):
    email_username_pattern = '^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$'
    if not re.search(email_username_pattern, email):
        abort(406, "Incorrect email " + email)


def get_new_password_and_api_token():
    api_token = uuid.uuid1()
    password = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(8))
    password_encoded = base64.b64encode(password.encode("utf-8"))
    password_encoded = str(password_encoded, 'utf-8')
    return password, password_encoded, api_token


def writeDataToFile(filename, data, pretty=False):
    with open(filename, 'w', encoding='utf-8') as fp:
        if pretty:
            from pprint import PrettyPrinter
            pp = PrettyPrinter(indent=4)
            fp.write(pp.pformat(data))
        else:
            json.dump(data, fp)


def readDatafromFile(fileName):
    with open(fileName, "r", encoding='utf-8' ) as read_file:
        data = json.load(read_file)
    return data
