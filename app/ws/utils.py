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

import glob
import logging
import os
import os.path
import shutil
import time
import datetime
from flask import current_app as app
import pandas as pd
import numpy as np
import re
import io
from isatools.model import Protocol, ProtocolParameter, OntologySource
from app.ws.mm_models import OntologyAnnotation
from lxml import etree
from mzml2isa.parsing import convert as isa_convert

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


def get_year_plus_one(isa_format=False):
    """
    Get a string with the current date 20170302
    :return: %Y%m%d - full year, two digit month, day
    """
    today = datetime.date.today()
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
                        if os.path.isdir(source):
                            logger.info(source + ' is a directory')
                            try:
                                shutil.copytree(source, destination, symlinks=symlinks, ignore=ignore)
                            except OSError as e:
                                logger.error('Folder already exists? Can not copy %s to %s', source, destination, str(e))
                        else:  # elif not os.path.exists(destination):
                            logger.info(source + ' is not a directory')
                            try:
                                shutil.copy2(source, destination)  # Should retain all file metadata, ie. timestamps
                                logger.info('Copied file %s to %s', source, destination)
                            except OSError as e:
                                logger.error('File already exists? Can not copy %s to %s', source, destination, str(e))
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
    assay_mandatory_type = ""

    if assay_type is None or assay_type == 'a':
        logger.error('Assay Type is empty or incorrect!')
        return tidy_header_row, tidy_data_row, protocols, assay_desc, assay_data_type, assay_mandatory_type

    logger.info(' - get_assay_headers_and_protcols for assay type ' + assay_type)
    assay_master_template = './resources/MetaboLightsAssayMaster.tsv'
    master_df = read_tsv(assay_master_template)

    header_row = master_df.loc[master_df['name'] == assay_type + '-header']
    data_row = master_df.loc[master_df['name'] == assay_type + '-data']
    protocol_row = master_df.loc[master_df['name'] == assay_type + '-protocol']
    assay_desc_row = master_df.loc[master_df['name'] == assay_type + '-assay']
    assay_data_type_row = master_df.loc[master_df['name'] == assay_type + '-type']
    assay_data_mandatory_row = master_df.loc[master_df['name'] == assay_type + '-mandatory']

    try:
        protocols = get_protocols_for_assay(protocol_row, assay_type)
        assay_desc = get_desc_for_assay(assay_desc_row, assay_type)
        assay_data_type = get_data_type_for_assay(assay_data_type_row, assay_type)
        assay_mandatory_type = get_mandatory_data_for_assay(assay_data_mandatory_row, assay_type)
        tidy_header_row = tidy_template_row(header_row)  # Remove empty cells after end of column definition
        tidy_data_row = tidy_template_row(data_row)
    except:
        logger.error('Could not retrieve all required template info for this assay type: ' + assay_type)

    return tidy_header_row, tidy_data_row, protocols, assay_desc, assay_data_type, assay_mandatory_type


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
        tidy_header_row, tidy_data_row, protocols, assay_desc, assay_data_type, assay_data_mandatory = \
            get_assay_headers_and_protcols(assay_type)
        df_header['type'] = assay_data_type
        df_header['mandatory'] = assay_data_mandatory
        try:
            for i in range(0, len(df_header['index'])):
                mapping[df_header[0][i]] = {"index": df_header['index'][i], "data-type": df_header['type'][i],
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
        assay_type = file_part  # Only interested in the assay type part
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
    try:
        if os.path.getsize(file_name) == 0:
            table_df = pd.DataFrame()  # Empty file
            logger.info("Could not read file " + file_name)
        else:
            # Enforce str datatype for all columns we read from ISA-Tab tables
            col_names = pd.read_csv(file_name, sep="\t", nrows=0).columns
            types_dict = {col: str for col in col_names}
            table_df = pd.read_csv(file_name, sep="\t", header=0, encoding='utf-8', dtype=types_dict)
    except:  # Todo, should check if the file format is Excel. ie. not in the exception handler
        table_df = pd.read_csv(file_name, sep="\t", header=0, encoding='ISO-8859-1')  # Excel format
        logger.info("Have to open as Excel tsv file 'ISO-8859-1' file " + file_name)
    table_df = table_df.replace(np.nan, '', regex=True)  # Remove NaN
    return table_df


def tidy_template_row(df):
    row = df.iloc[0]
    new_row = []
    cell_count = 0
    for cell in row:
        if cell_count > 0:   # Skip first cell, this is only for our labeling
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

    for file_loc in [study_location, upload_location]:  # Check both study and upload location. Study first!!!!
        if os.path.isdir(file_loc):  # Only check if the folder exists
            files = glob.iglob(os.path.join(file_loc, '*.mzML'))  # Are there mzML files there?
            if files.gi_yieldfrom is None:  # No files, check sub-folders
                logger.info('Could not find any mzML files, checking any sub-folders')
                files = glob.iglob(os.path.join(file_loc, '**/*.mzML'), recursive=True)
            # TODO validate the XSD here, only needed once
            for file in files:
                try:
                    logger.info('Validating mzML file ' + file)
                    status, result = validate_xml(os.path.join(script_loc, xsd_name), file)
                    if not status:
                        return status, result
                    # Ok, the file validated, so we now copy it file to the study folder
                    if file_loc == upload_location:
                        shutil.copy(file, study_location)
                        #copy_file(file, study_location)
                        try:
                            logger.info('Moving mzML file "' + file + '" into study location ' + study_location)
                            # Rename the file so that we don't have to validate/copy it again
                            shutil.move(file, file + ".MOVED")
                        except Exception:
                            return False, "Could not copy the mzML file " + file
                except Exception:
                    return status

    return status, result


def validate_xml(xsd, xml):

    xmlschema_doc = etree.parse(xsd)
    xmlschema = etree.XMLSchema(xmlschema_doc)

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
    input_folder = study_location
    output_folder = study_location
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


def create_maf(technology, study_location, assay_file_name, annotation_file_name):
    resource_folder = "./resources/"
    update_maf = False

    if technology is None:
        if "nmr" in assay_file_name.lower():
            technology = "NMR"

    # Fixed column headers to look for in the MAF, defaults to MS
    sample_name = 'Sample Name'
    assay_name = 'MS Assay Name'
    annotation_file_template = resource_folder + 'm_metabolite_profiling_mass_spectrometry_v2_maf.tsv'

    # NMR MAF and assay name
    if technology == "NMR":
        annotation_file_template = resource_folder + 'm_metabolite_profiling_NMR_spectroscopy_v2_maf.tsv'
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
    fname, ext = os.path.splitext(file_name)
    fname = fname.lower()
    ext = ext.lower()
    # Metadata first, current is if the files are present in the investigation and assay files
    if fname.startswith(('i_', 'a_', 's_', 'm_')):
        if fname.startswith('a_'):
            if is_file_referenced(file_name, directory, 'i_'):
                return 'metadata_assay', active_status, folder
        elif fname.startswith('s_'):
            if is_file_referenced(file_name, directory, 'i_'):
                return 'metadata_sample', active_status, folder
        elif fname.startswith('m_'):
            if is_file_referenced(file_name, directory, 'a_', assay_file_list=assay_file_list):
                return 'metadata_maf', active_status, folder
        elif fname.startswith('i_'):
            investigation = os.path.join(directory, 'i_')
            if os.sep + 'audit' + os.sep in directory:
                return 'metadata_investigation', none_active_status, folder
            for invest_file in glob.glob(investigation + '*'):  # Default investigation file pattern
                if open(invest_file, encoding='utf8', errors="ignore").read():
                    return 'metadata_investigation', active_status, folder
        return 'metadata', none_active_status, folder
    elif file_name == 'fid':  # NMR data
        return 'fid', active_status, folder
    elif ext in ('.xls', '.xlsx', '.xlsm', '.csv', '.tsv'):
        return 'spreadsheet', active_status, folder
    elif ext in ('.sdf', '.mol'):
        return 'chemical_structure', active_status, folder
    elif ext in ('.png', '.tiff', '.tif', '.jpeg', '.mpg', '.jpg'):
        return 'image', active_status, folder
    elif ext in ('.result_c', '.mcf', '.mcf_idx', '.hdx', '.u2', '.method', '.unt', '.hss', '.ami', '.baf', '.content',
                 '.baf_idx', '.baf_xtr', '.xmc') or fname == 'synchelper':
        return 'part_of_raw', active_status, folder
    elif ext in ('.txt', '.text', '.tab', '.html', '.ini'):
        return 'text', active_status, folder
    elif fname.startswith('~') or ext.endswith('~') or ext in('.temp', '.tmp'):
        return 'temp', none_active_status, folder
    elif ext in ('.r', '.java', '.py', '.rdata', '.xsd', '.scan'):
        return 'programmatic', none_active_status, folder
    elif ext in ('.partial', '.aspera-ckpt'):
        return 'aspera-control', none_active_status, folder
    elif file_name == 'audit':
        return 'audit', none_active_status, True
    elif file_name == '.DS_Store':
        return 'macos_special_file', none_active_status, False
    elif ext in ('.mzml', '.nmrml', '.mzxml', '.xml', '.mzdata'):
        if is_file_referenced(file_name, directory, 'a_', assay_file_list=assay_file_list):
            return 'derived', active_status, folder
        else:
            return 'derived', none_active_status, folder
    elif ext in ('.zip', 'zipx', '.gz', '.CDF.gz', '.tar', '.7z', '.z', '.g7z', '.arj', 'rar',
                 '.bz2', '.arj', '.z', '.war'):
        if is_file_referenced(file_name, directory, 'a_', assay_file_list=assay_file_list):
            return 'compressed', active_status, folder
        else:
            return 'compressed', none_active_status, folder
    elif fname == 'metexplore_mapping.json' or fname == 'chebi_pipeline_annotations':
        return 'internal_mapping', active_status, folder
    elif fname.endswith(('.tsv.split', '_pubchem.tsv', '_annotated.tsv')):
        return 'maf_pipeline_file', active_status, folder
    else:
        if is_file_referenced(file_name, directory, 'a_', assay_file_list=assay_file_list):
            if os.path.isdir(os.path.join(directory, file_name)):
                return 'raw', active_status, True
            else:
                return 'raw', active_status, folder
        else:
            if ext in ('.d', '.raw', '.idb', 'cdf', '.wiff', '.scan', '.dat'):
                if os.path.isdir(os.path.join(directory, file_name)):
                    return 'raw', none_active_status, True
                else:
                    return 'raw', none_active_status, folder

            if os.path.isdir(os.path.join(directory, file_name)):
                return 'unknown', none_active_status, True

        return 'unknown', none_active_status, folder


def is_file_referenced(file_name, directory, isa_tab_file_to_check, assay_file_list=None):
    """ There can be more than one assay, so each MAF must be checked against
    each Assay file. Do not state a MAF as not in use if it's used in the 'other' assay """
    found = False
    start_time = time.time()
    if os.sep + 'audit' + os.sep in directory:
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

    logger.info("Looking for file name " + file_name + " in ISA-Tab files took %s seconds" % round(time.time() - start_time, 2))
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
