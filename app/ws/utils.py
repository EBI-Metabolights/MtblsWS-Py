import glob
import logging
import os
import shutil
import time
import datetime
from flask import current_app as app
import pandas as pd
import numpy as np
import re
from isatools.model import Protocol, ProtocolParameter
from app.ws.mm_models import OntologyAnnotation
from lxml import etree
from mzml2isa.parsing import convert as isa_convert

"""
Utils

Misc of utils
"""

logger = logging.getLogger('wslog')

def get_timestamp():
    """
    Get a string with the current date & time as a timestamp i.e. 20170302143045
    :return: %Y%m%d%H%M%S - full year, two digit month, day, 24 hour, minutes and seconds
    """
    return time.strftime("%Y%m%d%H%M%S")


def get_year_plus_one():
    """
    Get a string with the current date 20170302
    :return: %Y%m%d - full year, two digit month, day
    """
    today = datetime.date.today()
    now = datetime.date(today.year + 1, today.month, today.day)
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
    except Exception:
        logger.error('Could not create a new folder for the study')
        raise


def copytree(src, dst, symlinks=False, ignore=None, metadata=True):
    # Todo, add a feature to only copy metadata files
    try:
        if not os.path.exists(dst):
            logger.info('Creating a new folder for the study, %s', dst)
            os.makedirs(dst, exist_ok=True)

        for item in os.listdir(src):
            source = os.path.join(src, item)
            destination = os.path.join(dst, item)
            try:
                time_diff = os.stat(source).st_ctime - os.stat(destination).st_ctime
            except FileNotFoundError:
                time_diff = 1  # Destination folder does not exist

            if os.path.isdir(destination):
                pass  # We already have this folder

            if int(time_diff) >= 1:
                if os.path.isdir(source):
                    shutil.copytree(source, destination, symlinks, ignore)
                else:  # elif not os.path.exists(destination):
                    shutil.copy2(source, destination)  # Should retain all file metadata, ie. timestamps
                    logger.info('Copied file %s to %s', source, destination)

    except Exception:
        raise


def copy_files_and_folders(source, destination, metabdata):
    """
      Make a copy of files/folders from origin to destnation. If destination already exists, it will be replaced.
      :param source:  string containing the full path to the source file, including filename
      :param destination: string containing the path to the source file, including filename
      :param metabdata: Copy metadata only, Boolean (default True)
      :return:
      """

    if source is None or destination is None:
        return False, 'Study or upload folder is not known, aborting'

    try:
        # copy origin to destination
        logger.info("Copying %s to %s", source, destination)
        copytree(source, destination)
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
        backup_file = dest_file.strip('.txt') + '.bak'
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


def get_all_files(path):
    try:
        files = get_file_information(path)
    except:
        files = []  # The upload folder for this study does not exist, this is normal
    return files


def is_file_referenced(file_name, directory, isa_tab_file_to_check):
    """ There can be more than one assay, so each MAF must be checked against
    each Assay file. Do not state a MAF as not in use if it's used in the 'other' assay """
    found = False
    isa_tab_file_to_check = isa_tab_file_to_check + '*.txt'
    isa_tab_file = os.path.join(directory, isa_tab_file_to_check)
    for ref_file_name in glob.glob(isa_tab_file):
        """ The filename we pass in is found referenced in the metadata (ref_file_name)
        One possible problem here is of the maf is found in an old assay file, then we will report it as 
        current """
        if file_name in open(ref_file_name).read():
            found = True
    return found


def map_file_type(file_name, directory):
    active_status = 'active'
    none_active_status = 'unreferenced'
    # Metadata first, current is if the files are present in the investigation and assay files
    if file_name.startswith(('i_', 'a_', 's_', 'm_')):
        if file_name.startswith('a_'):
            if is_file_referenced(file_name, directory, 'i_'):
                return 'metadata_assay', active_status
        elif file_name.startswith('s_'):
            if is_file_referenced(file_name, directory, 'i_'):
                return 'metadata_sample', active_status
        elif file_name.startswith('m_'):
            if is_file_referenced(file_name, directory, 'a_'):
                return 'metadata_maf', active_status
        elif file_name.startswith('i_'):
            investigation = os.path.join(directory, 'i_')
            for invest_file in glob.glob(investigation + '*'):  # Default investigation file pattern
                if open(invest_file).read():
                    return 'metadata_investigation', active_status
        return 'metadata', 'old'
    elif file_name.lower().endswith(('.xls', '.xlsx', '.csv', '.tsv')):
        return 'spreadsheet', active_status
    elif file_name.endswith('.txt'):
        return 'text', active_status
    elif file_name == 'audit':
        return 'audit', active_status
    elif file_name.lower().endswith(('.mzml', '.nmrml', '.mzxml', '.xml')):
        if is_file_referenced(file_name, directory, 'a_'):
            return 'derived', active_status
        else:
            return 'derived', none_active_status
    elif file_name.lower().endswith(('.zip', '.gz', '.tar', '.7z', '.z')):
        if is_file_referenced(file_name, directory, 'a_'):
            return 'compressed', active_status
        else:
            return 'compressed', none_active_status
    elif file_name == 'metexplore_mapping.json':
        return 'internal_mapping', active_status
    else:
        if is_file_referenced(file_name, directory, 'a_'):
            return 'raw', active_status
        else:
            return 'unknown', none_active_status


def get_file_information(directory):
    file_list = []
    for file_name in os.listdir(directory):
        if not file_name.startswith('.'):  # ignore hidden files on Linux/UNIX
            dt = time.gmtime(os.path.getmtime(os.path.join(directory, file_name)))
            raw_time = time.strftime('%Y%m%d%H%M%S', dt)  # 20180724092134
            file_time = time.strftime('%B %d %Y %H:%M:%S', dt)  # 20180724092134
            file_type, status = map_file_type(file_name, directory)
            file_list.append({"file": file_name, "createdAt": file_time, "timestamp": raw_time,
                              "type": file_type, "status": status})
    return file_list


def get_single_file_information(file_name):
    file_time = ''
    try:
        if not file_name.startswith('.'):  # ignore hidden files on Linux/UNIX
            dt = time.gmtime(os.path.getmtime(file_name))
            file_time = time.strftime('%Y%m%d%H%M%S', dt)  # 20180724092134
    except:
        logger.info('Could not find file ' + file_name)

    return file_time


def get_table_header(table_df):
    # Get an indexed header row
    df_header = pd.DataFrame(list(table_df))  # Get the header row only
    df_header = df_header.reset_index().to_dict(orient='list')
    mapping = {}
    print(df_header)
    for i in range(0, len(df_header['index'])):
        mapping[df_header[0][i]] = df_header['index'][i]
    return mapping


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
    table_df = pd.read_csv(file_name, sep="\t", header=0, encoding='utf-8')
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


def get_type_for_assay(df_row, assay_type):
    row = df_row.iloc[0]

    for cell in row:
        if cell != '' and cell != assay_type + '-assay':  # return first cell that is not the label
            return cell


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
            protocol_type=OntologyAnnotation(term=protocol_type),
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

    for file_loc in [upload_location, study_location]:  # Check both study and upload location
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


def to_isa_tab(study_id, input_folder, outout_folder):
    try:
        isa_convert(input_folder, outout_folder, study_id)
    except:
        return False, "Could not convert mzML to ISA-Tab study " + study_id

    return True, "ISA-Tab files generated for study " + study_id


def convert_to_isa(study_location, study_id):
    input_folder = study_location
    output_folder = study_location

    status, message = to_isa_tab(study_id, input_folder, output_folder)
    # if status:
    #     location = study_location
    #     files = glob.glob(os.path.join(location, 'i_Investigation.txt'))
    #     if files:
    #         file_path = files[0]
    #         filename = os.path.basename(file_path)
    #         try:
    #             return send_file(file_path, cache_timeout=-1,
    #                              as_attachment=True, attachment_filename=filename)
    #         except OSError as err:
    #             logger.error(err)
    #             return False, "Generated ISA-Tab i_Investigation.txt file could not be read."
    #     else:
    #         return False, "Generated ISA-Tab i_Investigation.txt file could not be found."
    # else:
    return status, message


def update_correct_sample_file_name(isa_study, study_location, study_id):
    #isa_study.identifier = study_id  # Adding the study identifier
    sample_file_name = isa_study.filename
    sample_file_name = os.path.join(study_location, sample_file_name)
    short_sample_file_name = 's_' + study_id.upper() + '.txt'
    default_sample_file_name = os.path.join(study_location, short_sample_file_name)
    if os.path.isfile(sample_file_name):
        if sample_file_name != default_sample_file_name:
            isa_study.identifier = study_id  # Adding the study identifier
            os.rename(sample_file_name, default_sample_file_name)  # Rename the sample file
            isa_study.filename = short_sample_file_name  # Add the new filename to the investigation

    return isa_study
