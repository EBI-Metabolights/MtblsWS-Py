import glob
import logging
import os
import shutil
import time
import datetime
from flask import current_app as app
import pandas as pd

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
        # copy origin to destiny
        logger.info("Copying %s to %s", source, destination)
        shutil.copyfile(source, destination)
    except Exception:
        raise


def copytree(src, dst, symlinks=False, ignore=None):
    try:
        if not os.path.exists(dst):
            os.makedirs(dst, exist_ok=True)
        for item in os.listdir(src):
            source = os.path.join(src, item)
            destination = os.path.join(dst, item)
            try:
                time_diff = os.stat(source).st_ctime - os.stat(destination).st_ctime
                #logger.info('Time difference is %s between %s and %s', time_diff, source, destination)

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


def copy_files_and_folders(source, destination):
    """
      Make a copy of files/folders from origin to destnation. If destination already exists, it will be replaced.
      :param source:  string containing the full path to the source file, including filename
      :param destination: string containing the path to the source file, including filename
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
