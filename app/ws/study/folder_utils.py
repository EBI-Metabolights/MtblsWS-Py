import datetime
import glob
import json
import logging
import os
import os.path
import os.path
import time
from copy import deepcopy
from operator import itemgetter

from flask import current_app as app

from app.ws.utils import date_format, file_date_format, map_file_type, new_timestamped_folder, copy_file

logger = logging.getLogger("wslog")


def get_all_files_from_filesystem(study_id, obfuscation_code, study_location, directory=None,
                                  include_raw_data=None, assay_file_list=None, validation_only=False,
                                  short_format=None, include_sub_dir=None,
                                  static_validation_file=None):

    logger.info('Getting list of all files for MTBLS Study %s. Study folder: %s.', study_id, study_location)

    start_time = time.time()
    s_start_time = time.time()

    log_path = os.path.join(study_location, app.config.get('UPDATE_PATH_SUFFIX'), 'logs')
    if not os.path.exists(log_path):
        os.makedirs(log_path, mode=777, exist_ok=True)

    study_files, latest_update_time = get_all_files(study_location, directory=directory,
                                                    include_raw_data=include_raw_data,
                                                    assay_file_list=assay_file_list, validation_only=validation_only,
                                                    short_format=short_format, include_sub_dir=include_sub_dir,
                                                    static_validation_file=static_validation_file)
    logger.info("Listing study files for " + study_id + " took %s seconds" % round(time.time() - s_start_time, 2))

    # Sort the two lists
    study_files = sorted(study_files, key=itemgetter('file'))

    s_files = deepcopy(study_files)
    for row in s_files:
        row.pop('status')

    upload_diff = []
    ftp_private_study_folder = study_id.lower() + "-" + obfuscation_code
    ftp_private_relative_root_path = app.config.get("PRIVATE_FTP_RELATIVE_STUDIES_ROOT_PATH")
    ftp_private_relative_study_path = os.path.join(ftp_private_relative_root_path, ftp_private_study_folder)
    upload_location = [None, ftp_private_relative_study_path]
    upload_files = []
    logger.info("Listing all files for " + study_id + " took %s seconds" % round(time.time() - start_time, 2))

    return study_files, upload_files, upload_diff, upload_location, latest_update_time


def get_all_files(path, directory=None, include_raw_data=False, assay_file_list=None,
                  validation_only=False, short_format=None, include_sub_dir=None,
                  static_validation_file=None):
    try:
        files, latest_update_time = get_file_information(study_location=path, path=path, directory=directory,
                                                         include_raw_data=include_raw_data,
                                                         assay_file_list=assay_file_list,
                                                         validation_only=validation_only, short_format=short_format,
                                                         include_sub_dir=include_sub_dir,
                                                         static_validation_file=static_validation_file)
    except Exception as e:
        logger.warning('Could not find folder ' + path + '. Error: ' + str(e))
        files = []  # The upload folder for this study does not exist, this is normal
        latest_update_time = ''

    return files, latest_update_time


def get_file_information(study_location=None, path=None, directory=None, include_raw_data=False,
                         assay_file_list=None, validation_only=False, short_format=None,
                         include_sub_dir=None, static_validation_file=None):
    file_list = []
    file_name = ""
    ignore_file_list = app.config.get('IGNORE_FILE_LIST')
    latest_update_time = ""
    try:
        timeout_secs = app.config.get('FILE_LIST_TIMEOUT')
        end_time = time.time() + timeout_secs

        if directory:
            path = os.path.join(path, directory)

        tree_file_list = []
        try:
            tree_file_list, static_file_found = \
                list_directories(study_location, dir_list=[], base_study_location=study_location,
                                 short_format=short_format, validation_only=validation_only,
                                 include_sub_dir=include_sub_dir, static_validation_file=static_validation_file,
                                 include_raw_data=include_raw_data, ignore_file_list=ignore_file_list)
            # tree_file_list, folder_list = traverse_subfolders(
            #     study_location=study_location, file_location=path, file_list=tree_file_list, all_folders=[], full_path=True)

        except Exception as e:
            logger.error('Could not read all the files and folders. Error: ' + str(e))
            file_list = os.listdir(path)

        if validation_only and short_format and not static_file_found:
            for file_name in assay_file_list:
                if os.path.isfile(os.path.join(study_location, file_name)) and file_name not in tree_file_list:
                    tree_file_list.append(file_name)

        for entry in flatten_list(tree_file_list):
            # {'file': '20160728_033.raw', 'createdAt': '', 'timestamp': '', 'type': 'raw', 'status': 'active', 'directory': True}
            file_type = None
            file_time = None
            raw_time = None
            status = None
            folder = None
            if short_format and not static_file_found:  # The static file contains more info and is fast to read
                file_name = entry
            else:
                file_name = entry['file']
                file_type = entry['type']
                status = entry['status']
                folder = entry['directory']

            if time.time() > end_time:
                logger.error('Listing files in folder %s, timed out after %s seconds', path, timeout_secs)
                return file_list  # Return after xx seconds regardless

            if not file_name.startswith('.'):  # ignore hidden files on Linux/UNIX:
                if not include_raw_data:  # Only return metadata files
                    if file_name.startswith(('i_', 'a_', 's_', 'm_')):
                        file_time, raw_time, file_type, status, folder = \
                            get_file_times(path, file_name, validation_only=validation_only)
                else:
                    file_time, raw_time, file_type, status, folder = \
                        get_file_times(path, file_name, assay_file_list=assay_file_list,
                                       validation_only=validation_only)

                if directory:
                    if file_name.startswith(('i_', 'a_', 's_', 'm_')):
                        status = 'old'  # metadata files in a sub-directory are not active

                    file_name = os.path.join(directory, file_name)

                if file_type:
                    file_list.append({"file": file_name, "createdAt": file_time, "timestamp": raw_time,
                                      "type": file_type, "status": status, "directory": folder})
                    if latest_update_time == "":
                        latest_update_time = file_time
                    else:
                        latest_update_time = latest_update_time if datetime.datetime.strptime(latest_update_time,
                                                                                     "%B %d %Y %H:%M:%S") > datetime.datetime.strptime(
                            file_time, "%B %d %Y %H:%M:%S") \
                            else file_time
    except Exception as e:
        logger.error('Error in listing files under ' + path + '. Last file was ' + file_name)
        logger.error(str(e))

    return file_list, latest_update_time


def flatten_list(list_name, flat_list=None):
    # Now, with sub-folders we may have lists of lists, so flatten the structure
    if not flat_list:
        flat_list = []

    for entry in list_name:
        if isinstance(entry, list):
            for sub_entry in entry:
                if isinstance(sub_entry, list):
                    flatten_list(sub_entry, flat_list=flat_list)
                elif sub_entry not in flat_list and type(sub_entry) != bool:
                    flat_list.append(sub_entry)
        else:
            if type(entry) != bool and entry not in flat_list:
                flat_list.append(entry)
    return flat_list


def get_file_times(directory, file_name, assay_file_list=None, validation_only=False):
    file_time = ""
    raw_time = ""
    file_type = ""
    status = ""
    folder = ""
    try:
        if not validation_only:
            dt = time.gmtime(os.path.getmtime(os.path.join(directory, file_name)))
            raw_time = time.strftime(date_format, dt)  # 20180724092134
            file_time = time.strftime(file_date_format, dt)  # 20180724092134

        file_type, status, folder = map_file_type(file_name, directory, assay_file_list)
    except Exception as e:
        logger.error(str(e))
        print(str(e))

    return file_time, raw_time, file_type, status, folder


def get_basic_files(study_location, include_sub_dir, assay_file_list=None, metadata_only=False):
    file_list = []
    start_time = time.time()

    if include_sub_dir:
        file_list = list_directories_full(study_location, file_list, base_study_location=study_location)
        # file_list = list_directories(study_location, file_list, base_study_location=study_location, include_sub_dir=include_sub_dir)
    else:
        for entry in os.scandir(study_location):
            if not entry.name.startswith("."):
                file_name = entry.name
                fname, ext = os.path.splitext(file_name)
                if metadata_only and fname.startswith(('i_', 'a_', 's_', 'm_')) and (ext == '.txt' or ext == '.tsv'):
                    file_type, status, folder = map_file_type(file_name, study_location,
                                                              assay_file_list=assay_file_list)
                else:
                    file_type, status, folder = map_file_type(file_name, study_location,
                                                              assay_file_list=assay_file_list)
                name = entry.path.replace(study_location + os.sep, '')

                file_list.append({"file": name, "createdAt": "", "timestamp": "", "type": file_type,
                                  "status": status, "directory": folder})

    logger.info("Basic tree listing for all files for "
                + study_location + " took %s seconds" % round(time.time() - start_time, 2))
    return file_list


def list_directories_full(file_location, dir_list, base_study_location, assay_file_list=None):
    for entry in os.scandir(file_location):
        name = entry.path.replace(base_study_location + os.sep, '')
        file_type, status, folder = map_file_type(entry.name, file_location, assay_file_list=assay_file_list)
        dir_list.append({"file": name, "createdAt": "", "timestamp": "", "type": file_type,
                         "status": status, "directory": folder})
        if entry.is_dir():
            dir_list.extend(list_directories_full(entry.path, [], base_study_location))
    return dir_list


def list_directories(file_location, dir_list, base_study_location, assay_file_list=None,
                     short_format=None, include_sub_dir=None, validation_only=None,
                     static_validation_file=None, include_raw_data=None, ignore_file_list=None):
    static_file_found = False
    validation_files_list = os.path.join(file_location, 'validation_files.json')
    folder_exclusion_list = app.config.get('FOLDER_EXCLUSION_LIST')

    if os.path.isfile(validation_files_list) and static_validation_file:
        try:
            with open(validation_files_list, 'r', encoding='utf-8') as f:
                validation_files = json.load(f)
                static_file_found = True
        except Exception as e:
            logger.error(str(e))
        dir_list = validation_files
    else:
        for entry in os.scandir(file_location):
            file_type = None
            ignored_file = False

            if validation_only and not entry.is_dir():
                final_filename = os.path.basename(entry.name).lower()
                for ignore in ignore_file_list:
                    if ignore in final_filename:
                        ignored_file = True
                        break

            if ignored_file:
                continue

            if not entry.name.startswith('.'):
                name = entry.path.replace(base_study_location + os.sep, '')

                # Only map/check metadata files if include_raw_data is False
                if not include_raw_data and not name.startswith(('i_', 'a_', 's_', 'm_')):
                    continue

                file_type, status, folder = map_file_type(entry.name, file_location, assay_file_list=assay_file_list)

                if short_format:
                    if name not in folder_exclusion_list:
                        dir_list.append(name)
                else:
                    dir_list.append({"file": name, "createdAt": "", "timestamp": "", "type": file_type,
                                     "status": status, "directory": folder})

                if entry.is_dir():
                    if not include_sub_dir:
                        # if short_format and name in folder_exclusion_list:
                        if validation_only and name in folder_exclusion_list:
                            continue
                    else:
                        if file_type == 'audit':
                            continue

                        if validation_only and name in folder_exclusion_list:
                            continue

                        if file_type in ['raw', 'derived'] and validation_only:  # or validation only?
                            continue
                        else:
                            dir_list.extend(list_directories(entry.path, [], base_study_location,
                                                             assay_file_list=assay_file_list,
                                                             short_format=short_format,
                                                             include_sub_dir=include_sub_dir,
                                                             static_validation_file=static_validation_file,
                                                             include_raw_data=include_raw_data,
                                                             ignore_file_list=ignore_file_list))
    return dir_list, static_file_found


def write_audit_files(study_location):
    """
    Write back an ISA-API Investigation object directly into ISA-Tab files
    :param study_location: the filesystem where the study is located
    :return:
    """
    # dest folder name is a timestamp
    update_path_suffix = app.config.get('UPDATE_PATH_SUFFIX')
    update_path = os.path.join(study_location, update_path_suffix)
    log_path = os.path.join(update_path, 'logs')
    if not os.path.exists(log_path):
        os.makedirs(log_path, mode=777, exist_ok=True)
    dest_path = new_timestamped_folder(update_path)

    try:
        # make a copy of ISA-Tab & MAF
        for isa_file in glob.glob(os.path.join(study_location, "?_*.t*")):
            isa_file_name = os.path.basename(isa_file)
            src_file = isa_file
            dest_file = os.path.join(dest_path, isa_file_name)
            logger.info("Copying %s to %s", src_file, dest_file)
            copy_file(src_file, dest_file)
    except:
        return False, dest_path

    return True, dest_path
