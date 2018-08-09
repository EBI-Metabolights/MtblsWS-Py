import glob
import logging
import os
import shutil
import time
import datetime
from flask import current_app as app
from html.parser import HTMLParser

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
    os.makedirs(new_folder)
    return new_folder


def copy_file(source, destiny):
    """
    Make a copy of origin to destiny. If destiny already exists, it will be replaced.
    :param source:  string containing the full path to the source file, including filename
    :param destiny: string containing the path to the source file, including filename
    :return:
    """
    try:
        # copy origin to destiny
        logger.info("Copying %s to %s", source, destiny)
        shutil.copyfile(source, destiny)
    except Exception:
        raise


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
        logger.info("Moving %s to %s", src_file, dest_file)
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


def get_file_information(directory):
    file_list = []
    for file_name in os.listdir(directory):
        dt = time.gmtime(os.path.getmtime(os.path.join(directory, file_name)))
        file_time = time.strftime('%Y%m%d%H%M%S', dt)  # 20180724092134
        file_list.append({"file": file_name, "createdAt": file_time})
    return file_list


class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)


def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()
