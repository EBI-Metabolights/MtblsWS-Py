import os
import logging
from shutil import copyfile
import time

"""
Utils

Misc of utils

author: jrmacias@ebi.ac.uk
date: 20170301
"""


def get_timestamp():
    """
    Get a string with the current date & time as a timestamp i.e. 20170302143045
    :return: %Y%m%d%H%M%S - full year, two digit month, day, 24 hour, minutes and seconds
    """
    return time.strftime("%Y%m%d%H%M%S")


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
    # TODO check source exists, and log accordingly
    # TODO check destiny exists, and log accordingly
    # copy origin to destiny
    try:
        copyfile(source, destiny)
    except Exception:
        # TODO , add logging
        raise

    return
