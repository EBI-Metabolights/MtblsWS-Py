import glob
import logging
import os
import shutil
import time
import datetime
import requests
from urllib.request import url2pathname, urlretrieve
from flask import current_app as app
from html.parser import HTMLParser
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


def copytree(src, dst, symlinks=False, ignore=None):
    try:
        if not os.path.exists(dst):
            os.makedirs(dst, exist_ok=True)
        for item in os.listdir(src):
            s = os.path.join(src, item)
            d = os.path.join(dst, item)
            try:
                time_diff = os.stat(s).st_mtime - os.stat(d).st_mtime
            except FileNotFoundError:
                time_diff = 1  # Destination folder does not exist

            if os.path.isdir(d):
                pass  # We already have this folder

            if int(time_diff) >= 1:
                if os.path.isdir(s):
                    shutil.copytree(s, d, symlinks, ignore)
                elif not os.path.exists(d):
                    shutil.copy2(s, d)

    except Exception:
        raise


def copy_files_and_folders(source, destination):
    """
      Make a copy of files/folders from origin to destnation. If destination already exists, it will be replaced.
      :param source:  string containing the full path to the source file, including filename
      :param destination: string containing the path to the source file, including filename
      :return:
      """
    try:
        # copy origin to destination
        logger.info("Copying %s to %s", source, destination)
        copytree(source, destination)
    except FileNotFoundError:
        return False, 'No files found under ' + source
    except IsADirectoryError:
        return False, 'Please give filename(s), not only upload folder ' + source
    except Exception:
        raise
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


def is_file_referenced(file_name, directory, isa_tab_file_to_check):
    isa_tab_file_to_check = isa_tab_file_to_check + '*.txt'
    isa_tab_file = os.path.join(directory, isa_tab_file_to_check)
    for ref_file_name in glob.glob(isa_tab_file):
        if file_name in open(ref_file_name).read():
            return True
        else:
            return False


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
            file_time = time.strftime('%Y%m%d%H%M%S', dt)  # 20180724092134
            file_type, status = map_file_type(file_name, directory)
            file_list.append({"file": file_name, "createdAt": file_time, "type": file_type, "status": status})
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


def get_table_header(table_df):
    # Get an indexed header row
    df_header = pd.DataFrame(list(table_df))  # Get the header row only
    df_header = df_header.reset_index().to_dict(orient='list')
    mapping = {}
    print(df_header)
    for i in range(0, len(df_header['index'])):
        mapping[df_header[0][i]] = df_header['index'][i]
    return mapping


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


def stream_files_to_browser(study_location, file_names):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    for file_name in file_names.split(','):
        url = 'file://' + os.path.join(study_location.strip(), file_name.strip())
        filename = file_name
        return urlretrieve(url, filename)

        # r = requests.session()
        # r.mount('file://', LocalFileAdapter())
        # r.get(url, stream=True)
        # with open(filename, 'wb') as f:
        #     for chunk in r..iter_content(chunk_size=1024):
        #         if chunk:  # filter out keep-alive new chunks
        #             f.write(chunk)
        #             # f.flush() commented by recommendation from J.F.Sebastian
        # return filename

        # r = requests.get(url, stream=True)
        # with open(filename, 'wb') as f:
        #     shutil.copyfileobj(r.raw, f)

        # return filename



class LocalFileAdapter(requests.adapters.BaseAdapter):
    """Protocol Adapter to allow Requests to GET file:// URLs

    @todo: Properly handle non-empty hostname portions.
    """

    @staticmethod
    def _chkpath(method, path):
        """Return an HTTP status for the given filesystem path."""
        if method.lower() in ('put', 'delete'):
            return 501, "Not Implemented"  # TODO
        elif method.lower() not in ('get', 'head'):
            return 405, "Method Not Allowed"
        elif os.path.isdir(path):
            return 400, "Path Not A File"
        elif not os.path.isfile(path):
            return 404, "File Not Found"
        elif not os.access(path, os.R_OK):
            return 403, "Access Denied"
        else:
            return 200, "OK"

    def send(self, req, **kwargs):  # pylint: disable=unused-argument
        """Return the file specified by the given request

        @type req: C{PreparedRequest}
        @todo: Should I bother filling `response.headers` and processing
               If-Modified-Since and friends using `os.stat`?
        """
        path = os.path.normcase(os.path.normpath(url2pathname(req.path_url)))
        response = requests.Response()

        response.status_code, response.reason = self._chkpath(req.method, path)
        if response.status_code == 200 and req.method.lower() != 'head':
            try:
                response.raw = open(path, 'rb')
            except (OSError, IOError) as err:
                response.status_code = 500
                response.reason = str(err)

        if isinstance(req.url, bytes):
            response.url = req.url.decode('utf-8')
        else:
            response.url = req.url

        response.request = req
        response.connection = self

        return response

    def close(self):
        pass
