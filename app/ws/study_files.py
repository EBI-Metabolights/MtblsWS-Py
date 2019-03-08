from flask import request, abort
from flask.json import jsonify
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import *
from app.ws.isaApiClient import IsaApiClient
from operator import itemgetter
from marshmallow import ValidationError
import json

logger = logging.getLogger('wslog')
wsc = WsClient()
iac = IsaApiClient()


class StudyFiles(Resource):

    @swagger.operation(
        summary="Get a list of all files in the study and upload folder(s)",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "include_raw_data",
                "description": "Include raw data files in the list. False = only list ISA-Tab metadata files.",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True
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
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self, study_id):

        # param validation
        if study_id is None:
            abort(404)

        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # If false, only sync ISA-Tab metadata files
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('include_raw_data', help='Include raw data')
        include_raw_data = False

        if request.args:
            args = parser.parse_args(req=request)
            include_raw_data = False if args['include_raw_data'].lower() != 'true' else True

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        logger.info('Getting list of all files for MTBLS Study %s', study_id)
        upload_location = app.config.get('MTBLS_FTP_ROOT') + study_id.lower() + "-" + obfuscation_code
        logger.info('Getting list of all files for MTBLS Study %s. Study folder: %s. Upload folder: %s', study_id,
                    study_location, upload_location)
        study_files = get_all_files(study_location, include_raw_data)
        upload_files = get_all_files(upload_location, include_raw_data)

        # Sort the two lists
        study_files, upload_files = [sorted(l, key=itemgetter('file')) for l in (study_files, upload_files)]

        upload_diff = [dict(i) for i in
                       {frozenset(row.items()) for row in upload_files} -
                       {frozenset(row.items()) for row in study_files}]

        upload_location = upload_location.split('/mtblight')  # FTP/Aspera root starts here

        return jsonify({'studyFiles': study_files,
                        'upload': upload_diff,
                        'upload_all': upload_files,
                        'upload_location': upload_location[1],
                        'obfuscation_code': obfuscation_code})


class CopyFilesFolders(Resource):
    @swagger.operation(
        summary="Copy files from upload folder to study folder",
        nickname="Copy from upload folder",
        notes="Copies files/folder from the upload directory to the study directory",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "include_raw_data",
                "description": "Include raw data file transfer. False = only copy ISA-Tab metadata files.",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True
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
                "message": "OK. Files/Folders were copied across."
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self, study_id):

        # param validation
        if study_id is None:
            abort(404, 'Please provide valid parameter for study identifier')
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('include_raw_data', help='Include raw data')
        include_raw_data = False

        # If false, only sync ISA-Tab metadata files
        if request.args:
            args = parser.parse_args(req=request)
            include_raw_data = False if args['include_raw_data'].lower() != 'true' else True

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        status = wsc.create_upload_folder(study_id, obfuscation_code, user_token)
        upload_location = status["os_upload_path"]

        logger.info("For %s we use %s as the upload path. The study path is %s", study_id, upload_location, study_location)
        status, message = copy_files_and_folders(upload_location, study_location,
                                                 include_raw_data=include_raw_data, include_investigation_file=False)
        if status:
            return {'Success': 'Copied files from ' + upload_location}
        else:
            return {'Warning': message}


class SampleStudyFiles(Resource):
    @swagger.operation(
        summary="Get a list of all sample names, mapped to files in the study and upload folder(s)",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
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
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self, study_id):

        # param validation
        if study_id is None:
            abort(404)

        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        upload_location = app.config.get('MTBLS_FTP_ROOT') + study_id.lower() + "-" + obfuscation_code

        # Get all unique file names
        all_files = get_files(get_all_files(study_location, include_raw_data=True)) + \
                    get_files(get_all_files(upload_location, include_raw_data=True))

        try:
            isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                             skip_load_tables=False,
                                                             study_location=study_location)
        except:
            abort(500, "Could not load the study metadata files")

        samples_and_files = []
        for sample in isa_study.samples:
            s_name = sample.name
            # For each sample, loop over all the files
            # Todo, some studies has lots more files than samples, consider if this should be reversed
            for f_name in all_files:
                filename, file_extension = os.path.splitext(f_name)
                s = s_name.lower()
                f = filename.lower()

                f_clean = clean_name(f)
                s_clean = clean_name(s)

                # Now, let's try to match up
                if f == s:  # File name and sample name is an exact match
                    samples_and_files.append({"sample_name": s_name, "file_name": f_name, "reliability": "exact_match"})
                elif s in f or f in s:  # Sample name appears in the file name, or file name appears in sample name
                    samples_and_files.append({"sample_name": s_name, "file_name": f_name, "reliability": "pattern"})
                elif s_clean in f_clean or f_clean in s_clean:  # Sample name appears in the file name, and other way
                    samples_and_files.append({"sample_name": s_name, "file_name": f_name, "reliability": "fuzzy"})

        return jsonify({'sample_files': samples_and_files})


class DeleteFiles(Resource):
    @swagger.operation(
        summary="Delete files from a given folder",
        nickname="Delete files",
        notes='''Delete files and folders from the study and/or upload folder<pre><code>
{    
    "files": [
        {"filename": "a_MTBLS123_LC-MS_positive_hilic_metabolite_profiling.txt"},
        {"filename": "Raw-File-001.raw"}
    ]
}</pre></code></br> 
"file_location" is one of: "study" (study folder), "upload" (upload folder) or "both" ''',
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "files",
                "description": 'Files to delete',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "file_location",
                "description": "Location of the file (study, upload, both)",
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
                "message": "OK. Files/Folders were removed."
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def delete(self, study_id):

        # param validation
        if study_id is None:
            abort(404, 'Please provide valid parameter for study identifier')
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('files', help='files')
        parser.add_argument('file_location', help='file_location')
        file_location = 'study'
        files = None

        if request.args:
            args = parser.parse_args(req=request)
            files = args['files'] if args['files'] else None
            file_location = args['file_location'] if args['file_location'] else None

        # body content validation
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['files']
            if data is None:
                abort(412)
            files = data
        except (ValidationError, Exception):
            abort(400, 'Incorrect JSON provided')

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        status = False
        message = None
        upload_location = app.config.get('MTBLS_FTP_ROOT') + study_id.lower() + "-" + obfuscation_code

        for file in files:
            f_name = file["filename"]
            try:
                if file_location == "study":
                    status, message = remove_file(study_location, f_name)
                elif file_location == "upload":
                    status, message = remove_file(upload_location, f_name)
                elif file_location == "both":
                    s_status, s_message = remove_file(study_location, f_name)
                    u_status, u_message = remove_file(upload_location, f_name)
                    if s_status or u_status:
                        return {'Success': "File " + f_name + " deleted"}
                    else:
                        return {'Error': "Can not find and/or delete file " + f_name + " in the study or upload folder"}

                if not status:
                    return {'Error': message}
            except:
                return {'Error': message}

        if status:
            return {'Success': message}
        else:
            return {'Error': message}


def clean_name(name):
    name = name.replace('_', '')
    name = name.replace('-', '')
    name = name.replace('&', '')
    name = name.replace('#', '')
    return name


def get_files(file_list):
    all_files = []
    for files in file_list:
        f_type = files["type"]  # Todo, don't use reserved words!
        f_name = files["file"]
        if f_type == 'raw' or f_type == 'derived' or f_type == 'compressed' or f_type == 'unknown':
            if f_name not in all_files:
                all_files.append(f_name)
    return all_files


def get_all_files(path, include_raw_data=False):
    try:
        files = get_file_information(path, include_raw_data)
    except:
        logger.error('Could not find folder ' + path)
        files = []  # The upload folder for this study does not exist, this is normal
    return files


def get_file_information(directory, include_raw_data):
    file_list = []
    try:
        timeout_secs = app.config.get('FILE_LIST_TIMEOUT')
        end_time = time.time() + timeout_secs
        for file_name in os.listdir(directory):
            file_time = None
            raw_time = None
            file_type = None
            status = None
            if time.time() > end_time:
                logger.error('Listing files in folder %s, timed out after %s seconds', directory, timeout_secs)
                return file_list  # Return after xx seconds regardless

            if not file_name.startswith('.'):  # ignore hidden files on Linux/UNIX:
                if not include_raw_data:  # Only return metadata files
                    if file_name.startswith(('i_', 'a_', 's_', 'm_')):
                        file_time, raw_time, file_type, status = get_file_times(directory, file_name)
                else:
                    file_time, raw_time, file_type, status = get_file_times(directory, file_name)

                if file_time:
                    file_list.append({"file": file_name, "createdAt": file_time, "timestamp": raw_time,
                                      "type": file_type, "status": status})
    except Exception as e:
        logger.error('Error in listing files under ' + directory + '. Last file was ' + file_name)
        logger.error(str(e))

    return file_list


def get_file_times(directory, file_name):
    dt = time.gmtime(os.path.getmtime(os.path.join(directory, file_name)))
    raw_time = time.strftime(date_format, dt)  # 20180724092134
    file_time = time.strftime(file_date_format, dt)  # 20180724092134
    file_type, status = map_file_type(file_name, directory)

    return file_time, raw_time, file_type, status


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
        try:
            if file_name in io.open(ref_file_name, 'r', encoding='utf8', errors="ignore").read():
                found = True
        except Exception as e:
            logger.error('File Format error? Cannot read or open file ' + file_name)
            logger.error(str(e))

    return found
