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

import json
import zipfile
from copy import deepcopy
from operator import itemgetter
from os import scandir

from flask.json import jsonify
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from marshmallow import ValidationError
from dirsync import sync
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsStudy import write_audit_files
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import *
from datetime import datetime

logger = logging.getLogger('wslog')
wsc = WsClient()
iac = IsaApiClient()


def get_all_files_from_filesystem(study_id, obfuscation_code, study_location, directory=None,
                                  include_raw_data=None, assay_file_list=None, validation_only=False,
                                  include_upload_folder=True, short_format=None, include_sub_dir=None,
                                  static_validation_file=None):
    upload_location = app.config.get('MTBLS_FTP_ROOT') + study_id.lower() + "-" + obfuscation_code
    logger.info('Getting list of all files for MTBLS Study %s. Study folder: %s. Upload folder: %s', study_id,
                study_location, upload_location)

    start_time = time.time()
    s_start_time = time.time()
    study_files, latest_update_time = get_all_files(study_location, directory=directory,
                                                    include_raw_data=include_raw_data,
                                                    assay_file_list=assay_file_list, validation_only=validation_only,
                                                    short_format=short_format, include_sub_dir=include_sub_dir,
                                                    static_validation_file=static_validation_file)
    logger.info("Listing study files for " + study_id + " took %s seconds" % round(time.time() - s_start_time, 2))
    upload_files = []
    if include_upload_folder:
        u_start_time = time.time()

        # Does the private FTP folder exist?
        try:
            os.stat(upload_location)
        except:
            os.mkdir(upload_location)

        upload_files, latest_update_time = get_all_files(upload_location, directory=directory,
                                                         include_raw_data=include_raw_data,
                                                         validation_only=validation_only, short_format=short_format,
                                                         static_validation_file=static_validation_file)
        logger.info("Listing upload files for " + study_id + " took %s seconds" % round(time.time() - u_start_time, 2))

    # Sort the two lists
    study_files, upload_files = [sorted(l, key=itemgetter('file')) for l in (study_files, upload_files)]

    # Now, the status for each file can differ from upload and study folder as only study files are
    # referenced in the ISA-Tab files. If they are referenced, they get status 'active'
    u_files = deepcopy(upload_files)
    for row in u_files:
        row.pop('status')

    s_files = deepcopy(study_files)
    for row in s_files:
        row.pop('status')

    upload_diff = [dict(i) for i in
                   {frozenset(row.items()) for row in u_files} -
                   {frozenset(row.items()) for row in s_files}]

    upload_location = upload_location.split('/mtblight')  # FTP/Aspera root starts here

    logger.info("Listing all files for " + study_id + " took %s seconds" % round(time.time() - start_time, 2))

    return study_files, upload_files, upload_diff, upload_location, latest_update_time


class StudyFiles(Resource):
    @swagger.operation(
        summary="Get a list, with timestamps, of all files in the study and upload folder(s)",
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
                "name": "directory",
                "description": "List first level of files in a sub-directory",
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
        parser.add_argument('directory', help='List files in sub-directory')
        include_raw_data = False
        directory = None

        if request.args:
            args = parser.parse_args(req=request)
            include_raw_data = False if args['include_raw_data'].lower() != 'true' else True
            directory = args['directory'] if args['directory'] else None

        if directory and directory.startswith(os.sep):
            abort(401, "You can only specify folders in the current study folder")

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        study_files, upload_files, upload_diff, upload_location, latest_update_time = \
            get_all_files_from_filesystem(study_id, obfuscation_code, study_location,
                                          directory=directory, include_raw_data=include_raw_data,
                                          assay_file_list=get_assay_file_list(study_location),
                                          static_validation_file=False)

        return jsonify({'study': study_files,
                        'latest': upload_diff,
                        'private': upload_files,
                        'uploadPath': upload_location[1],
                        'obfuscationCode': obfuscation_code})

    # 'uploadPath': upload_location[0], for local testing

    @swagger.operation(
        summary="Delete files from a given folder",
        nickname="Delete files",
        notes='''Delete files and folders from the study and/or upload folder<pre><code>
{    
    "files": [
        {"name": "a_MTBLS123_LC-MS_positive_hilic_metabolite_profiling.txt"},
        {"name": "Raw-File-001.raw"}
    ]
}</pre></code></br> 
"file_location" is one of: "study" (study folder), "upload" (upload folder) or "both". </br>
Please note you can not delete <b>active</b> metadata files (i_*.txt, s_*.txt, a_*.txt and m_*.tsv) 
without setting the "force" parameter to True''',
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
                "name": "location",
                "description": "Location of the file (study, upload, both)",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "force",
                "description": "Allow removal of active metadata files.",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": False,
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
    def post(self, study_id):

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
        parser.add_argument('location', help='Location')
        parser.add_argument('force', help='Remove active metadata files')
        file_location = 'study'
        files = None
        allways_remove = False

        # If false, only sync ISA-Tab metadata files
        if request.args:
            args = parser.parse_args(req=request)
            files = args['files'] if args['files'] else None
            file_location = args['location'] if args['location'] else 'study'
            allways_remove = False if args['force'].lower() != 'true' else True

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

        audit_status, dest_path = write_audit_files(study_location)

        for file in files:
            try:
                f_name = file["name"]

                if f_name.startswith('i_') and f_name.endswith('.txt') and not is_curator:
                    return {'Error': "Only MetaboLights curators can remove the investigation file"}

                if file_location == "study":
                    status, message = remove_file(study_location, f_name, allways_remove)
                elif file_location == "upload":
                    status, message = remove_file(upload_location, f_name, allways_remove)
                elif file_location == "both":
                    s_status, s_message = remove_file(study_location, f_name, allways_remove)
                    u_status, u_message = remove_file(upload_location, f_name, allways_remove)
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


class CopyFilesFolders(Resource):
    @swagger.operation(
        summary="Copy files from upload folder to study folder",
        nickname="Copy from upload folder",
        notes="""Copies files/folder from the upload directory to the study directory</p> 
        Note that MetaboLights curators will also trigger a copy of any new investigation file!</p>
        </p><pre><code>If you only want to copy, or rename, a specific file please use the files field: </p> 
    { 
        "files": [
            { 
                "from": "filename2.ext", 
                "to": "filename.ext" 
            }
        ]
    }
</code></pre>
              """,
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
                "name": "file_location",
                "description": "Alternative EMBL-EBI filesystem location for raw files, default is private FTP",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "files",
                "description": "Only copy specific files",
                "paramType": "body",
                "type": "string",
                "required": False,
                "allowMultiple": False
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
    def post(self, study_id):
        log_request(request)
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
        parser.add_argument('file_location', help='Alternative file location')
        include_raw_data = False
        file_location = None

        # If false, only sync ISA-Tab metadata files
        if request.args:
            args = parser.parse_args(req=request)
            include_raw_data = False if args['include_raw_data'].lower() != 'true' else True
            file_location = args['file_location']

        # body content validation
        files = {}
        single_files_only = False
        status = False
        if request.data:
            try:
                data_dict = json.loads(request.data.decode('utf-8'))
                files = data_dict['files']
                single_files_only = True
            except KeyError:
                logger.info("No 'files' parameter was provided.")

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        status = wsc.create_upload_folder(study_id, obfuscation_code, user_token)
        upload_location = status["os_upload_path"]
        if file_location:
            upload_location = file_location

        logger.info("For %s we use %s as the upload path. The study path is %s", study_id, upload_location,
                    study_location)

        audit_status, dest_path = write_audit_files(study_location)
        if single_files_only and len(files) >= 1:
            for file in files:
                try:
                    from_file = file["from"]
                    to_file = file["to"]
                    source_file = os.path.join(upload_location, to_file)
                    destination_file = os.path.join(study_location, to_file)

                    logger.info("Copying specific file %s to %s", from_file, to_file)

                    if not from_file or not to_file:
                        abort(417, "Please provide both 'from' and 'to' file parameters")

                    if from_file != to_file:
                        if os.path.isfile(source_file):
                            logger.info(
                                "The filename/folder you are copying to (%s) already exists in the upload folder, deleting first",
                                to_file)
                            os.remove(source_file, source_file)
                        else:
                            logger.info("Renaming file %s to %s", from_file, to_file)
                            os.rename(os.path.join(upload_location, from_file), source_file)

                    if os.path.isdir(source_file):
                        logger.info(source_file + ' is a directory')
                        try:
                            if os.path.exists(destination_file) and os.path.isdir(destination_file):
                                logger.info('Removing directory ' + destination_file)
                                shutil.rmtree(destination_file)  # Remove the destination file/folder first

                            logger.info("Copying folder '%s' to study folder '%s'", source_file, destination_file)
                            shutil.copytree(source_file, destination_file)
                            status = True
                        except OSError as e:
                            logger.error('Folder already exists? Can not copy %s to %s',
                                         source_file, destination_file, str(e))
                    else:
                        logger.info("Copying file %s to study folder %s", to_file, study_location)
                        shutil.copy2(source_file, destination_file)
                        status = True
                except Exception as e:
                    logger.error('File copy failed with error ' + str(e))

        else:
            logger.info("Copying all newer files from '%s' to '%s'", upload_location, study_location)
            include_inv = False
            if is_curator:
                include_inv = True
            status, message = copy_files_and_folders(upload_location, study_location,
                                                     include_raw_data=include_raw_data,
                                                     include_investigation_file=include_inv)

        if status:
            reindex_status, message = wsc.reindex_study(study_id, user_token)
            return {'Success': 'Copied files from ' + upload_location}
        else:
            return {'Warning': message}

class SyncFolder(Resource):
    @swagger.operation(
        summary="Copy files from study folder to private FTP  folder",
        nickname="Copy from study folder",
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
                "name": "directory_name",
                "description": "Only copy directory",
                "paramType": "query",
                "type": "string",
                "required": True,
                "allowMultiple": False
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
    def post(self, study_id):
        log_request(request)
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
        parser.add_argument('directory_name', help='Alternative file location')

        # If false, only sync ISA-Tab metadata files
        if request.args:
            args = parser.parse_args(req=request)
            directory_name = args['directory_name']

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        destination = app.config.get('MTBLS_FTP_ROOT') + study_id.lower() + '-' + obfuscation_code + "/" + directory_name
        source = study_location + "/" + directory_name
        logger.info("syncing files from " + source + " to " + destination)
        try:
            if not os.path.exists(destination):
                os.makedirs(destination)
                os.chmod(destination, 0o777)
            sync(source, destination, 'sync', purge=False, logger=logger)
            logger.info('Copied file %s to %s', source, destination)
            return {'Success': 'Copied files from study folder to  ftp folder'}
        except OSError as e:
            logger.error('Does the folder already exists? Can not copy %s to %s', source,
                         destination, str(e))
        except FileExistsError as e:
            logger.error('Folder already exists! Can not copy %s to %s', source, destination,
                         str(e))
        except Exception as e:
            logger.error('Other error! Can not copy %s to %s', source, destination, str(e))


class SampleStudyFiles(Resource):
    @swagger.operation(
        summary="Get a list of all sample names, mapped to files in the study and upload folder(s)",
        notes="A prefect match gives reliability score of '1.0'. Use the highest score possible for matching",
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
        all_files = get_files(get_all_files(study_location, include_raw_data=True,
                                            assay_file_list=get_assay_file_list(study_location))) + \
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
                    samples_and_files.append({"sample_name": s_name, "file_name": f_name, "reliability": "1.0"})
                elif s in f or f in s:  # Sample name appears in the file name, or file name appears in sample name
                    samples_and_files.append({"sample_name": s_name, "file_name": f_name, "reliability": "0.9"})
                elif s_clean in f_clean or f_clean in s_clean:  # Sample name appears in the file name, and other way
                    samples_and_files.append({"sample_name": s_name, "file_name": f_name, "reliability": "0.5"})

        return jsonify({'sample_files': samples_and_files})


class UnzipFiles(Resource):
    @swagger.operation(
        summary="Unzip files in the study folder",
        nickname="Unzip files",
        notes='''Unzip files in the study folder<pre><code>
    {    
        "files": [
            {"name": "Raw_files1.zip"},
            {"name": "Folders.zip"}
        ]
    }</pre></code>
    </p>
    Please note that we will not extract "i_Investigation.txt" files into the main study folder.'''
        ,
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
                "description": 'Files to unzip',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "force",
                "description": "Remove zip files after extraction",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": False,
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
                "message": "OK. Files unzipped."
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
    def post(self, study_id):

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
        parser.add_argument('force', help='Remove zip files')
        files = None
        remove_zip = False

        # If false, only sync ISA-Tab metadata files
        if request.args:
            args = parser.parse_args(req=request)
            files = args['files'] if args['files'] else None
            remove_zip = False if args['force'].lower() != 'true' else True

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

        audit_status, dest_path = write_audit_files(study_location)

        inv_message = ""

        for file in files:
            f_name = file["name"]
            try:
                with zipfile.ZipFile(os.path.join(study_location, f_name), "r") as zip_ref:
                    # zip_ref.extractall(study_location)
                    list_of_file_names = zip_ref.namelist()
                    for file_name in list_of_file_names:
                        if not (file_name.startswith('i_') and file_name.endswith('.txt')):
                            # Extract a single file from zip
                            zip_ref.extract(file_name, path=study_location)
                        else:
                            inv_message = '. Investigation file not extracted'

            except Exception as e:
                msg = 'Could not extract zip file ' + f_name
                logger.error(msg + ":" + str(e))
                return {'Error': msg}

            try:
                if remove_zip:
                    remove_file(study_location, f_name, allways_remove=True)
            except:
                msg = 'Could not remove zip file ' + f_name
                logger.error(msg)
                return {'Error': msg}

        ret_msg = 'Files unzipped' + inv_message
        if remove_zip:
            ret_msg = 'Files unzipped and removed' + inv_message

        return {'Success': ret_msg}


def clean_name(name):
    name = name.replace('_', '')
    name = name.replace('-', '')
    name = name.replace('&', '')
    name = name.replace('#', '')
    name = name.replace('.', '')
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
                        latest_update_time = latest_update_time if datetime.strptime(latest_update_time,
                                                                                     "%B %d %Y %H:%M:%S") > datetime.strptime(
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
        for entry in scandir(study_location):
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
    for entry in scandir(file_location):
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
        for entry in scandir(file_location):
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


class StudyFilesTree(Resource):
    @swagger.operation(
        summary="Get a basic list of all files, and subdirectories, in the study folder",
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
                "name": "include_sub_dir",
                "description": "Include files in all sub-directories. False = only list files in the study folder",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True
            },
            {
                "name": "directory",
                "description": "List first level of files in a sub-directory",
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
        parser.add_argument('include_sub_dir', help='include files in all sub-directories')
        parser.add_argument('directory', help='List files in a specific sub-directory')
        include_sub_dir = False
        directory = None

        if request.args:
            args = parser.parse_args(req=request)
            include_sub_dir = False if args['include_sub_dir'].lower() != 'true' else True
            directory = args['directory'] if args['directory'] else None

        if directory and directory.startswith(os.sep):
            abort(401, "You can only specify folders in the current study folder")

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        upload_location = app.config.get('MTBLS_FTP_ROOT') + study_id.lower() + "-" + obfuscation_code
        upload_location = upload_location.split('/mtblight')

        if directory:
            study_location = os.path.join(study_location, directory)

        try:
            file_list = get_basic_files(study_location, include_sub_dir, get_assay_file_list(study_location))
        except MemoryError:
            abort(408)

        return jsonify({'study': file_list, 'latest': [], 'private': [],
                        'uploadPath': upload_location[1], 'obfuscationCode': obfuscation_code})



class FileList(Resource):
    @swagger.operation(
        summary="Get a listof all files and directories  for the given location",
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
                "name": "directory_name",
                "description": "return list of files form this directory",
                "paramType": "query",
                "type": "string",
                "required": False,
                "allowMultiple": False
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

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('directory_name', help='Alternative file location')
        directory_name =""
        # If false, only sync ISA-Tab metadata files
        if request.args:
            args = parser.parse_args(req=request)
            directory_name = args['directory_name']


        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        source = study_location + "/" + directory_name
        files_list = []
        dir_list = []
        for root, dirs, files in os.walk(source):
            for filename in files:
                file = {'file': filename, 'path': os.path.join(source, filename)}
                files_list.append(file)
            for dirname in dirs:
                dir = {'directory': dirname, 'path': os.path.join(source, dirname)}
                dir_list.append(dir)
            break

        return jsonify({'files': files_list,
                        'directories': dir_list})