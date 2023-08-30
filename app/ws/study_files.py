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
import datetime
import glob
import json
import logging
import os
import pathlib
import re
import shutil
import time
from typing import Dict, List, Set

from isatools import isatab
from isatools.model import Investigation, Study, Assay
from flask import request, abort
from flask.json import jsonify
from flask_restful import abort, Resource, reqparse
from flask_restful_swagger import swagger
from jsonschema.exceptions import ValidationError
import pandas as pd
from pydantic import BaseModel
from app.config import get_settings
from app.config.utils import get_private_ftp_relative_root_path

from app.services.storage_service.storage_service import StorageService
from app.tasks.datamover_tasks.basic_tasks.file_management import list_directory
from app.tasks.datamover_tasks.curation_tasks import data_file_operations
from app.utils import MetabolightsException, metabolights_exception_handler
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.settings.utils import get_study_settings
from app.ws.study.folder_utils import get_all_files_from_filesystem, get_all_files, write_audit_files
from app.ws.study.study_service import StudyService, identify_study_id
from app.ws.study.user_service import UserService
from app.ws.study_folder_utils import FileMetadata, LiteFileSearchResult, evaluate_files, get_directory_files, get_referenced_file_set, sortFileMetadataList
from app.ws.utils import delete_remote_file, get_assay_file_list, remove_file, log_request

logger = logging.getLogger('wslog')
wsc = WsClient()
iac = IsaApiClient()


class StudyFiles(Resource):
    @swagger.operation(
        summary="Get a list, with timestamps, of all files in the study folder",
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
    def get(self, study_id: str):

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

        relative_studies_root_path = get_private_ftp_relative_root_path()
        folder_name = f'{study_id.lower()}-{obfuscation_code}'
        upload_path = os.path.join(os.sep, relative_studies_root_path.lstrip(os.sep), folder_name)

        return jsonify({'study': study_files,
                        'latest': [],
                        'private': [],
                        'uploadPath': upload_path,
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
        always_remove = False

        # If false, only sync ISA-Tab metadata files
        if request.args:
            args = parser.parse_args(req=request)
            files = args['files'] if args['files'] else None
            file_location = args['location'] if args['location'] else 'study'
            always_remove = False if args['force'].lower() != 'true' else True
            
        if file_location not in ["study", "upload"]:
            abort(400, error='Location is invalid')
        # body content validation
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['files']
            if data is None:
                abort(412)
            files = data
        except (ValidationError, Exception):
            abort(400, error='Incorrect JSON provided')

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403, error='Not authorized')

        errors = []
        
        deleted_files = []
        if file_location == "study":
            pattern = re.compile('([asi]_.*\.txt)|(m_.*\.tsv)')
            metadata_update = False
            for file in files:
                f_name = file["name"]
                match =  pattern.match(f_name)
                if match:
                    match_result = match.groups()
                    result =  match_result[0]
                    metadata_update = True
            if metadata_update:            
                write_audit_files(study_location)

            status = False
            message = None
            for file in files:
                try:
                    f_name = file["name"]

                    if f_name.startswith('i_') and f_name.endswith('.txt') and not is_curator:
                        errors.append({"status": "error", 'message': "Only MetaboLights curators can remove the investigation file", 'file': f_name})  
                    elif file_location == "study":
                        status, message = remove_file(study_location, f_name, always_remove)
                        if not status:
                            errors.append({"status": "error", 'message': message, 'file': f_name})
                        else:
                            deleted_files.append({"status": "success", 'message': message, 'file': f_name})
                except Exception as exc:
                    errors.append({"status": "error", 'message': str(exc), 'file': f_name})
                    
            return {"errors": errors, "deleted_files": deleted_files}
        else:
            ftp_root_path = get_settings().hpc_cluster.datamover.mounted_paths.cluster_private_ftp_root_path
            for file in files:
                try:
                    f_name = file["name"]
                    study_folder_root_path = os.path.join(ftp_root_path, f"{study_id.lower()}-{obfuscation_code}")
                    file_path = os.path.join(study_folder_root_path, f_name)
                    status, message = delete_remote_file(study_folder_root_path, file_path)

                    if not status:
                        errors.append({"status": "error", 'message': message, 'file': f_name})
                    else:
                        deleted_files.append({"status": "success", 'message': message, 'file': f_name})
                except Exception as exc:
                    errors.append({"status": "error", 'message': str(exc), 'file': f_name})
                    
            return {"errors": errors, "deleted_files": deleted_files}
            
        
class StudyRawAndDerivedDataFiles(Resource):
    @swagger.operation(
        summary="Search raw and derived data files in the study",
        notes="""
        Search data files in in the study. 
        """,
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
                "name": "search_pattern",
                "description": "search pattern (FILES/*.mzML, FILES/*.zip, FILES/**/*.d etc.). Default is FILES/*",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "default": 'FILES/*'
            },
            {
                "name": "file_match",
                "description": "Folder file matches",
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
                "default": True
            },
            {
                "name": "folder_match",
                "description": "Search folder matches",
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
                "default": False
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
    @metabolights_exception_handler
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
        parser.add_argument('search_pattern', help='Search pattern')
        parser.add_argument('folder_match', help='Folder matches')
        parser.add_argument('file_match', help='File matches')
        settings = get_study_settings()
        
        data_files_subfolder = settings.readonly_files_symbolic_link_name
        search_pattern = f'{data_files_subfolder}/*'
        file_match = False
        folder_match = False
        if request.args:
            args = parser.parse_args(req=request)
            file_match = True if args['file_match'].lower() == "true" else False
            folder_match = True if args['folder_match'].lower() == "true" else False
            if not folder_match and not file_match:
                raise MetabolightsException(http_code=401, message="At least one of them should be True: file_match, folder_match")
            search_pattern = f"{args['search_pattern']}" if 'search_pattern' in args and args['search_pattern'] else search_pattern
            if '..' + os.path.sep in search_pattern or '.' + os.path.sep in search_pattern:
                raise MetabolightsException(http_code=401, message="Relative folder search patterns (., ..) are not allowed")
            search_pattern_prefix = f'{data_files_subfolder}/'
            if not search_pattern.startswith(search_pattern_prefix):
                raise MetabolightsException(http_code=401, message=f"Search pattern should start with {f'{data_files_subfolder}/'}")

        UserService.get_instance().validate_user_has_curator_role(user_token)
        StudyService.get_instance().get_study_by_acc(study_id)
        settings = get_study_settings()
        study_folder = os.path.join(settings.mounted_paths.study_metadata_files_root_path, study_id)
        search_subfolder = os.path.join(settings.mounted_paths.study_metadata_files_root_path, study_id, data_files_subfolder)
        search_path = os.path.join(settings.mounted_paths.study_metadata_files_root_path, study_id, search_pattern)
        ignore_list = self.get_ignore_list(search_path)

        glob_search_result = glob.glob(os.path.join(search_subfolder, search_path), recursive=True)
        search_results = [os.path.abspath(file) for file in glob_search_result if not os.path.isdir(file)]
        excluded_folders = get_settings().file_filters.folder_exclusion_list

        excluded_folder_set = set(
            [os.path.basename(os.path.abspath(os.path.join(study_folder, file))) for file in excluded_folders])
        filtered_result = []
        warning_occurred = False
        for item in search_results:
            is_in_study_folder = item.startswith(search_subfolder + os.path.sep) and ".." + os.path.sep not in item
            if is_in_study_folder:
                relative_path = item.replace(search_subfolder + os.path.sep, f"{search_pattern_prefix}")
                sub_path = relative_path.split(os.path.sep)
                if sub_path and sub_path[0] not in excluded_folder_set:
                    filtered_result.append(relative_path)
            else:
                if not warning_occurred:
                    message = f"{search_pattern} pattern results for {study_id} are not allowed: {item}"
                    logger.warning(message)
                    warning_occurred = True

        files = [file for file in filtered_result if file not in ignore_list]
        files.sort()

        result = [{"name": file.replace(search_path + "/", "")} for file in files]

        return jsonify({'files': result})

    def get_ignore_list(self, search_path):
        ignore_list = []

        metadata_files = glob.glob(os.path.join(search_path, "[isam]_*.t[xs]?"))
        internal_file_names = get_settings().file_filters.internal_mapping_list
        internal_files = [os.path.join(search_path, file + ".json") for file in internal_file_names]
        internal_files.append(os.path.join(search_path, "missing_files.txt"))
        ignore_list.extend(metadata_files)
        ignore_list.extend(internal_files)
        return ignore_list

    @swagger.operation(
        summary="Move raw and drived data files to RAW_FILES, DERIVED_FILES or RECYCLE_BIN folder",
        nickname="Move files",
        notes='''Move files to RAW_FILES, DERIVED_FILES, or RECYCLE_BIN folder. <pre><code>
{    
    "files": [
        {"name": "FILES/Sample/data.d"},
        {"name": "FILES/Raw-File-001.raw"}
    ]
}</pre></code></br> 
''',
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
                "description": 'Files to move other folder',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "target_location",
                "description": "Target folder",
                "required": False,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["RAW_FILES", "DERIVED_FILES", "RECYCLE_BIN"]
            },
            {
                "name": "override",
                "description": "If file exists in target location, file is overridden",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": "false",
                "default": False
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
    @metabolights_exception_handler
    def put(self, study_id):

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
        parser.add_argument('target_location', help='Target Location')
        parser.add_argument('override', help='Override target file if it exists')

        target_location = None
        files = None
        override = False
        # If false, only sync ISA-Tab metadata files
        if request.args:
            args = parser.parse_args(req=request)
            files = args['files'] if args['files'] else None
            target_location = args['target_location'] if args['target_location'] else None
            override = True if args['override'] and args['override'].lower() == "true" else False

        if not target_location or target_location not in ("RAW_FILES", "DERIVED_FILES", "RECYCLE_BIN"):
            raise MetabolightsException(http_code=400, message=f"Target location is not valid")
        # body content validation
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['files']
            if data is None:
                raise MetabolightsException(http_code=412, message=f"Files are not defined")
            files = data
        except (ValidationError, Exception):
            raise MetabolightsException(http_code=412, message=f"Incorrect JSON")

        UserService.get_instance().validate_user_has_curator_role(user_token)
        StudyService.get_instance().get_study_by_acc(study_id)
        files_input = [{"name": str(x["name"])} for x in files if "name" in x and x["name"]]
        try:
            inputs = {"study_id": study_id, "files": files_input, "target_location": target_location, "override": override }
            result = data_file_operations.move_data_files.apply_async(kwargs=inputs, expires=60*5)

            result = {'content': f"Task has been started. Task id: {result.id}", 'message': None, "err": None}
            return result
        except Exception as ex:
            raise MetabolightsException(http_code=500, message=f"Move files task submission was failed", exception=ex)


class StudyRawAndDerivedDataFolder(Resource):
    @swagger.operation(
        summary="Search raw and derived data folders in the study.",
        notes="Search raw and derived data folders in the study. All files and internal folders are excluded.",
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
                "name": "search_pattern",
                "description": "search pattern (*HILIC*, POS*, etc.). Default is *",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "default": '*'
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
        parser.add_argument('search_pattern', help='Search pattern')

        search_pattern = '*'

        if request.args:
            args = parser.parse_args(req=request)
            search_pattern = args['search_pattern'] if args['search_pattern'] else '*'
            if '..' + os.path.sep in search_pattern or '.' + os.path.sep in search_pattern:
                abort(401, error="Relative folder search patterns (., ..) are not allowed")

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not is_curator:
            abort(403, error="User has no curator role")
        study_settings = get_study_settings()

        study_folder = os.path.join(study_settings.mounted_paths.study_metadata_files_root_path, study_id)
        search_path = os.path.join(study_settings.mounted_paths.study_readonly_files_root_path, study_id)


        glob_search_result = glob.glob(os.path.join(search_path, search_pattern))
        search_results = [os.path.abspath(file) for file in glob_search_result if os.path.isdir(file)]
        excluded_folders = get_settings().file_filters.folder_exclusion_list

        excluded_folder_set = set([self.get_validated_basename(study_folder, file) for file in excluded_folders])
        excluded_folder_set.add("RAW_FILES")
        excluded_folder_set.add("DERIVED_FILES")

        filtered_result = []
        warning_occurred = False
        for item in search_results:
            is_in_study_folder = item.startswith(search_path + os.path.sep) and ".." + os.path.sep not in item
            if is_in_study_folder:
                relative_path = item.replace(search_path + os.path.sep, '')
                sub_path = relative_path.split(os.path.sep)
                if sub_path and sub_path[0] not in excluded_folder_set:
                    filtered_result.append(item)
            else:
                if not warning_occurred:
                    message = f"{search_pattern} pattern results for {study_id} are not allowed: {item}"
                    logger.warning(message)
                    warning_occurred = True

        files = filtered_result
        files.sort()

        result = [{"name": file.replace(search_path + "/", "")} for file in files]

        return jsonify({'folders': result})


    def get_validated_basename(self, study_folder, file):

        return os.path.basename(os.path.abspath(os.path.join(study_folder, file)))

    @swagger.operation(
        summary="Move raw and drived data folders into RAW_FILES, DERIVED_FILES or RECYCLE_BIN folder",
        nickname="Move folders",
        notes='''Move folders to RAW_FILES, DERIVED_FILES, or RECYCLE_BIN folder<pre><code>
{    
    "folders": [
        {"name": "POS"},
        {"name": "Method_2"}
    ]
}</pre></code></br> 
''',
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
                "name": "folders",
                "description": 'Folders to move other folder',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "target_location",
                "description": "Target folder",
                "required": False,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["RAW_FILES", "DERIVED_FILES", "RECYCLE_BIN"]
            },
            {
                "name": "override",
                "description": "If file exists in target location, file is overridden",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": "false",
                "default": False
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
    def put(self, study_id):

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
        parser.add_argument('folders', help='folders')
        parser.add_argument('target_location', help='Target Location')
        parser.add_argument('override', help='Override target file if it exists')

        target_location = None
        folders = None
        override = False
        # If false, only sync ISA-Tab metadata files
        if request.args:
            args = parser.parse_args(req=request)
            folders = args['folders'] if args['folders'] else None
            target_location = args['target_location'] if args['target_location'] else None
            override = True if args['override'] and args['override'].lower() == "true" else False

        if not target_location or target_location not in ("RAW_FILES", "DERIVED_FILES", "RECYCLE_BIN"):
            abort(400, error='target location is invalid or not defined')
        # body content validation
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['folders']
            if data is None:
                abort(412, error='Folders are defined')
            folders = data
        except (ValidationError, Exception):
            abort(400, error='Incorrect JSON provided')

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)
        studies_folder = get_settings().study.mounted_paths.study_metadata_files_root_path
        study_path = os.path.abspath(os.path.join(studies_folder, study_id))
        excluded_folders = get_settings().file_filters.folder_exclusion_list

        excluded_folder_set = set([os.path.abspath(os.path.join(study_path, file)) for file in excluded_folders])

        raw_data_dir = os.path.abspath(os.path.join(study_path, "RAW_FILES"))
        derived_data_dir = os.path.abspath(os.path.join(study_path, "DERIVED_FILES"))
        excluded_folder_set.add(raw_data_dir)
        excluded_folder_set.add(derived_data_dir)

        recycle_bin_dir = os.path.abspath(os.path.join(studies_folder, "DELETED_FILES", study_id))
        if target_location == 'RAW_FILES':
            os.makedirs(raw_data_dir, exist_ok=True)
        elif target_location == 'DERIVED_FILES':
            os.makedirs(derived_data_dir, exist_ok=True)
        else:
            os.makedirs(recycle_bin_dir, exist_ok=True)

        warnings = []
        successes = []
        errors = []
        for folder in folders:

            if "name" in folder and folder["name"]:
                f_name = folder["name"]
                try:
                    file_path = os.path.abspath(os.path.join(study_path, f_name))
                    if not os.path.exists(file_path):
                        warnings.append({'folder': f_name, 'message': 'Operation is ignored. Folder does not exist.'})
                        continue

                    if file_path in excluded_folder_set:
                        warnings.append({'folder': f_name, 'message': 'Operation is ignored. Folder is in exclude list.'})
                        continue

                    base_name = os.path.basename(f_name)
                    if target_location == 'RAW_FILES':
                        target_path = os.path.abspath(os.path.join(raw_data_dir, base_name))
                    elif target_location == 'DERIVED_FILES':
                        target_path = os.path.abspath(os.path.join(derived_data_dir, base_name))
                    else:
                        target_path = os.path.abspath(os.path.join(recycle_bin_dir, f_name))
                        split = os.path.split(target_path)
                        if not os.path.exists(split[0]):
                            os.makedirs(split[0])
                    if f_name == target_path:
                        warnings.append({'folder': f_name, 'message': 'Operation is ignored. Target is same folder.'})
                        continue
                    if os.path.exists(target_path):
                        if not override:
                            warnings.append({'folder': f_name, 'message': 'Operation is ignored. Target folder exists.'})
                            continue
                        else:
                            date_format = "%Y%m%d%H%M%S"
                            shutil.move(target_path, target_path + "-" + time.strftime(date_format))

                    shutil.move(file_path, target_path)
                    successes.append({'folder': f_name, 'message': f'Folder is moved to {target_location}'})

                except Exception as e:
                    errors.append({'folder': f_name, 'message': str(e)})
        return jsonify({'successes': successes, 'warnings': warnings, 'errors': errors})



class StudyFilesReuse(Resource):
    @swagger.operation(
        summary="Get a list, with timestamps, of all files in the study folder from file-list result already created",
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
            },
            {
                "name": "obfuscation_code",
                "description": "obfuscation code of study",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False
            },
            {
                "name": "readonlyMode",
                "description": "readonlyMode",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True
            },            
            {
                "name": "force",
                "description": "Force writing Files list json to file",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": False,
                "default": True
            },
            {
                "name": "include_internal_files",
                "description": "Include internal mapping files",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": False,
                "default": False
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
    def get(self, study_id: str):

        # param validation
        if study_id is None:
            abort(404)

        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        readonly_mode = True
        obfuscation_code = None
        if "obfuscation_code" in request.headers:
            obfuscation_code = request.headers["obfuscation_code"]
            
        parser = reqparse.RequestParser()
        parser.add_argument('force', help='Force writing files list schema json')
        parser.add_argument('readonlyMode', help='Readonly Mode')
        parser.add_argument('include_internal_files', help='Ignores internal files')
        force_write = False
        include_internal_files = False
        if request.args:
            args = parser.parse_args(req=request)
            force_write = True if args['force'].lower() == 'true' else False
            readonly_mode = True if not args['readonlyMode'] or args['readonlyMode'].lower() == 'true' else False
            if args['include_internal_files']:
                include_internal_files = False if args['include_internal_files'].lower() != 'true' else True
        settings = get_study_settings()
        study_id, obfuscation_code = identify_study_id(study_id, obfuscation_code)

            
        UserService.get_instance().validate_user_has_read_access(user_token=user_token, obfuscationcode=obfuscation_code, study_id=study_id)
        study = StudyService.get_instance().get_study_by_acc(study_id)
        upload_folder = study_id.lower() + "-" + study.obfuscationcode

        study_metadata_location = os.path.join(settings.mounted_paths.study_metadata_files_root_path, study_id)
        # files_list_json = settings.files_list_json_file_name
        # files_list_json_file = os.path.join(study_metadata_location, settings.internal_files_symbolic_link_name, files_list_json)
        # indexed_files = None
        # if os.path.exists(files_list_json_file):
        #     try:
        #         with open(files_list_json_file, "r") as fp:
        #             cached_data = json.load(fp)
        #             indexed_files = StudyFolderIndex.parse_obj(cached_data)
        #     except Exception as exc:
        #         logger.warning(f"Error reading {files_list_json} for study {study_id}")
        
        referenced_files = get_referenced_file_set(study_id=study_id, metadata_path=study_metadata_location)
        exclude_list = set(get_settings().file_filters.internal_mapping_list)
        # if force_write:
        directory_files = get_directory_files(study_metadata_location, None, search_pattern="**/*", recursive=False, exclude_list=exclude_list)
        # metadata_files = get_all_metadata_files()
        # internal_files_path = os.path.join(study_metadata_location, settings.internal_files_symbolic_link_name)
        # internal_files = glob.glob(os.path.join(internal_files_path, "*.json"))
        search_result = evaluate_files(directory_files, referenced_files)
        if not readonly_mode:
            try:
                settings = get_settings()
                ftp_root_path = settings.hpc_cluster.datamover.mounted_paths.cluster_private_ftp_root_path
                ftp_folder_path = os.path.join(ftp_root_path, f"{study.acc.lower()}-{study.obfuscationcode}")
                inputs = {"path": ftp_folder_path}
                task = list_directory.apply_async(kwargs=inputs, expires=60*5)
                output = task.get(timeout=settings.hpc_cluster.configuration.task_get_timeout_in_seconds * 2)
                ftp_files = LiteFileSearchResult.parse_obj(output).study
                
                search_result.latest = ftp_files  
                sortFileMetadataList(ftp_files)
            except Exception as exc:
                logger.error(f"Error for study {study.acc}: {str(exc)}")
                ftp_files = []

        ftp_private_relative_root_path = get_private_ftp_relative_root_path()
        upload_path = os.path.join(ftp_private_relative_root_path, upload_folder)
        search_result.uploadPath = upload_path
        search_result.obfuscationCode = study.obfuscationcode
        sortFileMetadataList(search_result.study)
        
        return search_result.dict()

        
        # return update_files_list_schema(study_id, obfuscation_code, study_metadata_location,
        #                                 files_list_json_file, include_internal_files=include_internal_files)
        # if os.path.isfile(files_list_json_file):
        #     logger.info("Files list json found for studyId - %s!", study_id)
        #     try:
        #         with open(files_list_json_file, 'r', encoding='utf-8') as f:
        #             files_list_schema = json.load(f)
        #             logger.info("Listing files list from files-list json file!")
        #     except Exception as e:
        #         logger.error('Error while reading file list schema file: ' + str(e))
        #         files_list_schema = update_files_list_schema(study_id, obfuscation_code, study_metadata_location,
        #                                                      files_list_json_file,
        #                                                      include_internal_files=include_internal_files)
        # else:
        #     logger.info(" Files list json not found! for studyId - %s!", study_id)
        #     files_list_schema = update_files_list_schema(study_id, obfuscation_code, study_metadata_location,
        #                                                  files_list_json_file,
        #                                                  include_internal_files=include_internal_files)
        # 
        # return files_list_schema


def update_files_list_schema(study_id, obfuscation_code, study_location, files_list_json_file,
                             include_internal_files: bool = False, include_sub_dir=None):
    study_files, upload_files, upload_diff, upload_location, latest_update_time = \
        get_all_files_from_filesystem(study_id, obfuscation_code, study_location,
                                      directory=None, include_raw_data=True,
                                      assay_file_list=get_assay_file_list(study_location),
                                      static_validation_file=False, include_sub_dir=include_sub_dir)
    if not include_internal_files:
        study_files = [item for item in study_files if 'type' in item and item['type'] != 'internal_mapping']

    relative_studies_root_path = get_private_ftp_relative_root_path()
    folder_name = f'{study_id.lower()}-{obfuscation_code}'
    upload_path = os.path.join(os.sep, relative_studies_root_path.lstrip(os.sep), folder_name)
    files_list_schema = {'study': study_files,
                                 'latest': upload_diff,
                                 'private': upload_files,
                                 'uploadPath': upload_path,
                                 'obfuscationCode': obfuscation_code}

    logger.info(" Writing Files list schema to a file for studyid - %s ", study_id)
    try:
        with open(files_list_json_file, 'w', encoding='utf-8') as f:
            json.dump(files_list_schema, f, ensure_ascii=False)
    except Exception as e:
        logger.error('Error writing Files schema file: ' + str(e))

    return jsonify(files_list_schema)


class CopyFilesFolders(Resource):
    @swagger.operation(
        summary="[Deprecated] Copy files from upload folder to study folder",
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

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_write_access(user_token, study_id)

        study = StudyService.get_instance().get_study_by_acc(study_id)
        study_path = os.path.join(get_settings().study.mounted_paths.study_metadata_files_root_path, study_id)
        storage = StorageService.get_ftp_private_storage()

        result = storage.check_folder_sync_status(study_id, study.obfuscationcode, study_path)
        return jsonify(result.dict())

    #     log_request(request)
    #     # param validation
    #     if study_id is None:
    #         abort(404, 'Please provide valid parameter for study identifier')
    #     study_id = study_id.upper()
    #
    #     # User authentication
    #     user_token = None
    #     if "user_token" in request.headers:
    #         user_token = request.headers["user_token"]
    #
    #     # query validation
    #     parser = reqparse.RequestParser()
    #     parser.add_argument('include_raw_data', help='Include raw data')
    #     parser.add_argument('file_location', help='Alternative file location')
    #     include_raw_data = False
    #     file_location = None
    #
    #     # If false, only sync ISA-Tab metadata files
    #     if request.args:
    #         args = parser.parse_args(req=request)
    #         include_raw_data = False if args['include_raw_data'].lower() != 'true' else True
    #         file_location = args['file_location']
    #
    #     # body content validation
    #     files = {}
    #     single_files_only = False
    #     status = False
    #     if request.data:
    #         try:
    #             data_dict = json.loads(request.data.decode('utf-8'))
    #             files = data_dict['files'] if 'files' in data_dict else {}
    #             if files:
    #                 single_files_only = True
    #         except KeyError:
    #             logger.info("No 'files' parameter was provided.")
    #
    #     # check for access rights
    #     is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
    #     study_status = wsc.get_permissions(study_id, user_token)
    #     if not write_access:
    #         abort(403)
    #
    #     status = wsc.create_upload_folder(study_id, obfuscation_code, user_token)
    #     upload_location = status["os_upload_path"]
    #     if file_location:
    #         upload_location = file_location
    #
    #     logger.info("For %s we use %s as the upload path. The study path is %s", study_id, upload_location,
    #                 study_location)
    #     ftp_private_storage = StorageService.get_ftp_private_storage()
    #     audit_status, dest_path = write_audit_files(study_location)
    #     if single_files_only:
    #         for file in files:
    #             try:
    #                 from_file = file["from"]
    #                 to_file = file["to"]
    #                 if not from_file or not to_file:
    #                     abort(417, "Please provide both 'from' and 'to' file parameters")
    #
    #                 if not file_location:
    #                     ftp_source_file = os.path.join(upload_location, from_file)
    #                     destination_file = os.path.join(study_location, to_file)
    #                     # download directly to study folder
    #                     ftp_private_storage.sync_from_storage(ftp_source_file, destination_file, logger=logger)
    #                     continue
    #
    #                 # continue if manual upload folder defined
    #                 source_file = os.path.join(upload_location, to_file)
    #                 destination_file = os.path.join(study_location, to_file)
    #
    #                 logger.info("Copying specific file %s to %s", from_file, to_file)
    #
    #
    #                 if from_file != to_file:
    #                     if os.path.isfile(source_file):
    #                         logger.info(
    #                             "The filename/folder you are copying to (%s) already exists in the upload folder, deleting first",
    #                             to_file)
    #                         os.remove(source_file)
    #                     else:
    #                         logger.info("Renaming file %s to %s", from_file, to_file)
    #                         os.rename(os.path.join(upload_location, from_file), source_file)
    #
    #                 if os.path.isdir(source_file):
    #                     logger.info(source_file + ' is a directory')
    #                     try:
    #                         if os.path.exists(destination_file) and os.path.isdir(destination_file):
    #                             logger.info('Removing directory ' + destination_file)
    #                             shutil.rmtree(destination_file)  # Remove the destination file/folder first
    #
    #                         logger.info("Copying folder '%s' to study folder '%s'", source_file, destination_file)
    #                         shutil.copytree(source_file, destination_file)
    #                         status = True
    #                     except OSError as e:
    #                         logger.error('Folder already exists? Can not copy %s to %s',
    #                                      source_file, destination_file, str(e))
    #                 else:
    #                     logger.info("Copying file %s to study folder %s", to_file, study_location)
    #                     shutil.copy2(source_file, destination_file)
    #                     status = True
    #             except Exception as e:
    #                 logger.error('File copy failed with error ' + str(e))
    #
    #     else:
    #         logger.info("Copying all newer files from '%s' to '%s'", upload_location, study_location)
    #         include_inv = False
    #         if is_curator:
    #             include_inv = True
    #         if file_location:
    #             status, message = copy_files_and_folders(upload_location, study_location,
    #                                                  include_raw_data=include_raw_data,
    #                                                  include_investigation_file=include_inv)
    #         else:
    #             status, message = ftp_private_storage.sync_from_storage(upload_location, study_location, logger=logger)
    #             ftp_private_storage.sync_from_storage(upload_location, study_location, logger=logger)
    #     message = ''
    #     if status:
    #         reindex_status, message = wsc.reindex_study(study_id, user_token)
    #         return {'Success': 'Copied files from ' + upload_location}
    #     else:
    #         return {'Warning': message}


class SyncFolder(Resource):
    @swagger.operation(
        summary="[Deprecated] Copy files from study folder to private FTP  folder",
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
        directory_name = ''
        # If false, only sync ISA-Tab metadata files
        if request.args:
            args = parser.parse_args(req=request)
            directory_name = args['directory_name']

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        if directory_name:
            if directory_name == os.sep:
                destination = study_id.lower() + '-' + obfuscation_code
                source = study_location
            else:
                destination = os.path.join(study_id.lower() + '-' + obfuscation_code, directory_name)
                source = os.path.join(study_location, directory_name)
        else:
            destination = study_id.lower() + '-' + obfuscation_code
            source = study_location

        ftp_private_storage = StorageService.get_ftp_private_storage()
        logger.info("syncing files from " + source + " to " + destination)
        try:

            # ftp_private_storage.remote.create_folder(destination, acl=Acl.AUTHORIZED_READ_WRITE, exist_ok=True)

            ftp_private_storage.sync_from_local(source, destination, logger=logger, purge=False)

            logger.info('Copying file %s to %s', source, destination)
            return {'Success': 'Copying files from study folder to ftp folder is started'}
        except FileExistsError as e:
            logger.error(f'Folder already exists! Can not copy {source} to {destination} {str(e)}')
        except OSError as e:
            logger.error(f'Does the folder already exists? Can not copy {source} to {destination} {str(e)}')

        except Exception as e:
            logger.error(f'Other error! Can not copy {source} to {destination} {str(e)}')


class SampleStudyFiles(Resource):
    @swagger.operation(
        summary="Get a list of all sample names, mapped to files in the study folder",
        notes="A perfect match gives reliability score of '1.0'. Use the highest score possible for matching",
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

        upload_location = study_id.lower() + "-" + obfuscation_code

        # Get all unique file names
        all_files_in_study_location = get_all_files(study_location, include_raw_data=True, include_sub_dir=True,
                                                    assay_file_list=get_assay_file_list(study_location))
        filtered_files_in_study_location = get_files(all_files_in_study_location[0])
        all_files = get_files(filtered_files_in_study_location)

        isa_study = None
        try:
            isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                             skip_load_tables=False,
                                                             study_location=study_location)
        except:
            abort(500, error="Could not load the study metadata files")

        samples_and_files = []
        for sample in isa_study.samples:
            s_name = sample.name
            # For each sample, loop over all the files
            # Todo, some studies has lots more files than samples, consider if this should be reversed
            for file_item in all_files:
                f_name = file_item["file"]
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
    @metabolights_exception_handler
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
                raise MetabolightsException(http_code=400, message=f"files parameter is invalid")
            files = data
        except (ValidationError, Exception):
            raise MetabolightsException(http_code=400, message=f"files parameter is invalid")
        UserService.get_instance().validate_user_has_curator_role(user_token)
        StudyService.get_instance().get_study_by_acc(study_id)
        mounted_paths = get_settings().hpc_cluster.datamover.mounted_paths
        files_input = [{"name": str(x["name"])} for x in files if "name" in x and x["name"]]
        study_metadata_path = os.path.join(mounted_paths.cluster_study_metadata_files_root_path, study_id)
        try:
            inputs = {"study_metadata_path": study_metadata_path, "files": files_input, "remove_zip_files": True, "override": True }
            result = data_file_operations.unzip_folders.apply_async(kwargs=inputs, expires=60*5)

            result = {'content': f"Task has been started. Result will be sent by email. Task id: {result.id}", 'message': None, "err": None}
            return result
        except Exception as ex:
            raise MetabolightsException(http_code=500, message=f"Unzip task submission was failed", exception=ex)
        
        
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
                all_files.append(files)
    return all_files


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
                "name": "include_internal_files",
                "description": "Include internal mapping files",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": False,
                "default": False
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
                "name": "location",
                "description": "location is upload or study",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["study", "upload"],
                "defaultValue": "study",
                "default": "study"
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
    @metabolights_exception_handler
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
        parser.add_argument('location', help='File location')
        parser.add_argument('include_internal_files', help='Ignores internal files')
        include_sub_dir = False
        directory = None
        location = "study"
        include_internal_files = False
        if request.args:
            args = parser.parse_args(req=request)
            if args['include_sub_dir']:
                include_sub_dir = False if args['include_sub_dir'].lower() != 'true' else True
            location = args['location'] if  args['location'] else "study"
            directory = args['directory'] if args['directory'] else None
            if args['include_internal_files']:
                include_internal_files = False if args['include_internal_files'].lower() != 'true' else True
        if location not in ["study", "upload"]:
            abort(401, message="Study location is not valid")
        if directory and directory.startswith(os.sep):
            abort(401, message="You can only specify folders in the current study folder")
        settings = get_settings()
        study_id, obfuscation_code = identify_study_id(study_id)
        # check for access rights
        UserService.get_instance().validate_user_has_read_access(user_token, study_id=study_id, obfuscationcode=obfuscation_code)
        
        study = StudyService.get_instance().get_study_by_acc(study_id)
        upload_folder = study_id.lower() + "-" + study.obfuscationcode

        ftp_private_relative_root_path = get_private_ftp_relative_root_path()
        upload_path = os.path.join(ftp_private_relative_root_path, upload_folder)

        settings = get_settings().study
        if location == "study":
            study_metadata_location = os.path.join(settings.mounted_paths.study_metadata_files_root_path, study_id)
                
            # files_list_json_file = os.path.join(study_metadata_location, settings.readonly_files_symbolic_link_name, files_list_json)
            referenced_files = get_referenced_file_set(study_id=study_id, metadata_path=study_metadata_location)
            exclude_list = set(get_settings().file_filters.internal_mapping_list)
            # if force_write:
            directory_files = get_directory_files(study_metadata_location, directory, search_pattern="**/*", recursive=False, exclude_list=exclude_list, include_metadata_files=True)
            # metadata_files = get_all_metadata_files()
            # internal_files_path = os.path.join(study_metadata_location, settings.internal_files_symbolic_link_name)
            # internal_files = glob.glob(os.path.join(internal_files_path, "*.json"))
            search_result = evaluate_files(directory_files, referenced_files)
            sortFileMetadataList(search_result.study)
            
        else:
            try:
                settings = get_settings()
                ftp_root_path = settings.hpc_cluster.datamover.mounted_paths.cluster_private_ftp_root_path
                ftp_folder_path = os.path.join(ftp_root_path, f"{study.acc.lower()}-{study.obfuscationcode}", directory)
                inputs = {"path": ftp_folder_path}
                task = list_directory.apply_async(kwargs=inputs, expires=60*5)
                output = task.get(timeout=settings.hpc_cluster.configuration.task_get_timeout_in_seconds * 2)
                search_result = LiteFileSearchResult.parse_obj(output)
                # search_result.latest = search_result.study
                # search_result.study = []
                sortFileMetadataList(search_result.study)
            except Exception as exc:
                logger.error(f"Error for study {study.acc}: {str(exc)}")          
                return LiteFileSearchResult().dict()
        
        search_result.uploadPath = upload_path
        search_result.obfuscationCode = study.obfuscationcode
        return search_result.dict()
        
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
        directory_name = ""
        # If false, only sync ISA-Tab metadata files
        if request.args:
            args = parser.parse_args(req=request)
            directory_name = args['directory_name']

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)
        source = study_location
        if directory_name:
            source = os.path.join(study_location, directory_name)
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


class DeleteAsperaFiles(Resource):
    @swagger.operation(
        summary="Delete aspera incomplete transfer files such as *.aspx , *.aspera-ckpt, *.partial from study directory.",
        parameters=[
            {
                "name": "study_id",
                "description": "Study ID",
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
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token"
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    @metabolights_exception_handler
    def delete(self, study_id):

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if not user_token:
            abort(404)

        if user_token is None or study_id is None:
            abort(401)

        study_id = study_id.upper()
        UserService.get_instance().validate_user_has_curator_role(user_token)
        StudyService.get_instance().get_study_by_acc(study_id)
        
        logger.info('Deleting aspera files from study ' + study_id)
                
        inputs = {"study_id": study_id}
        result = data_file_operations.delete_aspera_files_from_data_files.apply_async(kwargs=inputs, expires=60*5)
        return  {
            "content": f"Task has been started. Task id: {result.id}",
            "message": None,
            "err": None,
        }
