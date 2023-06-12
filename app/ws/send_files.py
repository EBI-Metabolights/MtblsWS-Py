#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Aug-02
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

import logging
import os
import random
from zipfile import ZipFile

from flask import request, send_file, safe_join, abort, make_response
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from flask import current_app as app, jsonify
from app.utils import metabolights_exception_handler

from app.ws.db_connection import get_obfuscation_code
from app.ws.mtblsWSclient import WsClient
from app.ws.settings.utils import get_study_settings
from app.ws.study.study_service import identify_study_id
from app.ws.study.user_service import UserService
from app.ws.study_files import get_basic_files

logger = logging.getLogger('wslog')
# MetaboLights (Java-Based) WebService client
wsc = WsClient()


class SendFiles(Resource):
    @swagger.operation(
        summary="Stream file(s) to the browser",
        notes="Download/Stream files from the public study folder</p>"
              "To download all the ISA-Tab metadata in one zip file, use the word <b>'metadata'</b> in the file_name.",
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
                "name": "file",
                "description": "File(s) or folder name (comma separated, relative to study folder). Keyword 'metadata' can also be used.",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": False,
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
                "message": "Unauthorized. Access to the resource requires user authentication. "
                           "Please provide a study id and a valid user token"
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
    def get(self, study_id):
        # param validation
        if study_id is None:
            logger.info('No study_id given')
            abort(404)
        study_id = study_id.upper()

        if study_id == "MTBLS1405":
            abort(429)

        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            user_token = "public_access_only"

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('file', help='The file or sub-directory to download')
        file_name = None
        metadata_only = False

        if request.args:
            args = parser.parse_args(req=request)
            file_name = args['file'] if args['file'] else None

        if file_name is None:
            logger.info('No file name given')
            abort(404)

        # check for access rights
        is_curator, read_access, write_access, db_obfuscation_code, study_location_deprecated, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)

        if not read_access:
            abort(403)
        settings = get_study_settings()
        study_metadata_location = os.path.join(settings.study_metadata_files_root_path, study_id)
        files = ""
        if file_name == 'metadata':
            file_list = get_basic_files(study_metadata_location, include_sub_dir=False, assay_file_list=None)
            for _file in file_list:
                f_type = _file['type']
                f_name = _file['file']
                if "metadata" in f_type:
                    files = files + f_name + ','
            file_name = files.rstrip(",")

        remove_file = False
        safe_path = safe_join(study_metadata_location, file_name)
        zip_name = None
        try:
            download_folder_path = os.path.join(study_metadata_location, settings.internal_files_symbolic_link_name, "temp")
            os.makedirs(download_folder_path, exist_ok=True)
            short_zip = study_id + "_" + str(random.randint(100000, 200000)) + "_compressed_files.zip"
            zip_name = os.path.join(download_folder_path, short_zip)
            if os.path.isfile(zip_name):
                os.remove(zip_name)
            if ',' in file_name and not os.path.exists(safe_path):
                zipfile = ZipFile(zip_name, mode='a')
                remove_file = True
                files = file_name.split(',')
                for file in files:
                    safe_path = safe_join(study_metadata_location, file)
                    if os.path.isdir(safe_path):
                        for sub_file in recursively_get_files(safe_path):
                            f_name = sub_file.path.replace(study_metadata_location, '')
                            zipfile.write(sub_file.path, arcname=f_name)
                    else:
                        zipfile.write(safe_path, arcname=file)
                zipfile.close()
                remove_file = True
                safe_path = zip_name
                file_name = short_zip
            else:
                if os.path.isdir(safe_path):
                    zipfile = ZipFile(zip_name, mode='a')
                    for sub_file in recursively_get_files(safe_path):
                        zipfile.write(sub_file.path.replace(study_metadata_location, ''),
                                      arcname=sub_file.name)
                    zipfile.close()
                    remove_file = True
                    safe_path = zip_name
                    file_name = short_zip
                else:
                    head, tail = os.path.split(file_name)
                    file_name = tail

            resp = make_response(send_file(safe_path, as_attachment=True, attachment_filename=file_name, cache_timeout=0))
            # response.headers["Content-Disposition"] = "attachment; filename={}".format(file_name)
            resp.headers['Content-Type'] = 'application/octet-stream'
            return resp
        except FileNotFoundError as e:
            abort(404, "Could not find file " + file_name)
        except Exception as e:
            abort(404, "Could not create zip file " + str(e))
        finally:
            if remove_file and os.path.exists(zip_name):
                os.remove(zip_name)
                logger.info('Removed zip file ' + zip_name)


class SendFilesPrivate(Resource):
    @swagger.operation(
        summary="Stream file(s) to the browser",
        notes="Download/Stream files from the study folder</p>"
              "To download all the ISA-Tab metadata in one zip file, use the word <b>'metadata'</b> in the file_name."
              "</p>The 'obfuscation_code' path parameter is mandatory, but for any <b>PUBLIC</b> studies you can use the "
              "keyword <b>'public'</b> instead of the real obfuscation code",
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
                "name": "file",
                "description": "File(s) or folder name (comma separated, relative to study folder). Keyword 'metadata' can also be used.",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "obfuscation_code",
                "description": "Study obfuscation code",
                "required": True,
                "allowMultiple": False,
                "paramType": "header",
                "dataType": "string",
                "default": "public"
            },
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False
            }
        ],
        defaults=[{"obfuscation_code": "public"}],
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication. "
                           "Please provide a study id and a valid user token"
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
    def get(self, study_id, obfuscation_code):
        # param validation
        if study_id is None:
            logger.info('No study_id given')
            abort(404)
        study_id = study_id.upper()

        if study_id == "MTBLS1405":
            abort(429)

        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            user_token = "public_access_only"

        if 'obfuscation_code' in request.headers:
            obfuscation_code = request.headers["obfuscation_code"]

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('file', help='The file or sub-directory to download')
        file_name = None
        metadata_only = False

        if request.args:
            args = parser.parse_args(req=request)
            file_name = args['file'] if args['file'] else None

        if file_name is None:
            logger.info('No file name given')
            abort(404)
        settings = get_study_settings()
        study_metadata_location = os.path.join(settings.study_metadata_files_root_path, study_id)
        study_id, obfuscation_code = identify_study_id(study_id, obfuscation_code)
        # check for access rights
        # is_curator, read_access, write_access, db_obfuscation_code, study_location, release_date, submission_date, \
        #     study_status = wsc.get_permissions(study_id, user_token, obfuscation_code)
        if not obfuscation_code:
            abort(403)
        UserService.get_instance().validate_user_has_read_access(user_token, study_id, obfuscation_code)
        files = ""
        if file_name == 'metadata':
            file_list = get_basic_files(study_metadata_location, include_sub_dir=False, assay_file_list=None)
            for _file in file_list:
                f_type = _file['type']
                f_name = _file['file']
                if "metadata" in f_type:
                    files = files + f_name + ','
            file_name = files.rstrip(",")

        remove_file = False
        safe_path = safe_join(study_metadata_location, file_name)
        zip_name = None
        try:
            download_folder_path = os.path.join(study_metadata_location, settings.internal_files_symbolic_link_name, "temp")
            os.makedirs(download_folder_path, exist_ok=True)
            short_zip = study_id + "_" + str(random.randint(100000, 200000)) + "_compressed_files.zip"
            zip_name = os.path.join(download_folder_path, short_zip)
            if os.path.isfile(zip_name):
                os.remove(zip_name)
            if ',' in file_name and not os.path.exists(safe_path):
                zipfile = ZipFile(zip_name, mode='a')
                remove_file = True
                files = file_name.split(',')
                for file in files:
                    safe_path = safe_join(study_metadata_location, file)
                    if os.path.isdir(safe_path):
                        for sub_file in recursively_get_files(safe_path):
                            f_name = sub_file.path.replace(study_metadata_location, '')
                            zipfile.write(sub_file.path, arcname=f_name)
                    else:
                        zipfile.write(safe_path, arcname=file)
                zipfile.close()
                remove_file = True
                safe_path = zip_name
                file_name = short_zip
            else:
                if os.path.isdir(safe_path):
                    zipfile = ZipFile(zip_name, mode='a')
                    for sub_file in recursively_get_files(safe_path):
                        zipfile.write(sub_file.path.replace(study_metadata_location, ''),
                                      arcname=sub_file.name)
                    zipfile.close()
                    remove_file = True
                    safe_path = zip_name
                    file_name = short_zip
                else:
                    head, tail = os.path.split(file_name)
                    file_name = tail

            resp = make_response(send_file(safe_path, as_attachment=True, attachment_filename=file_name, cache_timeout=0))
            # response.headers["Content-Disposition"] = "attachment; filename={}".format(file_name)
            resp.headers['Content-Type'] = 'application/octet-stream'
            return resp
        except FileNotFoundError as e:
            abort(404, "Could not find file " + file_name)
        except Exception as e:
            abort(404, "Could not create zip file " + str(e))
        finally:
            if remove_file:
                os.remove(safe_path)
                logger.info('Removed zip file ' + safe_path)

def recursively_get_files(base_dir):
    for entry in os.scandir(base_dir):
        if entry.is_file():
            yield entry
        elif entry.is_dir(follow_symlinks=False):
            yield from recursively_get_files(entry.path)
