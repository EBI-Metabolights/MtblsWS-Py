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

from flask import make_response, request, send_file
from flask_restful import Resource, abort
from flask_restful_swagger import swagger

from app.utils import metabolights_exception_handler
from app.ws.auth.permissions import validate_submission_view
from app.ws.mtblsWSclient import WsClient
from app.ws.settings.utils import get_study_settings
from app.ws.study.folder_utils import get_basic_files
from app.ws.study.utils import get_study_metadata_path

logger = logging.getLogger("wslog")
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
                "dataType": "string",
            },
            {
                "name": "file",
                "description": "File(s) or folder name (comma separated, relative to study folder). Keyword 'metadata' can also be used.",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication. "
                "Please provide a study id and a valid user token",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def get(self, study_id):
        result = validate_submission_view(request)
        study_id = result.context.study_id
        study_metadata_location = get_study_metadata_path(study_id)

        file_name = request.args.get("file") or None

        if not file_name:
            logger.info("No file name given")
            abort(404)

        settings = get_study_settings()
        files = ""
        if file_name == "metadata":
            file_list = get_basic_files(
                study_metadata_location, include_sub_dir=False, assay_file_list=None
            )
            for _file in file_list:
                f_type = _file["type"]
                f_name = _file["file"]
                if "metadata" in f_type:
                    files = files + f_name + "|"
            file_name = files.rstrip("|")

        remove_file = False
        safe_path = os.path.join(study_metadata_location, file_name)
        zip_name = None
        try:
            download_folder_path = os.path.join(
                study_metadata_location,
                settings.internal_files_symbolic_link_name,
                "temp",
            )
            os.makedirs(download_folder_path, exist_ok=True)
            short_zip = (
                study_id
                + "_"
                + str(random.randint(100000, 200000))
                + "_compressed_files.zip"
            )
            zip_name = os.path.join(download_folder_path, short_zip)
            if os.path.isfile(zip_name):
                os.remove(zip_name)
            if "|" in file_name and not os.path.exists(safe_path):
                zipfile = ZipFile(zip_name, mode="a")
                remove_file = True
                files = file_name.split("|")
                for file in files:
                    safe_path = os.path.join(study_metadata_location, file)
                    if os.path.isdir(safe_path):
                        for sub_file in recursively_get_files(safe_path):
                            f_name = sub_file.path.replace(study_metadata_location, "")
                            zipfile.write(sub_file.path, arcname=f_name)
                    else:
                        zipfile.write(safe_path, arcname=file)
                zipfile.close()
                remove_file = True
                safe_path = zip_name
                file_name = short_zip
            else:
                if os.path.isdir(safe_path):
                    zipfile = ZipFile(zip_name, mode="a")
                    for sub_file in recursively_get_files(safe_path):
                        zipfile.write(
                            sub_file.path.replace(study_metadata_location, ""),
                            arcname=sub_file.name,
                        )
                    zipfile.close()
                    remove_file = True
                    safe_path = zip_name
                    file_name = short_zip
                else:
                    head, tail = os.path.split(file_name)
                    file_name = tail

            resp = make_response(
                send_file(
                    safe_path, as_attachment=True, download_name=file_name, max_age=0
                )
            )
            # response.headers["Content-Disposition"] = "attachment; filename={}".format(file_name)
            resp.headers["Content-Type"] = "application/octet-stream"
            return resp
        except FileNotFoundError as e:
            abort(404, message="Could not find file " + file_name)
        except Exception as e:
            abort(404, message="Could not create zip file " + str(e))
        finally:
            if remove_file and os.path.exists(zip_name):
                os.remove(zip_name)
                logger.info("Removed zip file %s", zip_name)


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
                "dataType": "string",
            },
            {
                "name": "file",
                "description": "File(s) or folder name (comma separated, relative to study folder). Keyword 'metadata' can also be used.",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "passcode",
                "description": "One time token to access private study data files.",
                "paramType": "query",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication. "
                "Please provide a study id and a valid user token",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def get(self, study_id, obfuscation_code):
        result = validate_submission_view(request)
        study_id = result.context.study_id
        obfuscation_code = result.context.obfuscation_code
        study_metadata_location = get_study_metadata_path(study_id)
        file_name = request.args.get("file") or None

        if file_name is None:
            logger.info("No file name given")
            abort(404)
        files = ""
        if file_name == "metadata":
            file_list = get_basic_files(
                study_metadata_location, include_sub_dir=False, assay_file_list=None
            )
            for _file in file_list:
                f_type = _file["type"]
                f_name = _file["file"]
                if "metadata" in f_type:
                    files = files + f_name + "|"
            file_name = files.rstrip("|")

        remove_file = False
        safe_path = os.path.join(study_metadata_location, file_name)
        zip_name = None
        try:
            download_folder_path = os.path.join(
                study_metadata_location,
                get_study_settings().internal_files_symbolic_link_name,
                "temp",
            )
            os.makedirs(download_folder_path, exist_ok=True)
            short_zip = (
                study_id
                + "_"
                + str(random.randint(100000, 200000))
                + "_compressed_files.zip"
            )
            zip_name = os.path.join(download_folder_path, short_zip)
            if os.path.isfile(zip_name):
                os.remove(zip_name)
            if "|" in file_name and not os.path.exists(safe_path):
                zipfile = ZipFile(zip_name, mode="a")
                remove_file = True
                files = file_name.split("|")
                for file in files:
                    safe_path = os.path.join(study_metadata_location, file)
                    if os.path.isdir(safe_path):
                        for sub_file in recursively_get_files(safe_path):
                            f_name = sub_file.path.replace(study_metadata_location, "")
                            zipfile.write(sub_file.path, arcname=f_name)
                    else:
                        zipfile.write(safe_path, arcname=file)
                zipfile.close()
                remove_file = True
                safe_path = zip_name
                file_name = short_zip
            else:
                if os.path.isdir(safe_path):
                    zipfile = ZipFile(zip_name, mode="a")
                    for sub_file in recursively_get_files(safe_path):
                        zipfile.write(
                            sub_file.path.replace(study_metadata_location, ""),
                            arcname=sub_file.name,
                        )
                    zipfile.close()
                    remove_file = True
                    safe_path = zip_name
                    file_name = short_zip
                else:
                    head, tail = os.path.split(file_name)
                    file_name = tail

            resp = make_response(
                send_file(
                    safe_path, as_attachment=True, download_name=file_name, max_age=0
                )
            )
            # response.headers["Content-Disposition"] = "attachment; filename={}".format(file_name)
            resp.headers["Content-Type"] = "application/octet-stream"
            return resp
        except FileNotFoundError as e:
            abort(404, message="Could not find file " + file_name)
        except Exception as e:
            abort(404, message="Could not create zip file " + str(e))
        finally:
            if remove_file:
                os.remove(safe_path)
                logger.info("Removed zip file " + safe_path)


def recursively_get_files(base_dir):
    for entry in os.scandir(base_dir):
        if entry.is_file():
            yield entry
        elif entry.is_dir(follow_symlinks=False):
            yield from recursively_get_files(entry.path)
