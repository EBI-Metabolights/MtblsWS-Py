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

import datetime
import json
import logging
import os
import pathlib
import random
import time
import uuid
from pathlib import Path
from zipfile import ZIP_STORED, ZipFile

from flask import (
    Response,
    make_response,
    redirect,
    request,
    send_file,
)
from flask_restful import Resource, abort
from flask_restful_swagger import swagger

from app.config import get_settings
from app.tasks.datamover_tasks.basic_tasks.file_management import copy, exists, isdir
from app.utils import metabolights_exception_handler
from app.ws.auth.permissions import validate_submission_view
from app.ws.mtblsWSclient import WsClient
from app.ws.settings.utils import get_study_settings
from app.ws.study.folder_utils import get_basic_files
from app.ws.study.utils import (
    get_cluster_study_data_files_path,
    get_study_audit_files_path,
    get_study_internal_files_path,
    get_study_metadata_path,
)

logger = logging.getLogger("wslog")
# MetaboLights (Java-Based) WebService client
wsc = WsClient()


def generate_file_chunks_with_cleanup(file_path, cleanup_func, chunk_size=8192):
    """Generator function to read file in chunks for streaming, then run cleanup."""
    try:
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk
    finally:
        cleanup_func()


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
    @metabolights_exception_handler
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
        file_path = request.args.get("file") or ""

        if not file_path:
            logger.info("No file name given")
            abort(404, message="No file name given")

        parts = file_path.split("/")
        initial_path = parts[0]
        basename = os.path.basename(file_path)
        if initial_path == "FILES":
            response = self.create_data_file_response(
                study_id, obfuscation_code, file_path
            )
            if response:
                return response
            settings = get_settings()
            study_data_file_index_path = os.path.join(
                settings.study.mounted_paths.study_internal_files_root_path,
                study_id,
                "DATA_FILES",
                "data_file_index.json",
            )
            if os.path.exists(study_data_file_index_path):
                data_files = json.loads(
                    pathlib.Path(study_data_file_index_path).read_text()
                )
                if file_path in data_files.get("public_data_files", {}):
                    public_config = settings.ftp_server.public.configuration

                    redirect_url = os.path.join(
                        public_config.public_studies_http_base_url, file_path
                    )
                    return redirect(redirect_url)
            raise Exception(f"Invalid file {file_path}")

        study_metadata_location = get_study_metadata_path(study_id)

        target_path = os.path.join(study_metadata_location, file_path)

        if initial_path == "INTERNAL_FILES":
            root_path = get_study_internal_files_path(study_id)
            subpath = file_path.replace("INTERNAL_FILES", "", 1).lstrip("/")
            target_path = os.path.join(root_path, subpath)
        elif initial_path == "AUDIT_FILES":
            root_path = get_study_audit_files_path(study_id)
            subpath = file_path.replace("INTERNAL_FILES", "", 1).lstrip("/")
            target_path = os.path.join(root_path, subpath)

        files = ""
        if file_path == "metadata":
            file_list = get_basic_files(
                study_metadata_location, include_sub_dir=False, assay_file_list=None
            )
            for _file in file_list:
                f_type = _file["type"]
                f_name = _file["file"]
                if "metadata" in f_type:
                    files = files + f_name + "|"
            file_path = files.rstrip("|")

        remove_file = False
        safe_path = os.path.join(study_metadata_location, file_path)
        zip_name = None
        try:
            download_folder_path = os.path.join(
                get_study_internal_files_path(study_id), "temp"
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
            if "|" in file_path and not os.path.exists(safe_path):
                zipfile = ZipFile(zip_name, mode="a")
                remove_file = True
                files = file_path.split("|")
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
                    file_name = basename
                    safe_path = target_path

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

    def create_data_file_response(
        self, study_id, obfuscation_code, file_path
    ) -> Response:
        parts = file_path.split("/")
        basename = os.path.basename(file_path)
        ftp_folder_path = get_cluster_study_data_files_path(study_id, obfuscation_code)
        source_path = os.path.join(ftp_folder_path, "/".join(parts[1:]))
        shared_path = get_settings().hpc_cluster.datamover.shared_path
        now = datetime.datetime.now()
        this_month = now.strftime("%Y-%m")
        now_str = now.strftime("%Y-%m-%d_%H-%M-%S")
        folder_name = (
            f"temp-download-data-files/{this_month}/{now_str}_{uuid.uuid4().hex[:6]}"
        )
        target_folder = os.path.join(shared_path, folder_name)
        shared_target_path = os.path.join(target_folder, basename)

        try:
            task = exists.apply_async(kwargs={"source_path": source_path}, expires=10)
            file_on_ftp_folder = task.get(timeout=10)
            if not file_on_ftp_folder:
                return None
            os.makedirs(target_folder, exist_ok=True)
            inputs = {
                "source_path": source_path,
                "target_path": shared_target_path,
            }
            exists_task = isdir.apply_async(
                kwargs={"source_path": source_path}, expires=1
            )
            is_dir = exists_task.get(timeout=2)
            process_folder = False
            if is_dir:
                _, ext = os.path.splitext(basename)
                try:
                    int_val = int(ext)
                except Exception:
                    int_val = 0
                if int_val > 0 or ext.lower() in [".d", ".raw"]:
                    process_folder = True
                else:
                    raise Exception("Source file is a directory")

            task = copy.apply_async(kwargs=inputs, expires=60)
            result = task.get(timeout=120)
            if result and result.get("status"):
                if process_folder:
                    zip_file_path = shared_target_path + ".zip"
                    if os.path.isfile(zip_file_path):
                        os.remove(zip_file_path)
                    with ZipFile(zip_file_path, "w", compression=ZIP_STORED) as zipfile:
                        src_path = Path(shared_target_path)
                        for file in src_path.rglob("*"):
                            zipfile.write(
                                file, arcname=file.relative_to(src_path.parent)
                            )
                    shared_target_path = zip_file_path
                    basename += ".zip"
                # Stream file in chunks
                # response = Response(
                #     stream_with_context(
                #         generate_file_chunks_with_cleanup(
                #             local_target_path,
                #             lambda: (
                #                 shutil.rmtree(target_folder)
                #                 if os.path.exists(target_folder)
                #                 else None
                #             ),
                #         )
                #     ),
                #     content_type="application/octet-stream",
                #     headers={
                #         "Content-Disposition": f'attachment; filename=basename'
                #     },
                # )
                # target_file  = AutoCleanupFile(file_path=shared_target_path)
                for _ in range(3):
                    if os.path.exists(shared_target_path):
                        break
                    time.sleep(1)
                if not os.path.exists(shared_target_path):
                    abort(404, message="file not found")
                response = make_response(
                    send_file(
                        shared_target_path,
                        as_attachment=True,
                        download_name=basename,
                        max_age=0,
                    )
                )
                response.headers["Content-Type"] = "application/octet-stream"
                return response
            if result:
                message = "Error while processing send data file task"
                logger.error(f"{result.get('message', message)}")
                raise Exception(message)
        except Exception as ex:
            raise ex


def recursively_get_files(base_dir):
    for entry in os.scandir(base_dir):
        if entry.is_file():
            yield entry
        elif entry.is_dir(follow_symlinks=False):
            yield from recursively_get_files(entry.path)
