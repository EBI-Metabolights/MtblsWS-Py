#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Jan-30
#  Modified by:   kenneth
#
#  Copyright 2020 EMBL - European Bioinformatics Institute
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
import shlex
import subprocess

from celery.result import AsyncResult
from flask import request
from flask_restful import Resource, abort
from flask_restful_swagger import swagger

from app.config import get_settings
from app.tasks.common_tasks.curation_tasks.chebi_pipeline import run_chebi_pipeline_task
from app.tasks.worker import celery
from app.utils import metabolights_exception_handler
from app.ws.auth.permissions import (
    raise_deprecation_error,
    validate_user_has_curator_role,
)
from app.ws.chebi_pipeline_utils import check_maf_for_pipes, print_log
from app.ws.redis.redis import get_redis_server
from app.ws.study.folder_utils import get_all_files_from_filesystem, write_audit_files
from app.ws.study.utils import get_study_metadata_path
from app.ws.utils import get_assay_file_list

logger = logging.getLogger("wslog")

NOT_COMPLETED_STATES = {
    "PENDING",
    "STARTED",
    "INITIATED",
    "RECEIVED",
    "STARTED",
    "RETRY",
    "PROGRESS",
}


class ChEBIPipeLine(Resource):
    @swagger.operation(
        summary="Search external resources using compound names in MAF (curator only)",
        nickname="ChEBI automated pipeline",
        notes="""Search and populate a given Metabolite Annotation File based on the 'metabolite_identification' column.
              New MAF files will be created in the 'chebi_pipeline_annotations' folder with extension '_pubchem.tsv'. These form part of
              the ChEBI submission pipeline. If no annotation_file_name is given, all MAF in the study are processed""",
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
                "name": "annotation_file_name",
                "description": "Metabolite Annotation File name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "classyfire_search",
                "description": "Search ClassyFire?",
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
            {
                "name": "update_study_maf",
                "description": "Update (overwrite) the submitted MAF directly. Classyfire will not be searched",
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": False,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The Metabolite Annotation File (MAF) is returned",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def post(self, study_id):
        settings = get_settings()
        permission = validate_user_has_curator_role(request, study_required=True)
        study_id = permission.context.study_id
        email = permission.context.username
        user_token = permission.context.user_api_token

        annotation_file_name = request.args.get("annotation_file_name")
        classyfire_search = request.args.get("classyfire_search")
        update_study_maf = request.args.get("update_study_maf")

        classyfire_search = True if classyfire_search == "true" else False
        update_study_maf = True if update_study_maf == "true" else False
        study_metadata_location = os.path.join(
            settings.study.mounted_paths.study_metadata_files_root_path, study_id
        )
        print_log("Creating a new study audit folder for study %s", study_id)
        write_audit_files(study_metadata_location)

        redis = get_redis_server()
        key = f"chebi_pipeline:{study_id}"
        task_id = None
        try:
            task_id = redis.get_value(key).decode()
        except Exception as exc:
            logger.warning("Error parsing redis value")

        if task_id:
            result: AsyncResult = celery.AsyncResult(task_id)
            if result and result.state in NOT_COMPLETED_STATES:
                abort(
                    401,
                    message=f"There is a task ({result.id}) in queue with status {result.state}",
                )

        inputs = {
            "study_id": study_id,
            "user_token": user_token,
            "annotation_file_name": annotation_file_name,
            "classyfire_search": classyfire_search,
            "update_study_maf": update_study_maf,
            "email": email,
        }
        task = run_chebi_pipeline_task.apply_async(kwargs=inputs, expires=60)
        return {
            "message": f"CHEBI Pipeline task is started for {study_id} {annotation_file_name}. Task id: {task.id}. Results will be send by email."
        }


class CheckCompounds(Resource):
    @swagger.operation(
        summary="Search external resources using compound names",
        nickname="Search compound names",
        notes="Search various resources based on compound names",
        parameters=[
            {
                "name": "compound_names",
                "description": "Compound names, one per line",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The Metabolite Annotation File (MAF) is returned",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def get(self):
        raise_deprecation_error(request)
        compound_names = request.args.get("compound_names")

        return {"success": compound_names}


class ChEBIPipeLineLoad(Resource):
    @swagger.operation(
        summary="Load generate SDF files into ChEBI (curator only)",
        nickname="Load ChEBI compounds",
        notes="",
        parameters=[
            {
                "name": "sdf_file_name",
                "description": "Metabolite Annotation File name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The Metabolite Annotation File (MAF) is returned",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def post(self):
        validate_user_has_curator_role(request)
        sdf_file_name = request.args.get("sdf_file_name")

        shell_script = get_settings().chebi.pipeline.chebi_upload_script
        command = shell_script
        if sdf_file_name:
            command = shlex.split(shell_script + " " + sdf_file_name)
        if subprocess.call(command) == 0:
            return {"Success": "ChEBI upload script started"}
        else:
            return {"Warning": "ChEBI upload script started"}


class SplitMaf(Resource):
    @swagger.operation(
        summary="MAF pipeline splitter (curator only)",
        nickname="Add rows based on pipeline splitting",
        notes="Split a given Metabolite Annotation File based on pipelines in cells. "
        "A new MAF will be created with extension '.split'. "
        "If no annotation_file_name is given, all MAF in the study is processed",
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
                "name": "annotation_file_name",
                "description": "Metabolite Annotation File name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK. The Metabolite Annotation File (MAF) is returned",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @metabolights_exception_handler
    def post(self, study_id):
        result = validate_user_has_curator_role(request, study_required=True)
        study_id = result.context.study_id
        obfuscation_code = result.context.obfuscation_code

        http_base_location = get_settings().server.service.ws_app_base_link
        http_file_location = http_base_location + os.sep + study_id + os.sep + "files"

        study_metdata_location = get_study_metadata_path(study_id)
        annotation_file_name = None
        if request.args.get("annotation_file_name"):
            annotation_file_name = request.args.get("annotation_file_name").strip()

        if not annotation_file_name:
            # Loop through all m_*_v2_maf.tsv files
            (
                study_files,
                _upload_files,
                _upload_diff,
                _upload_location,
                latest_update_time,
            ) = get_all_files_from_filesystem(
                study_id,
                obfuscation_code,
                study_metdata_location,
                directory=None,
                include_raw_data=False,
                assay_file_list=get_assay_file_list(study_metdata_location),
            )  # ToDo, Overkill just loop through the folder
            maf_count = 0
            maf_changed = 0
            for file in study_files:
                file_name = file["file"]
                if file_name.startswith("m_") and file_name.endswith("_v2_maf.tsv"):
                    maf_count += 1
                    maf_df, maf_len, new_maf_df, new_maf_len, split_file_name = (
                        check_maf_for_pipes(study_metdata_location, file_name)
                    )
                    if maf_len != new_maf_len:
                        maf_changed += 1
        else:
            if not annotation_file_name.endswith(
                "_maf.tsv"
            ) and not annotation_file_name.endswith("_pubchem.tsv"):
                abort(
                    404,
                    message="Annotation file name must end with '_maf.tsv' or '_pubchem.tsv'",
                )

            maf_df, maf_len, new_maf_df, new_maf_len, split_file_name = (
                check_maf_for_pipes(study_metdata_location, annotation_file_name)
            )

            return {
                "maf_rows": maf_len,
                "new_maf_rows": new_maf_len,
                "file_name": http_file_location + split_file_name.split(study_id)[1],
            }

        return {
            "success": str(maf_count)
            + " MAF files checked for pipelines, "
            + str(maf_changed)
            + " files needed updating."
        }
