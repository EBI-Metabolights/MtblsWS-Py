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
import logging
import os
from flask import request
from flask_restful import Resource, abort
from flask_restful_swagger import swagger
import pandas as pd
from app.utils import metabolights_exception_handler
from app.ws.db_connection import update_study_sample_type
from app.ws.isa_table_templates import create_sample_sheet

from app.ws.mtblsWSclient import WsClient
from app.ws.settings.utils import get_study_settings
from app.ws.study.study_folder_service import StudyFolderService
from app.ws.utils import read_tsv


logger = logging.getLogger("wslog")
wsc = WsClient()


class StudySampleTemplate(Resource):
    @swagger.operation(
        summary="Init Sample sheet",
        notes="""Initiate or Override sample sheet for given type from the template""",
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
                "name": "sample_type",
                "description": "Type of Sample",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": [
                    "minimum",
                    "clinical",
                    "in-vitro",
                    "only-in-vitro",
                    "non-clinical-tox",
                    "plant",
                    "isotopologue",
                    "metaspace-imaging",
                    "nmr-imaging",
                    "ms-imaging",
                    "minimum-bsd",
                ],
                "defaultValue": "minimum",
                "default": "minimum",
            },
            {
                "name": "template_version",
                "description": "Version of sample template. e.g., 1.0, 2.0",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "defaultValue": "",
                "default": "",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "force",
                "description": "Force overriding of sample sheet regardless of sample data if set True. Do nothing if data present and value set false",
                "required": False,
                "allowMultiple": False,
                "allowEmptyValue": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["True", "False"],
                "defaultValue": "False",
                "default": "False",
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
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
        # param validation
        if study_id is None:
            abort(404)
        study_id = study_id.upper()

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # query validation        sampleType = None
        force_override = False
        if request.args:
            template_version = request.args.get("sample_type").get(
                "template_version", None
            )
            if not template_version:
                template_version = (
                    get_study_settings().default_metadata_template_version
                )
            sample_type = request.args.get("sample_type", "").lower()
            if not sample_type:
                sample_type = "minimum"

            force_override = (
                True if request.args.get("force", "").lower() == "true" else False
            )

        logger.info(
            "Init Sample for %s; Type %s with version %s",
            study_id,
            sample_type,
            template_version,
        )
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not is_curator:
            abort(403)
        settings = get_study_settings()
        studies_path = settings.mounted_paths.study_metadata_files_root_path
        study_path = os.path.join(studies_path, study_id)

        sample_file_name = "s_" + study_id.upper() + ".txt"
        sample_file_fullpath = os.path.join(study_path, sample_file_name)
        have_data = False
        if os.path.exists(sample_file_fullpath):
            data: pd.DataFrame = read_tsv(sample_file_fullpath)
            row_count = len(data)
            if row_count > 1:
                have_data = True

        if have_data:
            StudyFolderService.create_audit_folder_with_study_id(study_id)

        status, filename = create_sample_sheet(
            study_id=study_id,
            file_path=study_path,
            sample_type=sample_type,
            template_version=template_version,
        )
        update_status = False
        if status:
            update_status = update_study_sample_type(
                study_id=study_id, sample_type=sample_type
            )
        if update_status:
            return {
                "The sample sheet creation status": "Successful",
                "filename": filename,
            }
        else:
            return {"The sample sheet creation status": "Unsuccessful"}
