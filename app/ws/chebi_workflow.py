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

import glob
import logging
import os
import re
import shlex
from typing import List
from pathlib import Path

import numpy as np
import pandas as pd
import pubchempy as pcp
import subprocess
from flask import request
from flask_restful import Resource, reqparse, abort
from flask_restful_swagger import swagger
from pubchempy import get_compounds
from zeep import Client
from unidecode import unidecode
from app.config import get_settings

from app.services.storage_service.acl import Acl
from app.services.storage_service.storage import Storage
from app.services.storage_service.storage_service import StorageService
from app.tasks.bash_client import BashClient
from app.tasks.lsf_client import LsfClient
from app.ws.chebi_pipeline_utils import check_maf_for_pipes, print_log, run_chebi_pipeline
from app.ws.cluster_jobs import submit_job
from app.ws.db_connection import get_user_email
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.settings.utils import get_study_settings
from app.ws.study.folder_utils import write_audit_files, get_all_files_from_filesystem
from app.ws.study.study_service import StudyService
from app.ws.study.user_service import UserService
from app.ws.utils import read_tsv, write_tsv, get_assay_file_list, safe_str

logger = logging.getLogger('wslog_chebi')

# MetaboLights (Java-Based) WebService client
wsc = WsClient()


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
                "dataType": "string"
            },
            {
                "name": "annotation_file_name",
                "description": "Metabolite Annotation File name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "classyfire_search",
                "description": "Search ClassyFire?",
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False
            },
            {
                "name": "run_silently",
                "description": "Do not generate console or log info when skipping rows",
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False
            },
            {
                "name": "run_on_cluster",
                "description": "Run in the background on the EBI LSF cluster",
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False
            },
            {
                "name": "update_study_maf",
                "description": "Update (overwrite) the submitted MAF directly. Classyfire will not be searched",
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": False,
                "format": "application/json",
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
                "message": "OK. The Metabolite Annotation File (MAF) is returned"
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
        settings = get_settings()

        # param validation
        if study_id is None:
            abort(404, messge='Please provide valid parameter for study identifier')
        study_id = study_id.upper()
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        UserService.get_instance().validate_user_has_curator_role(user_token)

        cluster_job = None
        try:
            cluster_job = request.args['source']
        except:
            pass

        run_silently = None
        run_on_cluster = None
        if cluster_job:
            if bool(request.data):
                x = re.split('=|&|,|\;', str(request.data)[2:-1])
                data = {x[i]: x[i + 1] for i in range(0, len(x), 2)}
                annotation_file_name = data['annotation_file_name']
                classyfire_search = data['classyfire_search']
                update_study_maf = data['update_study_maf']
        else:
            annotation_file_name = request.args['annotation_file_name']
            classyfire_search = request.args['classyfire_search']
            update_study_maf = request.args.get('update_study_maf')
            run_silently = request.args['run_silently']
            run_on_cluster = request.args['run_on_cluster']

        classyfire_search = True if classyfire_search == 'true' else False
        run_silently = True if run_silently == 'true' else False
        run_on_cluster = True if run_on_cluster == 'true' else False
        update_study_maf = True if update_study_maf == 'true' else False
        study_metadata_location = os.path.join(settings.study.mounted_paths.study_metadata_files_root_path, study_id)
        print_log("Creating a new study audit folder for study %s", study_id)
        audit_status, dest_path = write_audit_files(study_metadata_location)
        # file_path = settings.chebi.pipeline.run_standalone_chebi_pipeline_python_file
        # root_path = settings.hpc_cluster.singularity.docker_deployment_path
        # actual_path = os.path.join(root_path, file_path)
        # if run_on_cluster:
        # command = f"python3 {actual_path}"
        # command_arguments = f'{study_id} {user_token} "{annotation_file_name}", "{run_silently}" "{classyfire_search}" "{update_study_maf}"'
        # client: LsfClient = LsfClient()
        # task_name=f"chebi_pipeline_{study_id}"
        
        # job_id, messages = client.run_singularity(task_name, command=command, command_arguments=command_arguments, unique_task_name=True, hpc_queue_name=None)
        return run_chebi_pipeline(study_id, user_token, annotation_file_name, run_silently=run_silently, classyfire_search=classyfire_search, update_study_maf=update_study_maf, run_on_cluster=run_on_cluster)
        # return {"job_id": job_id, "messages": messages}


class CheckCompounds(Resource):
    @swagger.operation(
        summary="Search external resources using compound names",
        nickname="Search compound names",
        notes="Search various resources based on compound names",
        parameters=[
            {
                "name": "compound_names",
                "description": 'Compound names, one per line',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
                "message": "OK. The Metabolite Annotation File (MAF) is returned"
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
    def get(self):

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions('MTBLS1', user_token)
        if not read_access:
            abort(403)

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('compound_names', help='compound_names')
        args = parser.parse_args()
        compound_names = args['compound_names']

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
                "message": "OK. The Metabolite Annotation File (MAF) is returned"
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
    def post(self):
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        UserService.get_instance().validate_user_has_curator_role(user_token)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('sdf_file_name', help="SDF File to load into ChEBI", location="args")
        args = parser.parse_args()
        sdf_file_name = args['sdf_file_name']

        shell_script = get_settings().chebi.pipeline.chebi_upload_script
        command = shell_script
        if sdf_file_name:
            command = shlex.split(shell_script + ' ' + sdf_file_name)
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
                "dataType": "string"
            },
            {
                "name": "annotation_file_name",
                "description": "Metabolite Annotation File name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
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
                "message": "OK. The Metabolite Annotation File (MAF) is returned"
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
        http_base_location = get_settings().server.service.ws_app_base_link
        http_file_location = http_base_location + os.sep + study_id + os.sep + 'files'

        # param validation
        if study_id is None:
            abort(404, messge='Please provide valid parameter for study identifier')

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location_deprecated, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not is_curator:
            abort(403)
        settings = get_settings()
        study_metdata_location = os.path.join(settings.study.mounted_paths.study_metadata_files_root_path, study_id)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('annotation_file_name', help="Metabolite Annotation File", location="args")
        args = parser.parse_args()
        annotation_file_name = None
        if args['annotation_file_name']:
            annotation_file_name = args['annotation_file_name'].strip()

        if not annotation_file_name:
            # Loop through all m_*_v2_maf.tsv files
            study_files, _upload_files, _upload_diff, _upload_location, latest_update_time = \
                get_all_files_from_filesystem(
                    study_id, obfuscation_code, study_metdata_location, directory=None, include_raw_data=False,
                    assay_file_list=get_assay_file_list(study_metdata_location))  # ToDo, Overkill just loop through the folder
            maf_count = 0
            maf_changed = 0
            for file in study_files:
                file_name = file['file']
                if file_name.startswith('m_') and file_name.endswith('_v2_maf.tsv'):
                    maf_count += 1
                    maf_df, maf_len, new_maf_df, new_maf_len, split_file_name = \
                        check_maf_for_pipes(study_metdata_location, file_name)
                    if maf_len != new_maf_len:
                        maf_changed += 1
        else:

            if not annotation_file_name.endswith('_maf.tsv') and not annotation_file_name.endswith('_pubchem.tsv'):
                abort(404, messge="Annotation file name must end with '_maf.tsv' or '_pubchem.tsv'")

            maf_df, maf_len, new_maf_df, new_maf_len, split_file_name = \
                check_maf_for_pipes(study_metdata_location, annotation_file_name)

            return {"maf_rows": maf_len, "new_maf_rows": new_maf_len,
                    "file_name": http_file_location + split_file_name.split(study_id)[1]}

        return {"success": str(maf_count) + " MAF files checked for pipelines, " +
                           str(maf_changed) + " files needed updating."}