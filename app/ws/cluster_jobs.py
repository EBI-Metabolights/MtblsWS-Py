#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2022-Oct-13
#  Modified by:   Felix Amaladoss
#
#  Copyright 2022 EMBL - European Bioinformatics Institute
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
from typing import List
from flask_restful import Resource, abort
from flask_restful_swagger import swagger
from flask import request
from app.config import get_settings
from app.services.cluster.hpc_client import HpcJob, SubmittedJobResult
from app.services.cluster.hpc_utils import get_new_hpc_compute_client, get_new_hpc_datamover_client
from app.utils import current_time
from app.ws.db_connection import check_access_rights
from app.ws.study.user_service import UserService

logger = logging.getLogger('wslog')


def submit_job(email=False, account=None, queue=None, job_cmd=None, job_params=None, identifier=None, taskname=None , log=False,
               log_path=None):
    settings = get_settings()

    if job_cmd is None:
        return False, "JOB command should not be empty!", "No LSF job output", "No LSF job error"
    
    if queue is None:
        queue = settings.hpc_cluster.compute.default_queue
    
    if queue == settings.hpc_cluster.datamover.default_queue:
        client = get_new_hpc_datamover_client()
    else:
        client = get_new_hpc_compute_client()
    if email:
        if account is None:
            account = settings.email.email_service.configuration.hpc_cluster_job_track_email_address
    timestamp_str = str(int(current_time().timestamp()*1000))
    script_name = f"job_script_{timestamp_str}.sh"

    script_path = os.path.join(settings.server.temp_directory_path, "temp_commands", script_name)
    with open(script_path, "w") as f:
        command = f"{job_cmd} {job_params}" if job_params else job_cmd
        f.write(f"{command}\n")
    if not taskname:
        taskname = f"task_{timestamp_str}"
    job_name = identifier + "_" + taskname if identifier else "None_" + taskname
    if not log_path:
        now = current_time()
        date_time = now.strftime("%d-%m-%Y_%H-%M-%S")
        log_root_path = client.settings.job_track_log_location
        log_path = os.path.join(log_root_path, job_name + "_" + date_time + ".log")
    
    result: SubmittedJobResult = client.submit_hpc_job(script_path=script_path,
                                   job_name=job_name, 
                                   output_file=log_path, 
                                   account=account,
                                   queue=queue)
    if result.job_ids:
        return True, f"Job submitted successfully. Job id {result.job_ids[0]}", "\n".join(result.stdout), "\n".join(result.stderr), log_path
    else:
        return False, "Job submission failed", "\n".join(result.stdout), "\n".join(result.stderr), log_path
        


def list_jobs(queue=None, job_name=None):
    settings = get_settings()

    if queue == settings.hpc_cluster.datamover.default_queue:
        client = get_new_hpc_datamover_client()
    else:
        client = get_new_hpc_compute_client()
    jobs: List[HpcJob] = client.get_job_status()
    if not job_name:
        return jobs
    
    return [job for job in jobs if job_name == job.name]
            


def kill_job(queue=None, job_id=None):
    settings = get_settings()
    if job_id is None:
        return False, "Job id should not be empty!", "No LSF job output", "No LSF job error"
    if queue is None:
        return False, "Queue should not be empty!", "No LSF job output", "No LSF job error"
    if queue == settings.hpc_cluster.datamover.default_queue:
        client = get_new_hpc_datamover_client()
    else:
        client = get_new_hpc_compute_client()
    result: SubmittedJobResult = client.kill_jobs(job_id_list=[job_id], failing_gracefully=True)
    if result.job_ids:
        return True, f"Job id {result.job_ids[0]} deleted", "\n".join(result.stdout), "\n".join(result.stderr)
    else:
        return False, "Job not deleted", "\n".join(result.stdout), "\n".join(result.stderr)


def get_permissions(study_id, user_token, obfuscation_code=None):
    """
    Check MTBLS-WS for permissions on this Study for this user

    Study       User    Submitter   Curator     Reviewer/Read-only
    PROVISIONAL   ----    Read+Write  Read+Write  Read
    PRIVATE  ----    Read        Read+Write  Read
    INREVIEW    ----    Read        Read+Write  Read
    PUBLIC      Read    Read        Read+Write  Read

    :param obfuscation_code:
    :param study_id:
    :param user_token:
    :return: study details and permission levels

    """
    if not user_token:
        user_token = "public_access_only"

    # Reviewer access will pass the study obfuscation code instead of api_key
    if study_id and not obfuscation_code and user_token.startswith("ocode:"):
        logger.info("Study obfuscation code passed instead of user API_CODE")
        obfuscation_code = user_token.replace("ocode:", "")

    is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
    updated_date, study_status = check_access_rights(user_token, study_id.upper(),
                                                     study_obfuscation_code=obfuscation_code)

    logger.info("Read access: " + str(read_access) + ". Write access: " + str(write_access))

    return is_curator, read_access, write_access, obfuscation_code, study_location, release_date, \
           submission_date, study_status


class LsfUtils(Resource):
    @swagger.operation(
        summary="[Deprecated] Kill a EBI LSF cluster job (curator only)",
        parameters=[
            {
                "name": "job_id",
                "description": "running LSF job id",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "queue",
                "description": "queue on which job to be submitted",
                "required": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "user-token",
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
                "message": "Unauthorized. Access to the resource requires user authentication. "
                           "Please provide a study id and a valid user token"
            },
            {
                "code": 403,
                "message": "Forbidden. Please provide a valid user token"
            },
            {
                "code": 404,
                "message": "Not found. The requested job identifier is not valid or no longer exist"
            }
        ]
    )
    def delete(self):
        user_token = None
        lsf_job_id = ""

        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None:
            abort(401)
        if request.args:
            
            lsf_job_id = request.args.get('job_id')
            queue = request.args.get('queue')

        if lsf_job_id is None:
            abort(404)

        # param validation
        UserService.get_instance().validate_user_has_curator_role(user_token)

        status, message, job_out, job_err = kill_job(queue, lsf_job_id)

        if status:
            return {"success": message, "message": job_out, "error": job_err}
        else:
            return {"error": message, "message": job_out, "error": job_err}

    @swagger.operation(
        summary="List all LSF cluster jobs running on codon (curator only)",
        parameters=[
            {
                "name": "queue",
                "description": "queue on which job to be submitted",
                "required": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },{
                "name": "job_name",
                "description": "Job name to be queried on queue",
                "required": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "user-token",
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
                "message": "Unauthorized. Access to the resource requires user authentication. "
                           "Please provide a study id and a valid user token"
            },
            {
                "code": 403,
                "message": "Forbidden. Please provide a valid user token"
            },
            {
                "code": 404,
                "message": "Not found. The requested job identifier is not valid or no longer exist"
            }
        ]
    )
    def get(self):
        user_token = None

        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None:
            abort(401)

        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = get_permissions('MTBLS2', user_token)
        if not is_curator:
            abort(403)
        if request.args:
            
            queue = request.args.get('queue')
            job_name = request.args.get('job_name')

        status, message, job_out, job_err = list_jobs(queue, job_name)

        if status:
            return {"success": message, "message": job_out, "error": job_err}
        else:
            return {"error": message, "message": job_out, "error": job_err}

    @swagger.operation(
        summary="Submit a new cluster job on Codon (curator only)",
        parameters=[
            {
                "name": "command",
                "description": "command to run on the LSF cluster",
                "required": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "params",
                "description": "Params with space separated",
                "required": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "email",
                "description": "email to receive output",
                "required": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "queue",
                "description": "queue on which job to be submitted",
                "required": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "user-token",
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
                "message": "Unauthorized. Access to the resource requires user authentication. "
                           "Please provide a study id and a valid user token"
            },
            {
                "code": 403,
                "message": "Forbidden. Please provide a valid user token"
            },
            {
                "code": 404,
                "message": "Not found. The requested job identifier is not valid or no longer exist"
            }
        ]
    )
    def post(self):
        user_token = None
        command = None

        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None:
            abort(401)
        
        
        if request.args:
            
            command = request.args.get('command')
            params = request.args.get('params')
            email = request.args.get('email')
            queue = request.args.get('queue')

        if command is None:
            abort(404)

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = get_permissions('MTBLS2', user_token)
        if not is_curator:
            abort(403)

        status, message, job_out, job_err, log_file = submit_job(True, email, queue, command, params, None, True)
        if log_file:
            logger.info(" Output file " + log_file)
        else:
            logger.info(" There is no output file")
        
        if status:
            return {"Success": message, "message": job_out, "error": job_err}
        else:
            return {"Failure": message, "message": job_out, "error": job_err}
