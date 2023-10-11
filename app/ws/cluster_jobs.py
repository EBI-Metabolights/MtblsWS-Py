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

import subprocess
import logging
import os
from flask_restful import Resource, reqparse, abort
from flask_restful_swagger import swagger
from flask import request
from datetime import datetime
from app.config import get_settings
from app.ws.db_connection import check_access_rights
from app.ws.study.user_service import UserService
from app.ws.settings.utils import get_cluster_settings

logger = logging.getLogger('wslog')


def submit_job(email=False, account=None, queue=None, job_cmd=None, job_params=None, identifier=None, taskname=None , log=False,
               log_path=None):
    cluster_settings = get_cluster_settings()
    settings = get_settings()
    msg_out = "No LSF job output"
    msg_err = "No LSF job error"
    bsub_cmd = cluster_settings.job_submit_command
    lsf_host = settings.hpc_cluster.compute.connection.host
    lsf_host_user = settings.hpc_cluster.compute.connection.username
    ssh_cmd = settings.hpc_cluster.ssh_command
    log_file_path = None
    job_cmd1 = job_cmd
    
    if email:
        if account is None:
            account = settings.email.email_service.configuration.hpc_cluster_job_track_email_address

        bsub_cmd = bsub_cmd + " -u " + account

    if job_cmd is None:
        return False, "JOB command should not be empty!", str(msg_out), str(msg_err)

    if queue is None:
        queue = settings.hpc_cluster.compute.default_queue

    if queue == settings.hpc_cluster.datamover.queue_name:
        lsf_host = settings.hpc_cluster.datamover.connection.host
        lsf_host_user = settings.hpc_cluster.datamover.connection.username

    bsub_cmd = bsub_cmd + " -W 1440 -q " + queue
    ssh_cmd = ssh_cmd + " " + lsf_host_user + "@" + lsf_host

    if job_params:
        job_cmd1 = job_cmd1 + " " + job_params

    if identifier is None:
        identifier = "None"

    if log:
        if log_path is None:
            log_file_location = cluster_settings.job_track_log_location
            now = datetime.now()
            date_time = now.strftime("%d-%m-%Y_%H-%M-%S")
            log_file_path = log_file_location + "/" + identifier + "_" + taskname + "_" + date_time + ".log"
        else:
            log_file_path = log_path
        bsub_cmd = bsub_cmd + " -o " + log_file_path

    if job_cmd == 'rsync':
        bsub_cmd = bsub_cmd + " -J " + identifier + "_" + taskname
    cmd = ssh_cmd + " " + bsub_cmd + " " + job_cmd1
    status = False
    try:
        logger.info(" LSF command executing  : " + cmd)
        job_status = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True, check=True)
        if job_status.stdout:
            msg_out = job_status.stdout.decode("utf-8")
        if job_status.stderr:
            msg_err = job_status.stderr.decode("utf-8")
        status = True
        message = "Successfully submitted LSF job to codon cluster; job command '" + job_cmd1
    except Exception as e:
        message = 'Could not execute job ' + cmd + ' on LSF '
        logger.error(message + '. ' + str(e))

    return status, message, str(msg_out), str(msg_err), log_file_path


def list_jobs(queue=None, job_name=None):
    cluster_settings = get_cluster_settings()
    msg_out = "No LSF job output"
    msg_err = "No LSF job error"
    bjobs_cmd = cluster_settings.job_running_command
    
    settings = get_settings()
    lsf_host = settings.hpc_cluster.compute.connection.host
    lsf_host_user = settings.hpc_cluster.compute.connection.username
    ssh_cmd = settings.hpc_cluster.ssh_command

    if queue is None:
        queue = settings.hpc_cluster.compute.default_queue

    if queue == settings.hpc_cluster.datamover.queue_name:
        lsf_host = settings.hpc_cluster.datamover.connection.host
        lsf_host_user = settings.hpc_cluster.datamover.connection.username

    bjobs_cmd = f'{bjobs_cmd} -q {queue}'
    if job_name:
        bjobs_cmd = f'{bjobs_cmd} -J {job_name}'
    ssh_cmd = ssh_cmd + " " + lsf_host_user + "@" + lsf_host

    cmd = ssh_cmd + " " + bjobs_cmd

    try:
        logger.info(" LSF command executing  : " + cmd)
        job_status = subprocess.run(cmd, capture_output=True, shell=True, check=True)
        if job_status.stdout:
            msg_out = job_status.stdout.decode("utf-8")
        if job_status.stderr:
            msg_err = job_status.stderr.decode("utf-8")
        status = True
        message = "Successfully listed LSF jobs on codon cluster"
    except Exception as e:
        message = 'Could not list jobs from codon ; ' + queue
        status = False
        logger.error(message + ' ;  reason  :-' + str(e))

    return status, message, str(msg_out), str(msg_err)


def kill_job(queue=None, job_id=None):
    cluster_settings = get_cluster_settings()
    msg_out = "No LSF job output"
    msg_err = "No LSF job error"
    bkill_cmd = cluster_settings.job_kill_command
    settings = get_settings()
    lsf_host = settings.hpc_cluster.compute.connection.host
    lsf_host_user = settings.hpc_cluster.compute.connection.username
    ssh_cmd = settings.hpc_cluster.ssh_command

    if queue == settings.hpc_cluster.datamover.queue_name:
        lsf_host_user = settings.hpc_cluster.datamover.connection.username
        lsf_host = settings.hpc_cluster.datamover.connection.host

    bkill_cmd = bkill_cmd + " " + job_id
    ssh_cmd = ssh_cmd + " " + lsf_host_user + "@" + lsf_host

    cmd = ssh_cmd + " " + bkill_cmd

    try:
        logger.info(" LSF command executing  : " + cmd)
        job_status = subprocess.run(cmd, capture_output=True, shell=True, check=True)
        if job_status.stdout:
            msg_out = job_status.stdout.decode("utf-8")
        if job_status.stderr:
            msg_err = job_status.stderr.decode("utf-8")
        status = True
        message = "Successfully killed LSF jobs on codon cluster"
    except Exception as e:
        message = 'Could not kill jobs from codon ; ' + job_id
        status = False
        logger.error(message + ' ;  reason  :-' + str(e))

    return status, message, str(msg_out), str(msg_err)


def get_permissions(study_id, user_token, obfuscation_code=None):
    """
    Check MTBLS-WS for permissions on this Study for this user

    Study       User    Submitter   Curator     Reviewer/Read-only
    SUBMITTED   ----    Read+Write  Read+Write  Read
    INCURATION  ----    Read        Read+Write  Read
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
        summary="Kill a EBI LSF cluster job (curator only)",
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

        parser = reqparse.RequestParser()
        parser.add_argument('job_id', help="LSF job to terminate", location="args")
        parser.add_argument('queue', help="Queue in which Job running", location="args")
        if request.args:
            args = parser.parse_args()
            lsf_job_id = args['job_id']
            queue = args['queue']

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

        parser = reqparse.RequestParser()
        parser.add_argument('queue', help="queue on which job to be submitted")
        parser.add_argument('job_name', help="job name to be queried")
        if request.args:
            args = parser.parse_args(req=request)
            queue = args['queue']
            job_name = args['job_name']

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

        parser = reqparse.RequestParser()
        parser.add_argument('command', help="Command to run on the LSF cluster")
        parser.add_argument('params', help="Params with command to run on the LSF cluster")
        parser.add_argument('email', help="email to receive output")
        parser.add_argument('queue', help="queue on which job to be submitted")
        if request.args:
            args = parser.parse_args(req=request)
            command = args['command']
            params = args['params']
            email = args['email']
            queue = args['queue']

        if command is None:
            abort(404)

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = get_permissions('MTBLS2', user_token)
        if not is_curator:
            abort(403)

        status, message, job_out, job_err, log_file = submit_job(True, email, queue, command, params, None, True)
        logger.info(" Output file " + log_file)

        if status:
            return {"success": message, "message": job_out, "error": job_err}
        else:
            return {"Failure": message, "message": job_out, "error": job_err}
