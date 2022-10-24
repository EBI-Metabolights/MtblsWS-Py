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
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from flask import request, abort
from flask import current_app as app
from datetime import datetime
from app.ws.study import commons

logger = logging.getLogger('wslog')


# Deprecated use submit job instead
def lsf_job(job_cmd, job_param=None, send_email=True, user_email=""):
    no_jobs = "rm", "mv", "cat", "more", "ln", "ls", "mount", "kill", "who", "hostname", "ifconfig"
    status = True
    job_status = ""
    msg_out = "No LSF job output"
    msg_err = "No LSF job error"
    email = app.config.get('LSF_COMMAND_EMAIL') + "," + user_email
    message = "Successfully submitted LSF cluster job: '" + job_cmd
    if job_param:
        message = message + " " + job_param
    message = message + "'. Please see LSF cluster email sent to " + email

    for no_cmd in no_jobs:
        if job_param.startswith(no_cmd):
            abort(403, 'Nope, you cannot do that! ' + job_param)

    cmd = os.path.join(app.config.get('LSF_COMMAND_PATH'), job_cmd)
    if send_email:
        cmd = cmd + " -u " + email + " " + job_param
    else:
        cmd = cmd + " " + job_param
        logger.info('LSF job triggered with no email: ' + cmd)
    try:
        job_status = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True, check=True)
        if job_status.stdout:
            msg_out = job_status.stdout.decode("utf-8")
        if job_status.stderr:
            msg_err = job_status.stderr.decode("utf-8")
    except Exception as e:
        status = False
        message = 'Could not execute or list LSF jobs, try to Log into the EBI cluster and run "bjobs" to see all ' \
                  'running jobs '
        if e.returncode == 255:
            msg_err = "LSF Job " + job_param + " was not found. Check if the process still exists"
        logger.error(message + '. ' + str(e))

    return status, message, str(msg_out), str(msg_err)


def submit_job(email=False, account=None, queue=None, job_cmd=None, job_params=None, submitter=None, log=False, log_path=None):
    msg_out = "No LSF job output"
    msg_err = "No LSF job error"
    bsub_cmd = app.config.get('JOB_SUBMIT_COMMAND')
    lsf_host = app.config.get('LSF_HOST')
    lsf_host_user = app.config.get('LSF_HOST_USER')
    ssh_cmd = app.config.get('LSF_HOST_SSH_CMD')
    log_file = None
    job_cmd1 = job_cmd

    if email:
        if account is None:
            account = app.config.get('JOB_TRACK_EMAIL')

        bsub_cmd = bsub_cmd + " -u " + account

    if job_cmd is None:
        return False, "JOB command should not be empty!", str(msg_out), str(msg_err)

    if queue is None:
        queue = app.config.get('LSF_BSUB_DEFAULT_Q')

    if queue == app.config.get('LSF_DATAMOVER_Q'):
        lsf_host_user = app.config.get('LSF_DATAMOVER_USER')

    bsub_cmd = bsub_cmd + " -q " + queue
    ssh_cmd = ssh_cmd + " " + lsf_host_user + "@" + lsf_host

    if job_params:
        job_cmd1 = job_cmd1 + " " + job_params

    if submitter is None:
        submitter = "None"

    if log:
        if log_path is None:
            log_file_location = app.config.get('JOB_TRACK_LOG_LOCATION')
            now = datetime.now()
            date_time = now.strftime("%d-%m-%Y_%H-%M-%S")
            log_file = log_file_location + "/" + submitter + "_" + job_cmd + "_" + date_time + ".log"
        else:
            log_file = log_path + "/" + submitter + "_" + job_cmd + ".log"
        bsub_cmd = bsub_cmd + " -o " + log_file

    cmd = ssh_cmd + " " + bsub_cmd + " " + job_cmd1

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

    return status, message, str(msg_out), str(msg_err), log_file


def list_jobs(queue=None):
    msg_out = "No LSF job output"
    msg_err = "No LSF job error"
    bjobs_cmd = app.config.get('JOB_RUNNING_COMMAND')
    lsf_host = app.config.get('LSF_HOST')
    lsf_host_user = app.config.get('LSF_HOST_USER')
    ssh_cmd = app.config.get('LSF_HOST_SSH_CMD')

    if queue is None:
        queue = app.config.get('LSF_BSUB_DEFAULT_Q')

    if queue == app.config.get('LSF_DATAMOVER_Q'):
        lsf_host_user = app.config.get('LSF_DATAMOVER_USER')

    bjobs_cmd = bjobs_cmd + " -q " + queue
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
    msg_out = "No LSF job output"
    msg_err = "No LSF job error"
    bkill_cmd = app.config.get('JOB_KILL_COMMAND')
    lsf_host = app.config.get('LSF_HOST')
    lsf_host_user = app.config.get('LSF_HOST_USER')
    ssh_cmd = app.config.get('LSF_HOST_SSH_CMD')

    if queue == app.config.get('LSF_DATAMOVER_Q'):
        lsf_host_user = app.config.get('LSF_DATAMOVER_USER')

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
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = commons.get_permissions('MTBLS2', user_token)
        if not is_curator:
            abort(403)

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
        study_status = commons.get_permissions('MTBLS2', user_token)
        if not is_curator:
            abort(403)

        parser = reqparse.RequestParser()
        parser.add_argument('queue', help="queue on which job to be submitted")
        if request.args:
            args = parser.parse_args(req=request)
            queue = args['queue']

        status, message, job_out, job_err = list_jobs(queue)

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
        study_status = commons.get_permissions('MTBLS2', user_token)
        if not is_curator:
            abort(403)

        status, message, job_out, job_err, log_file = submit_job(True, email, queue, command, params, None, True)
        logger.info(" Output file " + log_file)

        if status:
            return {"success": message, "message": job_out, "error": job_err}
        else:
            return {"Failure": message, "message": job_out, "error": job_err}
