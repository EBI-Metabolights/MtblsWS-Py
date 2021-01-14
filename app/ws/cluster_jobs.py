#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Oct-23
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

import subprocess
import logging
import os
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from flask import request, abort
from app.ws.mtblsWSclient import WsClient
from flask import current_app as app

logger = logging.getLogger('wslog')
wsc = WsClient()


def lsf_job(job_cmd, job_param=None, send_email=True):
    no_jobs = "rm", "mv", "cat", "more", "ln", "ls", "mount", "kill", "who", "hostname", "ifconfig"
    status = True
    job_status = ""
    msg_out = "No LSF job output"
    msg_err = "No LSF job error"
    email = app.config.get('LSF_COMMAND_EMAIL')
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


class LsfUtils(Resource):
    @swagger.operation(
        summary="Kill a EBI LSF cluster job (curator only)",
        parameters=[
            {
                "name": "lsf_job_id",
                "description": "running LSF job id",
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
        parser.add_argument('lsf_job_id', help="LSF job to terminate", location="args")
        if request.args:
            args = parser.parse_args()
            lsf_job_id = args['lsf_job_id']

        if lsf_job_id is None:
            abort(404)

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions('MTBLS2', user_token)
        if not is_curator:
            abort(403)

        status, message, job_out, job_err = lsf_job('bkill', job_param=lsf_job_id)

        if status:
            return {"success": message, "message": job_out, "error": job_err}
        else:
            return {"error": message, "message": job_out, "error": job_err}

    @swagger.operation(
        summary="List all EBI LSF cluster jobs (curator only)",
        parameters=[
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
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None:
            abort(401)

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions('MTBLS2', user_token)
        if not is_curator:
            abort(403)

        status, message, job_out, job_err = lsf_job('bjobs', job_param="")

        if status:
            return {"success": message, "message": job_out, "error": job_err}
        else:
            return {"error": message, "message": job_out, "error": job_err}

    @swagger.operation(
        summary="Submit a new EBI LSF cluster job (curator only)",
        parameters=[
            {
                "name": "cluster_job",
                "description": "command to run on the LSF cluster",
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
        cluster_job = None

        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None:
            abort(401)

        parser = reqparse.RequestParser()
        parser.add_argument('cluster_job', help="Command to run on the LSF cluster")
        if request.args:
            args = parser.parse_args(req=request)
            cluster_job = args['cluster_job']

        if cluster_job is None:
            abort(404)

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions('MTBLS2', user_token)
        if not is_curator:
            abort(403)

        status, message, job_out, job_err = lsf_job('bsub', job_param=cluster_job)

        if status:
            return {"success": message, "message": job_out, "error": job_err}
        else:
            return {"Failure": message, "message": job_out, "error": job_err}

class LsfUtilsStatus(Resource):


    @swagger.operation(
        summary="List all EBI LSF cluster jobs",
        parameters=[
            {
                "name": "job_id",
                "description": "submitted job id",
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
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None:
            abort(401)
            # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('job_id', help='cluster job_id')
        job_id = ""
        if request.args:
            args = parser.parse_args(req=request)
            job_id = args['job_id']

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions('MTBLS2', user_token)
        if not is_curator:
            abort(403)

        cmd = "/usr/bin/ssh ebi-cli bjobs " + str(job_id).strip()
        logger.info(cmd)
        result = subprocess.run(cmd, stdout=subprocess.PIPE, shell=True, check=True)
        logger.info(result)
        result = result.stdout.decode("utf-8")
        return {'Success': result}
