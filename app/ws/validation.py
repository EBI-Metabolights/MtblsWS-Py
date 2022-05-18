#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Jan-09
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


import json
import logging
import os
import re
import threading
import traceback

from flask import current_app as app
from flask import request
from flask_restful import abort, Resource, reqparse
from flask_restful_swagger import swagger

from app.ws.db_connection import override_validations
from app.ws.study import commons
from app.ws.study.validation.commons import job_status, is_newer_timestamp, submitJobToCluser, is_newer_files, \
    update_val_schema_files, validate_study

logger = logging.getLogger('wslog')


class Validation(Resource):
    @swagger.operation(
        summary="Validate study",
        notes='''Validating the overall study. 
        This method will validate the study metadata and check the files study folder''',
        parameters=[
            {
                "name": "study_id",
                "description": "Study to validate",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "section",
                "description": "Specify which validations to run, default is all: "
                               "isa-tab, publication, protocols, people, samples, assays, maf, files",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "level",
                "description": "Specify which success-errors levels to report, default is all: "
                               "error, warning, info, success",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "static_validation_file",
                "description":
                    "Read validation and file list from pre-generated files ('In Review' and 'Public' status)."
                    "<b> NOTE that studies with a large number of files will force a static file listing</b>",
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
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

        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None or study_id is None:
            abort(401)

        study_id = study_id.upper()

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = commons.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('section', help="Validation section", location="args")
        parser.add_argument('level', help="Validation message levels", location="args")
        parser.add_argument('static_validation_file', help="Use pre-generated validations", location="args")
        args = parser.parse_args()
        section = args['section']
        log_category = args['level']
        static_validation_file = args['static_validation_file']
        if not static_validation_file:
            static_validation_file = 'true'  # Set to same as input default value
        static_validation_file = True if static_validation_file.lower() == 'true' else False

        log_categories = "error", "warning", "info", "success", "all"
        if log_category is None or log_category not in log_categories:
            log_category = 'all'

        val_sections = "all", "isa-tab", "publication", "protocols", "people", "samples", "assays", "maf", "files"
        if section is None or section not in val_sections:
            section = 'all'

        try:
            number_of_files = sum([len(files) for r, d, files in os.walk(study_location)])
        except:
            number_of_files = 0

        validation_files_limit = app.config.get('VALIDATION_FILES_LIMIT')
        force_static_validation = False

        # We can only use the static validation file when all values are used. MOE uses 'all' as default
        if section != 'all' or log_category != 'all':
            static_validation_file = False

        if section == 'all' or log_category == 'all':
            validation_file = os.path.join(study_location, 'validation_report.json')
            if os.path.isfile(validation_file):
                with open(validation_file, 'r', encoding='utf-8') as f:
                    validation_schema = json.load(f)
                    return validation_schema

        if section == 'all' and log_category == 'all' and number_of_files >= validation_files_limit:
            force_static_validation = True  # ToDo, We need to use static files until pagenation is implemented
            static_validation_file = force_static_validation

        study_status = study_status.lower()

        if (static_validation_file and study_status in ('in review', 'public')) or force_static_validation:

            validation_file = os.path.join(study_location, 'validation_report.json')

            # Some file in the filesystem is newer than the validation reports, so we need to re-generate
            if is_newer_files(study_location):
                return update_val_schema_files(validation_file, study_id, study_location, user_token,
                                               obfuscation_code, log_category=log_category, return_schema=True)

            if os.path.isfile(validation_file):
                try:
                    with open(validation_file, 'r', encoding='utf-8') as f:
                        validation_schema = json.load(f)
                except Exception as e:
                    logger.error(str(e))
                    validation_schema = update_val_schema_files(validation_file, study_id, study_location, user_token,
                                                                obfuscation_code, log_category=log_category,
                                                                return_schema=True)
                    # validation_schema = \
                    #     validate_study(study_id, study_location, user_token, obfuscation_code,
                    #                    validation_section=section,
                    #                    log_category=log_category, static_validation_file=False)
            else:
                validation_schema = update_val_schema_files(validation_file, study_id, study_location, user_token,
                                                            obfuscation_code, log_category=log_category,
                                                            return_schema=True)
                # validation_schema = \
                #     validate_study(study_id, study_location, user_token, obfuscation_code, validation_section=section,
                #                    log_category=log_category, static_validation_file=static_validation_file)

            # if study_status == 'in review':
            #     try:
            #         cmd = "curl --silent --request POST -i -H \\'Accept: application/json\\' -H \\'Content-Type: application/json\\' -H \\'user_token: " + user_token + "\\' '"
            #         cmd = cmd + app.config.get('CHEBI_PIPELINE_URL') + study_id + "/validate-study/update-file'"
            #         logger.info("Starting cluster job for Validation schema update: " + cmd)
            #         status, message, job_out, job_err = lsf_job('bsub', job_param=cmd, send_email=False)
            #         lsf_msg = message + '. ' + job_out + '. ' + job_err
            #         if not status:
            #             logger.error("LSF job error: " + lsf_msg)
            #         else:
            #             logger.info("LSF job submitted: " + lsf_msg)
            #     except Exception as e:
            #         logger.error(str(e))
        else:
            validation_schema = \
                validate_study(study_id, study_location, user_token, obfuscation_code, validation_section=section,
                               log_category=log_category, static_validation_file=static_validation_file)

        return validation_schema


class UpdateValidationFile(Resource):
    @swagger.operation(
        summary="Update validation file",
        notes="Update validation file",
        parameters=[
            {
                "name": "study_id",
                "description": "Study to validate",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
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
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token"
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def post(self, study_id):

        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None or study_id is None:
            abort(401)

        study_id = study_id.upper()

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = commons.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        validation_file = os.path.join(study_location, 'validation_report.json')
        """ Background thread to update the validations file """
        threading.Thread(
            target=update_val_schema_files(validation_file, study_id, study_location, user_token, obfuscation_code),
            daemon=True).start()

        return {"success": "Validation schema file updated"}


class OverrideValidation(Resource):
    @swagger.operation(
        summary="Approve or reject a specific validation rule (curator only)",
        notes='''For EBI curators to manually approve or fail a validation step.</br> "*" will override *all* errors!
        <pre><code>
    { 
      "validations": [
        {
          "publication_3": "The PubChem id is for a different paper",
          "people_3": "The contact has given an incorrect email address",
          "files_1": ""
        } 
      ]
    }
    </code></pre>''',
        parameters=[
            {
                "name": "study_id",
                "description": "Study to override validations",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "validations",
                "description": 'which validation rules to override.',
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
    def post(self, study_id):

        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None or study_id is None:
            abort(401)

        study_id = study_id.upper()

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = commons.get_permissions(study_id, user_token)
        if not is_curator:
            abort(403)

        val_feedback = ""
        override_list = []
        # First, get all existing validations from the database
        try:
            query_list = override_validations(study_id, 'query')
            if query_list:
                for val in query_list[0].split('|'):
                    override_list.append(val)
        except Exception as e:
            logger.error('Could not query existing overridden validations from the database')

        # Get the new validations submitted
        data_dict = json.loads(request.data.decode('utf-8'))
        validation_data = data_dict['validations']

        # only add unique validations to the update statement
        for val, val_message in validation_data[0].items():
            val_found = False
            for existing_val in override_list:
                if val + ":" in existing_val:  # Do we already have this validation rule in the database
                    val_found = True
                    val_feedback = val_feedback + "Validation '" + val + "' was already stored in the database. "

            if not val_found:
                override_list.append(val + ':' + val_message)
                val_feedback = "Validation '" + val + "' stored in the database"

        db_update_string = ""
        for existing_val in override_list:
            db_update_string = db_update_string + existing_val + '|'
        db_update_string = db_update_string[:-1]  # Remove trailing pipeline

        try:
            query_list = override_validations(study_id, 'update', override=db_update_string)
        except Exception as e:
            logger.error('Could not store overridden validations on the database')

        return {"success": val_feedback}


class ValidationComment(Resource):
    @swagger.operation(
        summary="Add Comment",
        notes='''Add a comment to a specific validation to give the user more context''',
        parameters=[
            {
                "name": "study_id",
                "description": "Study to add a validation comment to",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "comments",
                "description": "Which validation details to add comments for.",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": "True"

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
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token"
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def post(self, study_id):

        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None or study_id is None:
            abort(401)

        study_id = study_id.upper()

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = commons.get_permissions(study_id, user_token)
        if not is_curator:
            abort(403)

        feedback = ""
        comment_list = []
        # query_comments is a db_connection.py method
        query_list = query_comments(study_id)
        if query_list:
            for val in query_list[0].split('|'):
                comment_list.append(val)

        # Get the new validations submitted
        data_dict = json.loads(request.data.decode('utf-8'))
        new_comments = data_dict['comments']

        for val_sequence, comment in new_comments.items():
            val_found = False
            for i, existing_comment in enumerate(comment_list):
                if val_sequence + ":" in existing_comment:  # Do we already have this comment in the database
                    comment_list[i] += f' {comment}'
                    feedback += f"Comment for {val_sequence} has been updated."

            if not val_found:
                comment_list.append(val_sequence + ':' + comment)
                feedback += f"Comment for {val_sequence} has been stored in the database."

        db_update_string = f' {"|".join(comment_list)}'

        try:
            __ = update_comments(study_id, db_update_string)
        except Exception as e:
            logger.error(f"Could not store new comments in the database: {str(e)}")
            abort(500, str(e))

        return {"status": feedback}

class NewValidation(Resource):
    @swagger.operation(
        summary="Validate study",
        notes='''Validating the study with given section
        This method will validate the study metadata and check the files study folder''',
        parameters=[
            {
                "name": "study_id",
                "description": "Study to validate",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string"
            },
            {
                "name": "section",
                "description": "Specify which validations to run, default is Metadata: "
                               "all, assays, files",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["all", "assays", "files"]
            },
            {
                "name": "force_run",
                "description": "Run the validation again",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
                "enum": ["True", "False"]
            },
            {
                "name": "level",
                "description": "Specify which success-errors levels to report, default is all: "
                               "error, warning, info, success",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
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
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token"
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self, study_id):

        user_token = None
        # User authentication
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None or study_id is None:
            abort(401)

        study_id = study_id.upper()

        # param validation
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = commons.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('section', help="Validation section", location="args")
        parser.add_argument('level', help="Validation message levels", location="args")
        parser.add_argument('force_run', help="Validation message levels", location="args")
        args = parser.parse_args()
        section = args['section']
        force_run = args['force_run']
        if section is None or section == "":
            section = 'meta'
        if force_run is None:
            force_run = False
        if section:
            query = section.strip()
        log_category = args['level']

        log_categories = "error", "warning", "info", "success", "all"
        if log_category is None or log_category not in log_categories:
            log_category = 'all'

        val_sections = "all", "isa-tab", "publication", "protocols", "people", "samples", "assays", "maf", "files"

        script = app.config.get('VALIDATION_SCRIPT')
        para = ' -l {level} -i {study_id} -u {token} -s {section}'.format(level=log_category, study_id=study_id,
                                                                          token=user_token, section=section)
        file_name = None
        logger.info("Validation params are - " + str(log_category) + " " + str(section))
        pattern = re.compile(".validation_" + section + "\S+.json")

        for filepath in os.listdir(study_location):
            if pattern.match(filepath):
                file_name = filepath
                break

        if file_name:
            result = file_name[:-5].split('_')
            sub_job_id = result[2]
            # bacct -l 3861194
            # check job status
            status = job_status(sub_job_id)
            logger.info("job status " + sub_job_id + " " + status)
            if status == "PEND" or status == "RUN":
                return {
                    "message": "Validation is already in progress. Job " + sub_job_id + " is in running or pending state"}

            file_name = study_location + "/" + file_name
            if os.path.isfile(file_name) and status == "DONE":
                if not force_run:
                    try:
                        with open(file_name, 'r', encoding='utf-8') as f:
                            validation_schema = json.load(f)
                            return validation_schema
                    except Exception as e:
                        logger.error(str(e))
                        return {"message": "Error in reading the Validation"}
                else:
                    if is_newer_timestamp(study_location, file_name):
                        os.remove(file_name)
                        command = script + ' ' + para
                        return submitJobToCluser(command, section, study_location)
                    else:
                        try:
                            with open(file_name, 'r', encoding='utf-8') as f:
                                validation_schema = json.load(f)
                                return validation_schema
                        except Exception as e:
                            logger.error(str(e))
                            return {"message": "Error in reading the Validation file"}

            elif os.path.isfile(file_name) and os.path.getsize(file_name) > 0:
                if is_newer_timestamp(study_location, file_name):
                    logger.info(" job status is not present, creating new job")
                    os.remove(file_name)
                    command = script + ' ' + para
                    return submitJobToCluser(command, section, study_location)
                else:
                    try:
                        logger.info(" job status is not present and no update, returning validation")
                        with open(file_name, 'r', encoding='utf-8') as f:
                            validation_schema = json.load(f)
                            return validation_schema
                    except Exception as e:
                        logger.error(str(e))
                        return {"message": "Error in reading the Validation"}
        else:
            try:
                os.remove(file_name)
            except Exception as e:
                pass
                # submit a new job return job id
                logger.info(" no file present , creating new job")
                command = script + ' ' + para
                return submitJobToCluser(command, section, study_location)
