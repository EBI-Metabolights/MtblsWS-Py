import json
import logging
import os
import threading

from flask_restful import Resource, abort
from flask_restful_swagger import swagger
from flask import current_app as app, request, abort

from app.ws.misc_utilities.request_parsers import RequestParsers
from app.ws.services.validation_service import ValidationService
from app.ws.validation import update_val_schema_files
from app.ws.validation_dir.validations_utils import ValidationUtils, PermissionsObj

logger = logging.getLogger('wslog')


class StudyValidation(Resource):
    """
    This is the primary resource for study validation. Each study will have to be fed into this resource. The exception
    being if the study is too large and would take too much processing time as a result. In this instance the cron job
    resource should be used instead.

    The contents of this method are subject to an ongoing refactor. Once the rewrite has been completed a more
    substantial description of the inner workings of the method should be completed to make the lives of all future
    developers easier.
    """
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
        """
        Return the validations for a given study. Either returns the existing validation report, or if one is not found
        the study is validated from scratch.

        :param study_id: Study accession number.
        :return: validation schema json.
        """

        # instantiate permissions object ( which retrieves all permissions on initialisation )
        perms = PermissionsObj(study_id=study_id, req_headers=request.headers)

        if not perms.write_access:
            abort(403)

        # query validation
        parser = RequestParsers.study_validation_parser()
        args = parser.parse_args()

        # Instantiate validation parameters object
        validation_parameters = ValidationUtils\
            .get_study_validation_parameters(args=args, study_location=perms.study_location)

        validation_service = ValidationService()

        return validation_service.choose_validation_pathway(perms=perms, validation_parameters=validation_parameters)


class ValidationFile(Resource):
    """
    Resource for operations on the validation file.
    """

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
        """
        Trigger a thread to update the validation file. Done in the background as it can take along time for larger
        files.

        :param study_id: Study accession number.
        :return: a message indicating the thread has been initialised.
        """

        # instantiate permissions object ( which retrieves all permissions on initialisation )
        perms = PermissionsObj(study_id=study_id, req_headers=request.headers)

        if not perms.write_access:
            abort(403)

        validation_file = os.path.join(perms.study_location, 'validation_report.json')
        """ Background thread to update the validations file """
        threading.Thread(
            target=update_val_schema_files(
                validation_file, perms.study_id, perms.study_location, perms.user_token, perms.obfuscation_code),
            daemon=True).start()

        return {"success": "Validation schema file is updating in the background. Consult logs if the update is not"
                           " viewable."}


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

        # instantiate permissions object ( which retrieves all permissions on initialisation )
        perms = PermissionsObj(study_id=study_id, req_headers=request.headers)

        if not perms.is_curator:
            abort(403)

        validation_service = ValidationService()

        return validation_service.override(
            study_id=study_id, validation_data=json.loads(request.data.decode('utf-8'))['validations']
        )


class ClusterStudyValidation(Resource):
    @swagger.operation(
        summary="Validate study via Cluster ",
        notes='''Validating the study with given section
        This method will validate the study metadata and check the files study folder. Note that this passes the 
        validation off to a separate process.''',
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

        # instantiate permissions object ( which retrieves all permissions on initialisation )
        perms = PermissionsObj(study_id=study_id, req_headers=request.headers)

        if not perms.write_access:
            abort(403)

        # query validation
        parser = RequestParsers.cluster_validation_parser()
        args = parser.parse_args()
        val_params = ValidationUtils.get_cluster_validation_params(args)

        validation_service = ValidationService()

        return validation_service.cluster(
            val_params=val_params, perms=perms, script=app.config.get('VALIDATION_SCRIPT')
        )

