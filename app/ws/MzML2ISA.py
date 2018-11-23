import json
from flask import request, abort, send_file
from flask.json import jsonify
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import *
from app.ws.isaApiClient import IsaApiClient
from mzml2isa.parsing import convert

logger = logging.getLogger('wslog')
wsc = WsClient()
iac = IsaApiClient()


class Convert2ISAtab(Resource):
    @swagger.operation(
        summary="Convert mzML files into ISA-Tab",
        notes='''</P><B>Be aware that any ISA-Tab files will be overwritten for this study</P>
        This process can run for a long while, please be patient</B>''',
        parameters=[
            {
                "name": "study_id",
                "description": "Existing Study Identifier for generating new ISA-Tab files",
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
        read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        input_folder = study_location
        outout_folder = study_location

        logger.info('Creating a new study upload folder for study %s', study_id)
        status = convert_to_isa(study_id, input_folder, outout_folder)
        return status


def convert_to_isa(study_id, input_folder, outout_folder):
    status = convert(input_folder, outout_folder, study_id)
    return status