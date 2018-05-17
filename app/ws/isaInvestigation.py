from flask import request
from flask_restful import Resource, abort, reqparse
from marshmallow import ValidationError
from app.ws.mm_models import *
from app.ws.models import *
from flask_restful_swagger import swagger
import logging.config


class IsaInvestigation(Resource):

    @swagger.operation(
        summary="Get ISA Investigation",
        notes="Get the whole ISA Investigation in a single JSON.",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier",
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
                "required": False,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax."
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
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # User authentication
        user_token = None
        if 'user_token' in request.headers:
            user_token = request.headers['user_token']
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help='Study Sample name')
        obj_name = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name']

        logger.info('Getting Investigation %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        # Using context to avoid envelop tags in contained objects
        sch = IsaInvestigationSchema()
        sch.context['investigation'] = Investigation()
        logger.info('Got %s', isa_inv)
        return sch.dump(isa_inv)

    @swagger.operation(
        summary="Update Study",
        notes="Update Study.",
        parameters=[
            {
                "name": "study",
                "description": "Study in ISA-JSON format",
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
            },
            {
                "name": "save_audit_copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False
            }
        ],
        responseMessages=[
            {
                "code": 201,
                "message": "Created."
            },
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax."
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user."
            }
        ]
    )
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            # user token is required
            abort(401)

        # check for keeping copies
        save_audit_copy = False
        save_msg_str = "NOT be"
        if "save_audit_copy" in request.headers and \
                request.headers["save_audit_copy"].lower() == 'true':
            save_audit_copy = True
            save_msg_str = "be"

        # body content validation
        updated_inv = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['investigation']
            # if partial=True missing fields will be ignored
            result = IsaInvestigationSchema().load(data, partial=True)
            updated_inv = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # update Study details
        logger.info('Updating Study Publication details for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        # isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        isa_inv = updated_inv

        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path,
                            save_investigation_copy=save_audit_copy,
                            save_samples_copy=save_audit_copy, save_assays_copy=save_audit_copy)
        logger.info('Updated %s', updated_inv.title)

        return IsaInvestigationSchema().dump(updated_inv)
