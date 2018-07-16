from flask import request, jsonify
from flask_restful import Resource, abort, marshal_with, reqparse
from marshmallow import ValidationError
from app.ws import utils
from app.ws.isaApiClient import IsaApiClient
from app.ws.mm_models import *
from app.ws.mtblsWSclient import WsClient
from app.ws.models import *
from flask_restful_swagger import swagger
from flask import current_app as app
import logging
import time


logger = logging.getLogger('wslog')
iac = IsaApiClient()
wsc = WsClient()


# Allow for a more detailed logging when on DEBUG mode
def log_request(request_obj):
    if app.config.get('DEBUG'):
        if app.config.get('DEBUG_LOG_HEADERS'):
            logger.debug('REQUEST HEADERS -> %s', request_obj.headers)
        if app.config.get('DEBUG_LOG_BODY'):
            logger.debug('REQUEST BODY    -> %s', request_obj.data)
        if app.config.get('DEBUG_LOG_JSON'):
            try:
                logger.debug('REQUEST JSON    -> %s', request_obj.json)
            except:
                logger.debug('REQUEST JSON    -> EMPTY')


class IsaJsonStudy(Resource):
    @swagger.operation(
        summary="Get Study",
        nickname="Get Study",
        notes="Get Study.",
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
    @marshal_with(Investigation_api_model, envelope='investigation')
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info('Getting ISA-JSON Study %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_obj = iac.get_isa_json(study_id, user_token)
        str_inv = json.dumps({'investigation': isa_obj}, default=serialize_investigation, sort_keys=True)
        logger.info('... found Study: %s %s', isa_obj.get('title'), isa_obj.get('identifier'))
        return isa_obj


class StudyTitle(Resource):

    @swagger.operation(
        summary="Get Study Title",
        notes="Get Study title.",
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
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info('Getting Study title for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
        title = isa_study.title
        logger.info('Got %s', title)
        return jsonify({"title": title})

    @swagger.operation(
        summary='Update Study Title',
        notes="Update the title of a Study.",
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "title",
                "description": "New title",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        # body content validation
        if request.data is None or request.json is None:
            abort(400)
        # data_dict = request.get_json(force=True)
        data_dict = json.loads(request.data.decode('utf-8'))
        new_title = data_dict['title']
        # check for keeping copies
        save_audit_copy = False
        save_msg_str = "NOT be"
        if "save_audit_copy" in request.headers and request.headers["save_audit_copy"].lower() == 'true':
            save_audit_copy = True
            save_msg_str = "be"

        # update study title
        logger.info('Updating Study title for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
        isa_study.title = new_title
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Applied %s', new_title)
        return jsonify({"title": new_title})


class StudyDescription(Resource):

    @swagger.operation(
        summary="Get Study Description",
        notes="Get the description of a Study.",
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
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info('Getting Study description for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
        description = isa_study.description
        logger.info('Got %s', description)
        return jsonify({"description": description})

    @swagger.operation(
        summary='Update Study Description',
        notes="Update the description of a Study.",
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "description",
                "description": "New description",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.debug('Request headers   : %s', request.headers)
        logger.debug('Request data      : %s', request.data)
        logger.debug('Request data-utf8 : %s', request.data.decode('utf-8'))

        # body content validation
        if request.data is None or request.json is None:
            abort(400)
        data_dict = json.loads(request.data.decode('utf-8'))
        new_description = data_dict['description']
        # check for keeping copies
        save_audit_copy = False
        save_msg_str = "NOT be"
        if "save_audit_copy" in request.headers and request.headers["save_audit_copy"].lower() == 'true':
            save_audit_copy = True
            save_msg_str = "be"

        # update study description
        logger.info('Updating Study description for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
        isa_study.description = new_description
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Applied %s', new_description)
        return jsonify({"description": new_description})


class StudyContacts(Resource):

    @swagger.operation(
        summary='Add new Study Contact',
        notes='Add new Contact to a Study',
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "contact",
                "description": 'details for contact in ISA-JSON format.',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
            },
            {
                "code": 409,
                "message": "Conflict. The request could not be completed due to a conflict"
                           " with the current state of study. This is usually issued to prevent duplications."
            }
        ]
    )
    def post(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('email', help="Contact's email")
        args = parser.parse_args()
        email = args['email']
        # No email param allowed, just to prevent confusion with UPDATE
        if email:
            abort(400)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            abort(401)

        # check for keeping copies
        save_audit_copy = False
        save_msg_str = "NOT be"
        if "save_audit_copy" in request.headers and \
                request.headers["save_audit_copy"].lower() == 'true':
            save_audit_copy = True
            save_msg_str = "be"

        # body content validation
        new_contact = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['contact']
            # if partial=True missing fields will be ignored
            result = PersonSchema().load(data, partial=False)
            new_contact = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # Add new contact
        logger.info('Adding new Contact %s for %s, using API-Key %s', new_contact.email, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
        # check for contact added already
        for index, person in enumerate(isa_study.contacts):
            if person.email == new_contact.email:
                abort(409)

        # check for number of comments
        # this is a workaround for ISA-API not handling properly comments in Person()
        # causing the investigation tab file being saved empty or corrupt
        self.check_number_comments(isa_study.contacts[0], new_contact)

        # add contact
        isa_study.contacts.append(new_contact)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Added %s', new_contact.email)

        return PersonSchema().dump(new_contact)

    def check_number_comments(self, contactA, contactB):
        # Number of comments in all contacts MUSt be the same
        if len(contactA.comments) != len(contactB.comments):
            # returning a malformed request error
            logger.error("Malformed query: All contacts must have same number of comments.")
            abort(400)

    @swagger.operation(
        summary="Get Study Contacts",
        notes="""Get Contacts associated with a Study.
              <br>
              Use contact's email as a query parameter to filter out.""",
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
                "name": "email",
                "description": "Contact's email",
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
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        # ToDo add more filters: lastName, firstName,...
        parser = reqparse.RequestParser()
        parser.add_argument('email', help="Contact's email")
        email = None
        if request.args:
            args = parser.parse_args(req=request)
            email = args['email']

        logger.info('Getting Contacts %s for Study %s, using API-Key %s', email, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)

        obj_list = isa_study.contacts
        # Using context to avoid envelop tags in contained objects
        sch = PersonSchema()
        sch.context['contact'] = Person()
        if email is None:
            # return a list of objs
            logger.info('Got %s contacts', len(obj_list))
            return sch.dump(obj_list, many=True)
        else:
            # return a single obj
            found = False
            for index, obj in enumerate(obj_list):
                if obj.email == email:
                    found = True
                    break
            if not found:
                abort(404)
            logger.info('Got %s', obj.email)
            return sch.dump(obj)

    @swagger.operation(
        summary='Update Study Contact',
        notes="""Update Contact associated with a Study.
              <br>
              Use contact's email as a query parameter to filter out.""",
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
                "name": "email",
                "description": "Contact's email",
                "required": True,
                "allowEmptyValue": False,
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
            },
            {
                "name": "contact",
                "description": 'details for contact in ISA-JSON format.',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('email', help="Contact's email")
        args = parser.parse_args()
        email = args['email']
        if email is None:
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
        updated_contact = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['contact']
            # if partial=True missing fields will be ignored
            result = PersonSchema().load(data, partial=False)
            updated_contact = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # update contact details
        logger.info('Updating Contact details for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)

        # check for number of comments
        # this is a workaround for ISA-API not handling properly comments in Person()
        # causing the investigation tab file being saved empty or corrupt
        self.check_number_comments(isa_study.contacts[0], updated_contact)

        person_found = False
        for index, person in enumerate(isa_study.contacts):
            if person.email == email:
                person_found = True
                # update person details
                isa_study.contacts[index] = updated_contact
                break
        if not person_found:
            abort(404)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Updated %s', updated_contact.email)

        return PersonSchema().dump(updated_contact)

    @swagger.operation(
        summary='Delete Study Contact',
        notes="""Delete Contact associated with a Study.
              <br>
              Use contact's email as a query parameter to filter out.""",
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
                "name": "email",
                "description": "Contact's email",
                "required": True,
                "allowEmptyValue": False,
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
    def delete(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('email', help="Contact's email", location="args")
        args = parser.parse_args()
        email = args['email']
        if email is None:
            abort(404)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            abort(401)

        # check for keeping copies
        save_audit_copy = False
        save_msg_str = "NOT be"
        if "save_audit_copy" in request.headers and \
                request.headers["save_audit_copy"].lower() == 'true':
            save_audit_copy = True
            save_msg_str = "be"

        # delete contact
        logger.info('Deleting contact %s for %s, using API-Key %s', email, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
        person_found = False
        for index, person in enumerate(isa_study.contacts):
            if person.email == email:
                person_found = True
                # delete contact
                del isa_study.contacts[index]
                break
        if not person_found:
            abort(404)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Deleted %s', person.email)

        return PersonSchema().dump(person)


class StudyProtocols(Resource):

    @swagger.operation(
        summary='Add new Study Protocol',
        notes='Add new Protocol to a Study.',
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "protocol",
                "description": 'Protocol in ISA-JSON format.',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
            },
            {
                "code": 409,
                "message": "Conflict. The request could not be completed due to a conflict"
                           " with the current state of study. This is usually issued to prevent duplications."
            }
        ]
    )
    def post(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help="Protocol name")
        args = parser.parse_args()
        obj_name = args['name']
        # No protocol param allowed, just to prevent confusion with UPDATE
        if obj_name:
            abort(400)
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
        new_obj = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['protocol']
            # if partial=True missing fields will be ignored
            result = ProtocolSchema().load(data, partial=False)
            new_obj = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # Add new protocol
        logger.info('Adding new Protocol %s for %s, using API-Key %s', new_obj.name, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)

        # check for protocol added already
        obj = isa_study.get_prot(obj_name)
        if obj:
            abort(409)
        # add obj
        isa_study.protocols.append(new_obj)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Added %s', new_obj.name)

        return ProtocolSchema().dump(new_obj)

    @swagger.operation(
        summary="Get Study Protocols",
        notes="""Get Study protocols.
              <br>
              Use protocol name as a query parameter to filter out.""",
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
                "name": "name",
                "description": "Protocol name",
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
        parser.add_argument('name', help='Protocol name')
        obj_name = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name']

        logger.info('Getting Study protocols for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)

        obj_list = isa_study.protocols
        # Using context to avoid envelop tags in contained objects
        sch = ProtocolSchema()
        sch.context['protocol'] = Protocol()
        if obj_name is None:
            # return a list of objs
            logger.info('Got %s protocols', len(obj_list))
            return sch.dump(obj_list, many=True)
        else:
            # return a single obj
            found = False
            for index, obj in enumerate(obj_list):
                if obj.name == obj_name:
                    found = True
                    break
            if not found:
                abort(404)
            logger.info('Got %s', obj.name)
            return sch.dump(obj)

    @swagger.operation(
        summary='Delete Study Protocol',
        notes="""Delete Study protocol.
              <br>
              Use protocol name as a query parameter to filter out.""",
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
                "name": "name",
                "description": "Protocol name",
                "required": True,
                "allowEmptyValue": False,
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
    def delete(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help="Protocol name", location="args")
        args = parser.parse_args()
        obj_name = args['name']
        if obj_name is None:
            abort(404)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            abort(401)

        # check for keeping copies
        save_audit_copy = False
        save_msg_str = "NOT be"
        if "save_audit_copy" in request.headers and \
                request.headers["save_audit_copy"].lower() == 'true':
            save_audit_copy = True
            save_msg_str = "be"

        # delete protocol
        logger.info('Deleting protocol %s for %s, using API-Key %s', obj_name, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)

        obj = isa_study.get_prot(obj_name)
        if not obj:
            abort(404)
        # remove object
        isa_study.protocols.remove(obj)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Deleted %s', obj.name)

        return ProtocolSchema().dump(obj)

    @swagger.operation(
        summary='Update Study Protocol',
        notes="""Update Study Protocol.
              <br>
              Use protocol name as a query parameter to filter out.""",
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
                "name": "name",
                "description": "Protocol name",
                "required": True,
                "allowEmptyValue": False,
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
            },
            {
                "name": "protocol",
                "description": 'Protocol in ISA-JSON format.',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help="Protocol name")
        args = parser.parse_args()
        name = args['name']
        if name is None:
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
        updated_protocol = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['protocol']
            # if partial=True missing fields will be ignored
            result = ProtocolSchema().load(data, partial=False)
            updated_protocol = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # update protocol details
        logger.info('Updating Protocol details for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
        found = False
        for index, protocol in enumerate(isa_study.protocols):
            if protocol.name == name:
                found = True
                # update protocol details
                isa_study.protocols[index] = updated_protocol
                break
        if not found:
            abort(404)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Updated %s', updated_protocol.name)

        return ProtocolSchema().dump(updated_protocol)


class StudyFactors(Resource):

    @swagger.operation(
        summary='Add new Study Factor',
        notes='Add new Factor to a Study.',
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "protocol",
                "description": 'Study Factor in ISA-JSON format.',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
            },
            {
                "code": 409,
                "message": "Conflict. The request could not be completed due to a conflict"
                           " with the current state of study. This is usually issued to prevent duplications."
            }
        ]
    )
    def post(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help="Study Factor name")
        args = parser.parse_args()
        obj_name = args['name']
        # No params allowed, just to prevent confusion with UPDATE
        if obj_name:
            abort(400)
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
        new_obj = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['factor']
            # if partial=True missing fields will be ignored
            result = StudyFactorSchema().load(data, partial=False)
            new_obj = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # Add new Study Factor
        logger.info('Adding new Study Factor %s for %s, using API-Key %s', new_obj.name, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)

        # check for factor added already
        obj = isa_study.get_factor(obj_name)
        if obj:
            abort(409)
        # add obj
        isa_study.factors.append(new_obj)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Added %s', new_obj.name)

        return StudyFactorSchema().dump(new_obj)

    @swagger.operation(
        summary="Get Study Factors",
        notes="""Get Study Factors.
              <br>
              Use factor name as a query parameter to filter out.""",
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
                "name": "name",
                "description": "Factor name",
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
        parser.add_argument('name', help='Factor name')
        obj_name = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name']

        logger.info('Getting Study Factors for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)

        obj_list = isa_study.factors
        # Using context to avoid envelop tags in contained objects
        sch = StudyFactorSchema()
        sch.context['factor'] = StudyFactor()
        if obj_name is None:
            # return a list of objs
            logger.info('Got %s factors', len(obj_list))
            return sch.dump(obj_list, many=True)
        else:
            # return a single obj
            found = False
            for index, obj in enumerate(obj_list):
                if obj.name == obj_name:
                    found = True
                    break
            if not found:
                abort(404)
            logger.info('Got %s', obj.name)
            return sch.dump(obj)

    @swagger.operation(
        summary='Delete Study Factor',
        notes="""Delete Study Factor.
              <br>
              Use factor name as a query parameter to filter out.""",
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
                "name": "name",
                "description": "Factor name",
                "required": True,
                "allowEmptyValue": False,
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
    def delete(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help="Factor name", location="args")
        args = parser.parse_args()
        obj_name = args['name']
        if obj_name is None:
            abort(404)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            abort(401)

        # check for keeping copies
        save_audit_copy = False
        save_msg_str = "NOT be"
        if "save_audit_copy" in request.headers and \
                request.headers["save_audit_copy"].lower() == 'true':
            save_audit_copy = True
            save_msg_str = "be"

        # delete Study Factor
        logger.info('Deleting Study Factor %s for %s, using API-Key %s', obj_name, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)

        obj = isa_study.get_factor(obj_name)
        if not obj:
            abort(404)
        # remove object
        isa_study.factors.remove(obj)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Deleted %s', obj.name)

        return StudyFactorSchema().dump(obj)

    @swagger.operation(
        summary='Update Study Factor',
        notes="""Update Study Factor.
              <br>
              Use factor name as a query parameter to filter out.""",
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
                "name": "name",
                "description": "Factor name",
                "required": True,
                "allowEmptyValue": False,
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
            },
            {
                "name": "protocol",
                "description": 'Factor in ISA-JSON format.',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help="Factor name")
        args = parser.parse_args()
        factor_name = args['name']
        if factor_name is None:
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
        updated_factor = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['factor']
            # if partial=True missing fields will be ignored
            result = StudyFactorSchema().load(data, partial=False)
            updated_factor = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # update Study Factor details
        logger.info('Updating Study Factor details for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
        found = False
        for index, factor in enumerate(isa_study.factors):
            if factor.name == factor_name:
                found = True
                # update protocol details
                isa_study.factors[index] = updated_factor
                break
        if not found:
            abort(404)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Updated %s', updated_factor.name)

        return StudyFactorSchema().dump(updated_factor)


class StudyDescriptors(Resource):

    @swagger.operation(
        summary='Add new Study Design Descriptor',
        notes='Add new Design Descriptor to a Study.',
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "studyDesignDescriptor",
                "description": 'Study Design Descriptor in ISA-JSON format.',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
            },
            {
                "code": 409,
                "message": "Conflict. The request could not be completed due to a conflict"
                           " with the current state of study. This is usually issued to prevent duplications."
            }
        ]
    )
    def post(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('term', help="Study Design Descriptor annotation value")
        args = parser.parse_args()
        obj_term = args['term']
        # No params allowed, just to prevent confusion with UPDATE
        if obj_term:
            abort(400)
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
        new_obj = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['studyDesignDescriptor']
            # if partial=True missing fields will be ignored
            result = StudyDesignDescriptorSchema().load(data, partial=False)
            new_obj = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # Add new Study Descriptor
        logger.info('Adding new Study Design Descriptor %s for %s, using API-Key %s',
                    new_obj.term, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)

        # check for Study Descriptor added already
        for index, obj in enumerate(isa_study.design_descriptors):
            if obj.term == new_obj.term:
                abort(409)
        # add Study Descriptor
        isa_study.design_descriptors.append(new_obj)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Added %s', new_obj.term)

        return StudyDesignDescriptorSchema().dump(new_obj)

    @swagger.operation(
        summary="Get Study Design Descriptors",
        notes="""Get Study Design Descriptors.
              <br>
              Use descriptor annotation value as a query parameter to filter out.""",
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
                "name": "annotationValue",
                "description": "Design Descriptor annotation value",
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
        parser.add_argument('annotationValue', help='Design Descriptor value')
        obj_term = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_term = args['annotationValue']

        logger.info('Getting Study Design Descriptors for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)

        obj_list = isa_study.design_descriptors
        # Using context to avoid envelop tags in contained objects
        sch = StudyDesignDescriptorSchema()
        sch.context['descriptor'] = StudyDescriptors()
        if obj_term is None:
            # return a list of objs
            logger.info('Got %s descriptors', len(obj_list))
            return sch.dump(obj_list, many=True)
        else:
            # return a single obj
            found = False
            for index, obj in enumerate(obj_list):
                if obj.term == obj_term:
                    found = True
                    break
            if not found:
                abort(404)
            logger.info('Got %s', obj.term)
            return sch.dump(obj)

    @swagger.operation(
        summary='Delete Study Design Descriptor',
        notes="""Delete Study Design Descriptor.
              <br>
              Use descriptor annotation value as a query parameter to filter out.""",
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
                "name": "term",
                "description": "Design Descriptor annotation value",
                "required": True,
                "allowEmptyValue": False,
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
    def delete(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('term', help="Design Descriptor annotation value", location="args")
        args = parser.parse_args()
        obj_term = args['term']
        if obj_term is None:
            abort(404)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            abort(401)

        # check for keeping copies
        save_audit_copy = False
        save_msg_str = "NOT be"
        if "save_audit_copy" in request.headers and \
                request.headers["save_audit_copy"].lower() == 'true':
            save_audit_copy = True
            save_msg_str = "be"

        # delete Study Design Descriptor
        logger.info('Deleting Study Design Descriptor %s for %s, using API-Key %s', obj_term, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)

        found = False
        for index, obj in enumerate(isa_study.design_descriptors):
            if obj.term == obj_term:
                found = True
                # delete Study Design Descriptor
                del isa_study.design_descriptors[index]
                break
        if not found:
            abort(404)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Deleted %s', obj.term)

        return StudyDesignDescriptorSchema().dump(obj)

    @swagger.operation(
        summary='Update Study Design Descriptor',
        notes="""Update Study Design Descriptor.
              <br>
              Use descriptor annotation value as a query parameter to filter out.""",
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
                "name": "term",
                "description": "Design Descriptor annotation value",
                "required": True,
                "allowEmptyValue": False,
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
            },
            {
                "name": "protocol",
                "description": 'Design Descriptor in ISA-JSON format.',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('term', help="Design Descriptor annotation value")
        args = parser.parse_args()
        descriptor_term = args['term']
        if descriptor_term is None:
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
        updated_descriptor = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['studyDesignDescriptor']
            # if partial=True missing fields will be ignored
            result = StudyDesignDescriptorSchema().load(data, partial=False)
            updated_descriptor = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # update Study Design Descriptor details
        logger.info('Updating Study Design Descriptor details for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
        found = False
        for index, descriptor in enumerate(isa_study.design_descriptors):
            if descriptor.term == descriptor_term:
                found = True
                # update protocol details
                isa_study.design_descriptors[index] = updated_descriptor
                break
        if not found:
            abort(404)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Updated %s', updated_descriptor.term)

        return StudyDesignDescriptorSchema().dump(updated_descriptor)


class StudyPublications(Resource):

    @swagger.operation(
        summary='Add new Study Publication',
        notes='Add new Publication to a Study.',
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "publication",
                "description": 'Study Publication in ISA-JSON format.',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
            },
            {
                "code": 409,
                "message": "Conflict. The request could not be completed due to a conflict"
                           " with the current state of study. This is usually issued to prevent duplications."
            }
        ]
    )
    def post(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('title', help="Study Publication title")
        args = parser.parse_args()
        publication_title = args['title']
        # No params allowed, just to prevent confusion with UPDATE
        if publication_title:
            abort(400)
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
        new_publication = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['publication']
            # if partial=True missing fields will be ignored
            result = PublicationSchema().load(data, partial=False)
            new_publication = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # Add new Publication
        logger.info('Adding new Publication %s for %s, using API-Key %s',
                    new_publication.title, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
        # check for Publication added already
        for index, publication in enumerate(isa_study.publications):
            if publication.title == new_publication.title:
                abort(409)
        # add Study Publication
        isa_study.publications.append(new_publication)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Added %s', new_publication.title)

        return PublicationSchema().dump(new_publication)

    @swagger.operation(
        summary="Get Study Publications",
        notes="""Get Study Publications.
              <br>
              Use publication title as a query parameter to filter out.""",
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
                "name": "title",
                "description": "Publication title",
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
        # ToDo add authors, PubMedID and DOI filters
        parser = reqparse.RequestParser()
        parser.add_argument('title', help='Publication title')
        obj_title = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_title = args['title']

        logger.info('Getting Study Publications for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)

        obj_list = isa_study.publications
        # Using context to avoid envelop tags in contained objects
        sch = PublicationSchema()
        sch.context['publication'] = Publication()
        if obj_title is None:
            # return a list of publications
            logger.info('Got %s publications', len(isa_study.publications))
            return sch.dump(obj_list, many=True)
        else:
            # return a single publication
            found = False
            for index, obj in enumerate(isa_study.publications):
                if obj.title == obj_title:
                    found = True
                    break
            if not found:
                abort(404)
            logger.info('Got %s', obj.title)
            return sch.dump(obj)

    @swagger.operation(
        summary='Delete Study Publication',
        notes="""Delete Study Publication.
              <br>
              Use publication title as a query parameter to filter out.""",
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
                "name": "title",
                "description": "Publication title",
                "required": True,
                "allowEmptyValue": False,
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
    def delete(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('title', help="Publication title", location="args")
        args = parser.parse_args()
        publication_title = args['title']
        if publication_title is None:
            abort(404)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            abort(401)

        # check for keeping copies
        save_audit_copy = False
        save_msg_str = "NOT be"
        if "save_audit_copy" in request.headers and \
                request.headers["save_audit_copy"].lower() == 'true':
            save_audit_copy = True
            save_msg_str = "be"

        # delete publication
        logger.info('Deleting Study Publication %s for %s, using API-Key %s', publication_title, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
        found = False
        for index, publication in enumerate(isa_study.publications):
            if publication.title == publication_title:
                found = True
                # delete Study Publication
                del isa_study.publications[index]
                break
        if not found:
            abort(404)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Deleted %s', publication.title)

        return PublicationSchema().dump(publication)

    @swagger.operation(
        summary='Update Study Publication',
        notes="""Update Study Publication.
              <br>
              Use publication title as a query parameter to filter out.""",
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
                "name": "title",
                "description": "Publication title",
                "required": True,
                "allowEmptyValue": False,
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
            },
            {
                "name": "protocol",
                "description": 'Publication in ISA-JSON format.',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('title', help="Publication title")
        args = parser.parse_args()
        publication_title = args['title']
        if publication_title is None:
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
        updated_publication = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['publication']
            # if partial=True missing fields will be ignored
            result = PublicationSchema().load(data, partial=False)
            updated_publication = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # update Study Publication details
        logger.info('Updating Study Publication details for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
        found = False
        for index, publication in enumerate(isa_study.publications):
            if publication.title == publication_title:
                found = True
                # update protocol details
                isa_study.publications[index] = updated_publication
                break
        if not found:
            abort(404)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Updated %s', updated_publication.title)

        return PublicationSchema().dump(updated_publication)


class StudySources(Resource):

    @swagger.operation(
        summary="Get Study Sources",
        notes="""Get Study Sources.
              <br>
              Use source name as a query parameter to filter out.""",
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
                "name": "name",
                "description": "Study Source name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "list_only",
                "description": "List names only",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True
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
        parser.add_argument('list_only', help='List names only')
        list_only = None
        obj_name = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name']
            list_only = args['list_only']

        logger.info('Getting Study Sources for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        obj_list = isa_study.sources
        # Using context to avoid envelop tags in contained objects
        sch = SourceSchema()
        sch.context['source'] = Source()
        if obj_name is None:
            # return a list of objs
            logger.info('Got %s sources', len(obj_list))
            if list_only in ['true', 'True']:
                sch = SourceSchema(only=['name'])
                sch.context['source'] = Source()
            return sch.dump(obj_list, many=True)
        else:
            # return a single obj
            found = False
            for index, obj in enumerate(obj_list):
                if obj.name == obj_name:
                    found = True
                    break
            if not found:
                abort(404)
            logger.info('Got %s', obj.name)
            return sch.dump(obj)

    @swagger.operation(
        summary='Update Study Source',
        notes="""Update Study Source.
              <br>
              Use source name as a query parameter to filter out.""",
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
                "name": "name",
                "description": "Study Source name",
                "required": True,
                "allowEmptyValue": False,
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
            },
            {
                "name": "source",
                "description": 'Study Source in ISA-JSON format.',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help="Study Source name")
        args = parser.parse_args()
        obj_name = args['name']
        if obj_name is None:
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
        updated_obj = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['source']
            # if partial=True missing fields will be ignored
            result = SourceSchema().load(data, partial=False)
            updated_obj = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # update Study Source details
        logger.info('Updating Study Source details for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        obj_list = isa_study.sources
        found = False
        for i, proc in enumerate(isa_study.process_sequence):
            for index, src in enumerate(proc.inputs):
                if isinstance(src, Source):
                    if src.name == obj_name:
                        found = True
                        proc.inputs[index] = Source(name=updated_obj.name,
                                                    characteristics=updated_obj.characteristics,
                                                    comments=updated_obj.comments)
                        break
        if not found:
            abort(404)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path,
                            save_investigation_copy=save_audit_copy,
                            save_samples_copy=True, save_assays_copy=True)
        logger.info('Updated %s', updated_obj.name)

        return SourceSchema().dump(updated_obj)


class StudySamples(Resource):

    @swagger.operation(
        summary='Add new Study Sample',
        notes='Add new Sample to a Study.',
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "sample",
                "description": 'Study Sample in ISA-JSON format.',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
            },
            {
                "code": 409,
                "message": "Conflict. The request could not be completed due to a conflict"
                           " with the current state of study. This is usually issued to prevent duplications."
            }
        ]
    )
    def post(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help="Study Sample name")
        args = parser.parse_args()
        obj_name = args['name']
        # No params allowed, just to prevent confusion with UPDATE
        if obj_name:
            abort(400)
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
        new_sample = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            # TODO change to allow multiple samples
            data = data_dict['sample']
            # if partial=True missing fields will be ignored
            result = SampleSchema().load(data, partial=False)
            new_sample = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # Add new Study Sample
        logger.info('Adding new Sample %s to %s, using API-Key %s',
                    new_sample.name, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        # check for existing Samples
        sample_list = isa_study.samples
        for index, sample in enumerate(sample_list):
            if sample.name == new_sample.name:
                abort(409)

        # add Sources to the Study
        # apparently duplications are handled by the API, so no checking
        for source in new_sample.derives_from:
            isa_study.sources.append(source)
        isa_study.samples.append(new_sample)

        # check for existing Protocol
        sc_protocol = None
        for prot in isa_study.protocols:
            if prot.name == 'Sample collection':
                sc_protocol = prot
                break
        # if not found, create a new one
        if not sc_protocol:
            sc_protocol = Protocol(name='Sample collection', protocol_type=OntologyAnnotation(term='Sample collection'))

        # add the ProcessSequence
        sc_process = Process(name=new_sample.name, executes_protocol=sc_protocol, inputs=new_sample.derives_from,
                             outputs=[new_sample],
                             comments=[Comment(name='Created', value=time.strftime("%Y%m%d%H%M%S"))])
        isa_study.process_sequence.append(sc_process)

        # persist everything
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path,
                            save_investigation_copy=save_audit_copy,
                            save_samples_copy=True, save_assays_copy=True)
        logger.info('Added %s', new_sample.name)

        return SampleSchema().dump(new_sample)

    @swagger.operation(
        summary="Get Study Samples",
        notes="""Get Study Samples.
              <br>
              Use sample name as a query parameter to filter out.""",
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
                "name": "name",
                "description": "Study Sample name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "list_only",
                "description": "List names only",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True
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
        parser.add_argument('list_only', help='List names only')
        list_only = None
        obj_name = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name']
            list_only = args['list_only']

        logger.info('Getting Samples for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        obj_list = isa_study.samples
        # Using context to avoid envelop tags in contained objects
        sch = SampleSchema()
        sch.context['sample'] = Sample()
        if obj_name is None:
            # return a list of objs
            logger.info('Got %s samples', len(obj_list))
            if list_only in ['true', 'True']:
                sch = SampleSchema(only=['name'])
                sch.context['sample'] = Sample()
            return sch.dump(obj_list, many=True)
        else:
            # return a single obj
            found = False
            for index, obj in enumerate(obj_list):
                if obj.name == obj_name:
                    found = True
                    break
            if not found:
                abort(404)
            logger.info('Got %s', obj.name)
            return sch.dump(obj)

    @swagger.operation(
        summary='Update Study Sample',
        notes="""Update Study Sample.
              <br>
              Use sample name as a query parameter to filter out.""",
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
                "name": "name",
                "description": "Study Sample name",
                "required": True,
                "allowEmptyValue": False,
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
            },
            {
                "name": "sample",
                "description": 'Study Sample in ISA-JSON format.',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help="Study Sample name")
        args = parser.parse_args()
        obj_name = args['name']
        if obj_name is None:
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
        updated_obj = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['sample']
            # if partial=True missing fields will be ignored
            result = SampleSchema().load(data, partial=False)
            updated_obj = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # update Study Source details
        logger.info('Updating Study Sample details for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        obj_list = isa_study.samples
        found = False
        for i, proc in enumerate(isa_study.process_sequence):
            for index, src in enumerate(proc.outputs):
                if isinstance(src, Sample):
                    if src.name == obj_name:
                        found = True
                        proc.outputs[index] = Sample(name=updated_obj.name,
                                                     characteristics=updated_obj.characteristics,
                                                     factor_values=updated_obj.factor_values,
                                                     derives_from=updated_obj.derives_from,
                                                     comments=updated_obj.comments)
                        break
        if not found:
            abort(404)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path,
                            save_investigation_copy=save_audit_copy,
                            save_samples_copy=True, save_assays_copy=True)
        logger.info('Updated %s', updated_obj.name)

        return SampleSchema().dump(updated_obj)

    @swagger.operation(
        summary='Delete Study Samples',
        notes="""Remove all Samples marked to be deleted.
              <br>
              This method does not use the ISA-API. Instead, it access directly the ISA-Tab s_*.txt files
              in the MetaboLights Study folder and removes all rows marked to be deleted.
              Sample names must be prefixed with __TO_BE_DELETED__<samplename>
              <br><br>
              <b>NOTE:</b> No other actions (removing assays or raw data files) are done, so 
              it may result with some orphan objects left in the study.""",
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
    def delete(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            abort(401)

        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)

        # delete samples
        logger.info('Deleting all marked Samples for %s, using API-Key %s', study_id, user_token)
        location = wsc.get_study_location(study_id, user_token)
        removed_lines = 0
        try:
            removed_lines = utils.remove_samples_from_isatab(location)
        except Exception:
            abort(500)
        return {'removed_lines': removed_lines}


class StudyOtherMaterials(Resource):

    @swagger.operation(
        summary='Add new Study Other Materials',
        notes='Add new Material to a Study.',
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "material",
                "description": 'Study Material in ISA-JSON format.',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
            },
            {
                "code": 409,
                "message": "Conflict. The request could not be completed due to a conflict"
                           " with the current state of study. This is usually issued to prevent duplications."
            }
        ]
    )
    def post(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help="Material Sample name")
        args = parser.parse_args()
        obj_name = args['name']
        # No params allowed, just to prevent confusion with UPDATE
        if obj_name:
            abort(400)
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
        new_material = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['otherMaterial']
            # if partial=True missing fields will be ignored
            result = OtherMaterialSchema().load(data, partial=False)
            new_material = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # Add new Study Material
        logger.info('Adding new Material %s to %s, using API-Key %s',
                    new_material.name, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        # check for existing Material
        material_list = isa_study.other_material
        for index, material in enumerate(material_list):
            if material.name == new_material.name:
                abort(409)

        # add Material to the Study
        isa_study.other_material.append(new_material)

        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path,
                            save_investigation_copy=save_audit_copy,
                            save_samples_copy=True, save_assays_copy=True)
        logger.info('Added %s', new_material.name)

        sch = OtherMaterialSchema()
        sch.context['other_material'] = Material()
        return sch.dump(new_material)

    @swagger.operation(
        summary="Get Study Other Materials",
        notes="""Get Study Other Materials.
              <br>
              Use material name as a query parameter to filter out.""",
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
                "name": "name",
                "description": "Study Material name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "list_only",
                "description": "List names only",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True
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
        parser.add_argument('name', help='Study Other Materials name')
        parser.add_argument('list_only', help='List names only')
        list_only = None
        obj_name = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name']
            list_only = args['list_only']

        logger.info('Getting Other Materials for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        obj_list = isa_study.other_material
        # Using context to avoid envelop tags in contained objects
        sch = OtherMaterialSchema()
        sch.context['other_material'] = Material()
        if obj_name is None:
            # return a list of objs
            logger.info('Got %s Materials', len(obj_list))
            if list_only in ['true', 'True']:
                sch = OtherMaterialSchema(only=['name'])
                sch.context['other_material'] = Material()
            return sch.dump(obj_list, many=True)
        else:
            # return a single obj
            found = False
            for index, obj in enumerate(obj_list):
                if obj.name == obj_name:
                    found = True
                    break
            if not found:
                abort(404)
            logger.info('Got %s', obj.name)
            return sch.dump(obj)

    @swagger.operation(
        summary='Delete Study Other Materials',
        notes="""Delete Study Other Materials.
              <br>
              Use material name as a query parameter to filter out.""",
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
                "name": "name",
                "description": "Material name",
                "required": True,
                "allowEmptyValue": False,
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
    def delete(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help="Material name", location="args")
        args = parser.parse_args()
        obj_name = args['name']
        if obj_name is None:
            abort(404)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            abort(401)

        # check for keeping copies
        save_audit_copy = False
        save_msg_str = "NOT be"
        if "save_audit_copy" in request.headers and \
                request.headers["save_audit_copy"].lower() == 'true':
            save_audit_copy = True
            save_msg_str = "be"

        # delete Other Materials
        logger.info('Deleting Study Material %s for %s, using API-Key %s', obj_name, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        obj_list = isa_study.other_material
        found = False
        for index, material in enumerate(obj_list):
            if material.name == obj_name:
                found = True
                # delete material
                del isa_study.other_material[index]
                break
        if not found:
            abort(404)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Deleted %s', obj_name)

        return OtherMaterialSchema().dump(material)

    @swagger.operation(
        summary='Update Study Other Materials',
        notes="""Update Study Other Materials.
              <br>
              Use material name as a query parameter to filter out.""",
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
                "name": "name",
                "description": "Study Material name",
                "required": True,
                "allowEmptyValue": False,
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
            },
            {
                "name": "material",
                "description": 'Study Material in ISA-JSON format.',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help="Study Material name")
        args = parser.parse_args()
        obj_name = args['name']
        if obj_name is None:
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
        updated_obj = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['sample']
            # if partial=True missing fields will be ignored
            result = OtherMaterialSchema().load(data, partial=False)
            updated_obj = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # update Study Material details
        logger.info('Updating Study Material details for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        obj_list = isa_study.other_material
        found = False
        for index, material in enumerate(obj_list):
            if material.name == obj_name:
                found = True
                # update study
                isa_study.other_material[index] = updated_obj
                break
        if not found:
            abort(404)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path,
                            save_investigation_copy=save_audit_copy,
                            save_samples_copy=True, save_assays_copy=True)
        logger.info('Updated %s', updated_obj.name)

        return SampleSchema().dump(updated_obj)


class StudyProcesses(Resource):
    @swagger.operation(
        summary="Get Study Processes",
        notes="""Get Study Processes.
                  <br>
                  Use process name as a query parameter to filter out.""",
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
                "name": "name",
                "description": "Study Process name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "list_only",
                "description": "List names only",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": True,
                "default": True
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
        parser.add_argument('name', help='Study Processes name')
        parser.add_argument('list_only', help='List names only')
        list_only = None
        obj_name = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name']
            list_only = args['list_only']

        logger.info('Getting Study Processes for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        obj_list = isa_study.process_sequence
        # Using context to avoid envelop tags in contained objects
        sch = ProcessSchema()
        sch.context['process'] = Process()
        if obj_name is None:
            # return a list of objs
            logger.info('Got %s processes', len(obj_list))
            if list_only in ['true', 'True']:
                sch = ProcessSchema(only=['name', 'date', 'executes_protocol.name'])
                sch.context['process'] = Process()
            return sch.dump(obj_list, many=True)
        else:
            # return a single obj
            found = False
            for index, obj in enumerate(obj_list):
                if obj.name == obj_name:
                    found = True
                    break
            if not found:
                abort(404)
            logger.info('Got %s', obj.name)
            return sch.dump(obj)
