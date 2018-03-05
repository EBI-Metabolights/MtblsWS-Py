import logging
from flask import request, jsonify
from flask_restful import Resource, abort, marshal_with, reqparse
from marshmallow import ValidationError
from app.ws.isaApiClient import IsaApiClient
from app.ws.mm_models import *
from app.ws.mtblsWSclient import WsClient
from app.ws.models import *
from flask_restful_swagger import swagger

"""
ISA Study

Manage MTBLS studies from ISA-Tab files using ISA-API
"""

logger = logging.getLogger('wslog')
iac = IsaApiClient()
wsc = WsClient()


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


class IsaJsonStudies(Resource):
    @swagger.operation(
        summary="Get all Studies",
        notes="Get a list of all public Studies.",
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def get(self):
        logger.info('Getting all public studies')
        pub_list = wsc.get_public_studies()
        logger.info('... found %d public studies', len(pub_list['content']))
        return jsonify(pub_list)

    @swagger.operation(
        summary="Create new Study",
        notes="Create new Study.",
        nickname="New Study",
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
    @marshal_with(Investigation_api_model, envelope='investigation')
    def post(self):
        # User authentication
        if "user_token" not in request.headers:
            abort(401)
        user_token = request.headers["user_token"]

        if not wsc.is_user_token_valid(user_token):
            abort(403)

        # body content validation
        if request.data is None or request.json is None:
            abort(400)

        # read inv data from request body
        data_dict = request.get_json(force=True)
        # data_dict = json.loads(request.data.decode('utf-8'))
        try:
            title = data_dict['title']
            description = data_dict['description']
            sub_date = data_dict['submissionDate']
            pub_rel_date = data_dict['publicReleaseDate']
        except Exception as inst:
            logger.warning('Malformed request. Some of the required fields are missing')
            abort(400)

        logger.info('Creating a new Study.')
        inv_obj = iac.create_new_study(title=title,
                                       description=description,
                                       sub_date=sub_date,
                                       pub_rel_date=pub_rel_date)
        logger.info('New Study, title: %s, desc.: %s, pub.rel.date: %s',
                    title, description, pub_rel_date)

        return inv_obj, 201


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
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
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
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
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
        # add contact
        isa_study.contacts.append(new_contact)
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
        logger.info('Added %s', new_contact.email)

        return PersonSchema().dump(new_contact)

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

        if email is None:
            # return a list of users
            logger.info('Got %s contacts', len(isa_study.contacts))
            return PersonSchema().dump(isa_study.contacts, many=True)
        else:
            # return a single user
            isa_person_found = False
            contact = None
            for index, contact in enumerate(isa_study.contacts):
                if contact.email == email:
                    isa_person_found = True
                    break
            if not isa_person_found:
                abort(404)
            logger.info('Got %s', contact.email)
            return PersonSchema().dump(contact)

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
        person_found = False
        for index, person in enumerate(isa_study.contacts):
            if person.email == email:
                person_found = True
                # update person details
                isa_study.contacts[index] = updated_contact
                break
        if not person_found:
            abort(404)
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
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
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
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
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help="Protocol name")
        args = parser.parse_args()
        prot_name = args['name']
        # No protocol param allowed, just to prevent confusion with UPDATE
        if prot_name:
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
        new_protocol = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['protocol']
            # if partial=True missing fields will be ignored
            result = ProtocolSchema().load(data, partial=False)
            new_protocol = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # Add new protocol
        logger.info('Adding new Protocol %s for %s, using API-Key %s', new_protocol.name, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
        # check for protocol added already
        for index, prot_name in enumerate(isa_study.protocols):
            if prot_name.name == new_protocol.name:
                abort(409)
        # add protocol
        isa_study.protocols.append(new_protocol)
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
        logger.info('Added %s', new_protocol.name)

        return ProtocolSchema().dump(new_protocol)

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
        prot_name = None
        if request.args:
            args = parser.parse_args(req=request)
            prot_name = args['name']

        logger.info('Getting Study protocols for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)

        if prot_name is None:
            # return a list of protocols
            logger.info('Got %s protocols', len(isa_study.protocols))
            return ProtocolSchema().dump(isa_study.protocols, many=True)
        else:
            # return a single protocol
            protocol_found = False
            protocol = None
            for index, protocol in enumerate(isa_study.protocols):
                if protocol.name == prot_name:
                    protocol_found = True
                    break
            if not protocol_found:
                abort(404)
            logger.info('Got %s', protocol.name)
            return ProtocolSchema().dump(protocol)

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
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help="Protocol name", location="args")
        args = parser.parse_args()
        prot_name = args['name']
        if prot_name is None:
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
        logger.info('Deleting protocol %s for %s, using API-Key %s', prot_name, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
        protocol_found = False
        for index, protocol in enumerate(isa_study.protocols):
            if protocol.name == prot_name:
                protocol_found = True
                # delete protocol
                del isa_study.protocols[index]
                break
        if not protocol_found:
            abort(404)
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
        logger.info('Deleted %s', protocol.name)

        return ProtocolSchema().dump(protocol)

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
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
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
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help="Study Factor name")
        args = parser.parse_args()
        factor_name = args['name']
        # No params allowed, just to prevent confusion with UPDATE
        if factor_name:
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
        new_factor = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['factor']
            # if partial=True missing fields will be ignored
            result = StudyFactorSchema().load(data, partial=False)
            new_factor = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # Add new Study Factor
        logger.info('Adding new Study Factor %s for %s, using API-Key %s', new_factor.name, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
        # check for Study Factor added already
        for index, factor in enumerate(isa_study.factors):
            if factor.name == new_factor.name:
                abort(409)
        # add Study Factor
        isa_study.factors.append(new_factor)
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
        logger.info('Added %s', new_factor.name)

        return StudyFactorSchema().dump(new_factor)

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
        factor_name = None
        if request.args:
            args = parser.parse_args(req=request)
            factor_name = args['name']

        logger.info('Getting Study Factors for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)

        if factor_name is None:
            # return a list of factors
            logger.info('Got %s factors', len(isa_study.factors))
            return StudyFactorSchema().dump(isa_study.factors, many=True)
        else:
            # return a single factor
            found = False
            for index, factor in enumerate(isa_study.factors):
                if factor.name == factor_name:
                    found = True
                    break
            if not found:
                abort(404)
            logger.info('Got %s', factor.name)
            return StudyFactorSchema().dump(factor)

    @swagger.operation(
        summary='Delete Study Factor',
        notes="""Delete Study protocol.
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
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help="Factor name", location="args")
        args = parser.parse_args()
        factor_name = args['name']
        if factor_name is None:
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
        logger.info('Deleting Study Factor %s for %s, using API-Key %s', factor_name, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
        found = False
        for index, factor in enumerate(isa_study.factors):
            if factor.name == factor_name:
                found = True
                # delete Study Factor
                del isa_study.factors[index]
                break
        if not found:
            abort(404)
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
        logger.info('Deleted %s', factor.name)

        return StudyFactorSchema().dump(factor)

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
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
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
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('annotationValue', help="Study Design Descriptor annotation value")
        args = parser.parse_args()
        descriptor_value = args['annotationValue']
        # No params allowed, just to prevent confusion with UPDATE
        if descriptor_value:
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
        new_descriptor = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['studyDesignDescriptor']
            # if partial=True missing fields will be ignored
            result = OntologyAnnotationSchema().load(data, partial=False)
            new_descriptor = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # Add new Study Descriptor
        logger.info('Adding new Study Design Descriptor %s for %s, using API-Key %s',
                    new_descriptor.term, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
        # check for Study Descriptor added already
        for index, descriptor in enumerate(isa_study.design_descriptors):
            if descriptor.term == new_descriptor.term:
                abort(409)
        # add Study Descriptor
        isa_study.design_descriptors.append(new_descriptor)
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
        logger.info('Added %s', new_descriptor.term)

        return StudyDesignDescriptorSchema().dump(new_descriptor)

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
        descriptor_value = None
        if request.args:
            args = parser.parse_args(req=request)
            descriptor_value = args['annotationValue']

        logger.info('Getting Study Design Descriptors for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)

        if descriptor_value is None:
            # return a list of descriptors
            logger.info('Got %s descriptors', len(isa_study.design_descriptors))
            return StudyDesignDescriptorSchema().dump(isa_study.design_descriptors, many=True)
        else:
            # return a single descriptor
            found = False
            for index, descriptor in enumerate(isa_study.design_descriptors):
                if descriptor.term == descriptor_value:
                    found = True
                    break
            if not found:
                abort(404)
            logger.info('Got %s', descriptor.term)
            return StudyDesignDescriptorSchema().dump(descriptor)







# class StudyDescriptors(Resource):
#     @swagger.operation(
#         summary="Get Study Descriptors",
#         notes="Get the list of design descriptors associated with the Study.",
#         parameters=[
#             {
#                 "name": "study_id",
#                 "description": "MTBLS Identifier",
#                 "required": True,
#                 "allowMultiple": False,
#                 "paramType": "path",
#                 "dataType": "string"
#             },
#             {
#                 "name": "user_token",
#                 "description": "User API token",
#                 "paramType": "header",
#                 "type": "string",
#                 "required": False,
#                 "allowMultiple": False
#             }
#         ],
#         responseMessages=[
#             {
#                 "code": 200,
#                 "message": "OK."
#             },
#             {
#                 "code": 400,
#                 "message": "Bad Request. Server could not understand the request due to malformed syntax."
#             },
#             {
#                 "code": 401,
#                 "message": "Unauthorized. Access to the resource requires user authentication."
#             },
#             {
#                 "code": 403,
#                 "message": "Forbidden. Access to the study is not allowed for this user."
#             },
#             {
#                 "code": 404,
#                 "message": "Not found. The requested identifier is not valid or does not exist."
#             }
#         ]
#     )
#     @marshal_with(Inv_OntologyAnnotation_api_model, envelope='descriptors')
#     def get(self, study_id):
#         # param validation
#         if study_id is None:
#             abort(404)
#         # User authentication
#         user_token = None
#         if "user_token" in request.headers:
#             user_token = request.headers["user_token"]
#
#         logger.info('Getting Study design descriptors for %s, using API-Key %s', study_id, user_token)
#         # check for access rights
#         if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
#             abort(403)
#         isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
#         isa_descriptors = isa_study.design_descriptors
#         str_descriptors = json.dumps({'descriptors': isa_descriptors}, default=serialize_ontology_annotation,
#                                      sort_keys=True)
#         logger.info('Got %s', str_descriptors)
#         return isa_descriptors
#
#     @swagger.operation(
#         summary='Update Study descriptors',
#         notes='Update the list of design descriptors associated with the Study.',
#         parameters=[
#             {
#                 "name": "study_id",
#                 "description": "MTBLS Identifier",
#                 "required": True,
#                 "allowMultiple": False,
#                 "paramType": "path",
#                 "dataType": "string"
#             },
#             {
#                 "name": "user_token",
#                 "description": "User API token",
#                 "paramType": "header",
#                 "type": "string",
#                 "required": True,
#                 "allowMultiple": False
#             },
#             {
#                 "name": "descriptors",
#                 "description": 'Update the list of design descriptors associated with the Study.',
#                 "paramType": "body",
#                 "type": "string",
#                 "format": "application/json",
#                 "required": True,
#                 "allowMultiple": False
#             },
#             {
#                 "name": "save_audit_copy",
#                 "description": "Keep track of changes saving a copy of the unmodified files.",
#                 "paramType": "header",
#                 "type": "Boolean",
#                 "defaultValue": True,
#                 "format": "application/json",
#                 "required": False,
#                 "allowMultiple": False
#             }
#         ],
#         responseMessages=[
#             {
#                 "code": 200,
#                 "message": "OK."
#             },
#             {
#                 "code": 400,
#                 "message": "Bad Request. Server could not understand the request due to malformed syntax."
#             },
#             {
#                 "code": 401,
#                 "message": "Unauthorized. Access to the resource requires user authentication."
#             },
#             {
#                 "code": 403,
#                 "message": "Forbidden. Access to the study is not allowed for this user."
#             },
#             {
#                 "code": 404,
#                 "message": "Not found. The requested identifier is not valid or does not exist."
#             }
#         ]
#     )
#     @marshal_with(Inv_OntologyAnnotation_api_model, envelope='descriptors')
#     def put(self, study_id):
#         # param validation
#         if study_id is None:
#             abort(404)
#         # User authentication
#         user_token = None
#         if "user_token" in request.headers:
#             user_token = request.headers["user_token"]
#         # body content validation
#         if request.data is None or request.json is None:
#             abort(400)
#         data_dict = json.loads(request.data.decode('utf-8'))
#         json_descriptors = data_dict['descriptors']
#         isa_descriptors = list()
#         for json_descriptor in json_descriptors:
#             isa_descriptor = unserialize_ontology_annotation(json_descriptor)
#             isa_descriptors.append(isa_descriptor)
#         # check for keeping copies
#         save_audit_copy = False
#         save_msg_str = "NOT be"
#         if "save_audit_copy" in request.headers and request.headers["save_audit_copy"].lower() == 'true':
#             save_audit_copy = True
#             save_msg_str = "be"
#
#         # update study descriptors
#         logger.info('Updating Study descriptors for %s, using API-Key %s', study_id, user_token)
#         # check for access rights
#         if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
#             abort(403)
#         isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
#         isa_study.design_descriptors = isa_descriptors
#         logging.info("A copy of the previous files will %s saved", save_msg_str)
#         iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
#         logger.info('Applied %s', json_descriptors)
#         return isa_descriptors
#
#
# class StudyPublications(Resource):
#     @swagger.operation(
#         summary="Get Study Publications",
#         notes="Get the list of publications associated with the Study.",
#         parameters=[
#             {
#                 "name": "study_id",
#                 "description": "MTBLS Identifier",
#                 "required": True,
#                 "allowMultiple": False,
#                 "paramType": "path",
#                 "dataType": "string"
#             },
#             {
#                 "name": "user_token",
#                 "description": "User API token",
#                 "paramType": "header",
#                 "type": "string",
#                 "required": False,
#                 "allowMultiple": False
#             }
#         ],
#         responseMessages=[
#             {
#                 "code": 200,
#                 "message": "OK."
#             },
#             {
#                 "code": 400,
#                 "message": "Bad Request. Server could not understand the request due to malformed syntax."
#             },
#             {
#                 "code": 401,
#                 "message": "Unauthorized. Access to the resource requires user authentication."
#             },
#             {
#                 "code": 403,
#                 "message": "Forbidden. Access to the study is not allowed for this user."
#             },
#             {
#                 "code": 404,
#                 "message": "Not found. The requested identifier is not valid or does not exist."
#             }
#         ]
#     )
#     @marshal_with(StudyPublications_api_model, envelope='publications')
#     def get(self, study_id):
#         # param validation
#         if study_id is None:
#             abort(404)
#         # User authentication
#         user_token = None
#         if "user_token" in request.headers:
#             user_token = request.headers["user_token"]
#
#         logger.info('Getting Study publications for %s, using API-Key %s', study_id, user_token)
#         # check for access rights
#         if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
#             abort(403)
#         isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
#         isa_publications = isa_study.publications
#         str_publications = json.dumps({'publications': isa_publications},
#                                       default=serialize_study_publication, sort_keys=True)
#         logger.info('Got %s', str_publications)
#         return isa_publications
#
#     @swagger.operation(
#         summary='Update Study Publications',
#         notes='Update the list of publications associated with the Study.',
#         parameters=[
#             {
#                 "name": "study_id",
#                 "description": "MTBLS Identifier",
#                 "required": True,
#                 "allowMultiple": False,
#                 "paramType": "path",
#                 "dataType": "string"
#             },
#             {
#                 "name": "user_token",
#                 "description": "User API token",
#                 "paramType": "header",
#                 "type": "string",
#                 "required": True,
#                 "allowMultiple": False
#             },
#             {
#                 "name": "publications",
#                 "description": 'Update the list of publications associated with the Study.',
#                 "paramType": "body",
#                 "type": "string",
#                 "format": "application/json",
#                 "required": True,
#                 "allowMultiple": False
#             },
#             {
#                 "name": "save_audit_copy",
#                 "description": "Keep track of changes saving a copy of the unmodified files.",
#                 "paramType": "header",
#                 "type": "Boolean",
#                 "defaultValue": True,
#                 "format": "application/json",
#                 "required": False,
#                 "allowMultiple": False
#             }
#         ],
#         responseMessages=[
#             {
#                 "code": 200,
#                 "message": "OK."
#             },
#             {
#                 "code": 400,
#                 "message": "Bad Request. Server could not understand the request due to malformed syntax."
#             },
#             {
#                 "code": 401,
#                 "message": "Unauthorized. Access to the resource requires user authentication."
#             },
#             {
#                 "code": 403,
#                 "message": "Forbidden. Access to the study is not allowed for this user."
#             },
#             {
#                 "code": 404,
#                 "message": "Not found. The requested identifier is not valid or does not exist."
#             }
#         ]
#     )
#     @marshal_with(StudyPublications_api_model, envelope='publications')
#     def put(self, study_id):
#         # param validation
#         if study_id is None:
#             abort(404)
#         # User authentication
#         user_token = None
#         if "user_token" in request.headers:
#             user_token = request.headers["user_token"]
#         # body content validation
#         if request.data is None or request.json is None:
#             abort(400)
#         data_dict = json.loads(request.data.decode('utf-8'))
#         json_publications = data_dict['publications']
#         isa_publications = list()
#         for json_publication in json_publications:
#             isa_publication = unserialize_study_publication(json_publication)
#             isa_publications.append(isa_publication)
#         # check for keeping copies
#         save_audit_copy = False
#         save_msg_str = "NOT be"
#         if "save_audit_copy" in request.headers and request.headers["save_audit_copy"].lower() == 'true':
#             save_audit_copy = True
#             save_msg_str = "be"
#
#         # update study publications
#         logger.info('Updating Study publications for %s, using API-Key %s', study_id, user_token)
#         # check for access rights
#         if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
#             abort(403)
#         isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
#         isa_study.publications = isa_publications
#         logging.info("A copy of the previous files will %s saved", save_msg_str)
#         iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
#         logger.info('Applied %s', json_publications)
#         return isa_publications
#
#
# class StudyMaterials(Resource):
#     @swagger.operation(
#         summary="Get all materials in a Study",
#         notes="Get the list of materials associated with the Study.",
#         parameters=[
#             {
#                 "name": "study_id",
#                 "description": "MTBLS Identifier",
#                 "required": True,
#                 "allowMultiple": False,
#                 "paramType": "path",
#                 "dataType": "string"
#             },
#             {
#                 "name": "user_token",
#                 "description": "User API token",
#                 "paramType": "header",
#                 "type": "string",
#                 "required": False,
#                 "allowMultiple": False
#             }
#         ],
#         responseMessages=[
#             {
#                 "code": 200,
#                 "message": "OK."
#             },
#             {
#                 "code": 400,
#                 "message": "Bad Request. Server could not understand the request due to malformed syntax."
#             },
#             {
#                 "code": 401,
#                 "message": "Unauthorized. Access to the resource requires user authentication."
#             },
#             {
#                 "code": 403,
#                 "message": "Forbidden. Access to the study is not allowed for this user."
#             },
#             {
#                 "code": 404,
#                 "message": "Not found. The requested identifier is not valid or does not exist."
#             }
#         ]
#     )
#     @marshal_with(StudyMaterial_api_model, envelope='materials')
#     def get(self, study_id):
#         # param validation
#         if study_id is None:
#             abort(404)
#         # User authentication
#         user_token = None
#         if "user_token" in request.headers:
#             user_token = request.headers["user_token"]
#
#         logger.info('Getting Study sources for %s, using API-Key %s', study_id, user_token)
#         # check for access rights
#         if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
#             abort(403)
#         isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token)
#         isa_materials = isa_study.materials
#         logger.debug('Got %s', isa_materials)
#         return isa_materials
#
#
# class StudySources(Resource):
#     @swagger.operation(
#         summary="Get all sources in a Study",
#         notes="Get the list of source names associated with the Study.",
#         parameters=[
#             {
#                 "name": "study_id",
#                 "description": "MTBLS Identifier",
#                 "required": True,
#                 "allowMultiple": False,
#                 "paramType": "path",
#                 "dataType": "string"
#             },
#             {
#                 "name": "user_token",
#                 "description": "User API token",
#                 "paramType": "header",
#                 "type": "string",
#                 "required": False,
#                 "allowMultiple": False
#             }
#         ],
#         responseMessages=[
#             {
#                 "code": 200,
#                 "message": "OK."
#             },
#             {
#                 "code": 400,
#                 "message": "Bad Request. Server could not understand the request due to malformed syntax."
#             },
#             {
#                 "code": 401,
#                 "message": "Unauthorized. Access to the resource requires user authentication."
#             },
#             {
#                 "code": 403,
#                 "message": "Forbidden. Access to the study is not allowed for this user."
#             },
#             {
#                 "code": 404,
#                 "message": "Not found. The requested identifier is not valid or does not exist."
#             }
#         ]
#     )
#     def get(self, study_id):
#         # param validation
#         if study_id is None:
#             abort(404)
#         # User authentication
#         user_token = None
#         if "user_token" in request.headers:
#             user_token = request.headers["user_token"]
#
#         logger.info('Getting Study sources for %s, using API-Key %s', study_id, user_token)
#         # check for access rights
#         if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
#             abort(403)
#         isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token)
#         isa_sources_names = list()
#         for source in isa_study.sources:
#             isa_sources_names.append({'name': source.name})
#         logger.debug('Got %s', isa_sources_names)
#         return jsonify({"sources": isa_sources_names})
#
#
# class StudySource(Resource):
#     @swagger.operation(
#         summary="Get Study source in a Study",
#         notes="Get Study source, by name.",
#         parameters=[
#             {
#                 "name": "study_id",
#                 "description": "MTBLS Identifier",
#                 "required": True,
#                 "allowMultiple": False,
#                 "paramType": "path",
#                 "dataType": "string"
#             },
#             {
#                 "name": "source_name",
#                 "description": "Source name",
#                 "required": True,
#                 "allowMultiple": False,
#                 "paramType": "path",
#                 "dataType": "string"
#             },
#             {
#                 "name": "user_token",
#                 "description": "User API token",
#                 "paramType": "header",
#                 "type": "string",
#                 "required": False,
#                 "allowMultiple": False
#             }
#         ],
#         responseMessages=[
#             {
#                 "code": 200,
#                 "message": "OK."
#             },
#             {
#                 "code": 400,
#                 "message": "Bad Request. Server could not understand the request due to malformed syntax."
#             },
#             {
#                 "code": 401,
#                 "message": "Unauthorized. Access to the resource requires user authentication."
#             },
#             {
#                 "code": 403,
#                 "message": "Forbidden. Access to the study is not allowed for this user."
#             },
#             {
#                 "code": 404,
#                 "message": "Not found. The requested identifier is not valid or does not exist."
#             }
#         ]
#     )
#     @marshal_with(StudySource_api_model, envelope='source')
#     def get(self, study_id, source_name):
#         # param validation
#         if study_id is None:
#             abort(404)
#         if source_name is None:
#             abort(404)
#         # User authentication
#         user_token = None
#         if "user_token" in request.headers:
#             user_token = request.headers["user_token"]
#
#         logger.info('Getting Study source %s for %s, using API-Key %s', source_name, study_id, user_token)
#         # check for access rights
#         if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
#             abort(403)
#         isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token)
#         isa_source_found = False
#         for index, source in enumerate(isa_study.sources):
#             if source.name == source_name:
#                 isa_source_found = True
#                 break
#         if not isa_source_found:
#             abort(404)
#         logger.info('Got Study source %s', source.name)
#         return source
#
#     @swagger.operation(
#         summary="Update source in a Study",
#         notes="Update source, by name.",
#         parameters=[
#             {
#                 "name": "study_id",
#                 "description": "MTBLS Identifier",
#                 "required": True,
#                 "allowMultiple": False,
#                 "paramType": "path",
#                 "dataType": "string"
#             },
#             {
#                 "name": "source_name",
#                 "description": "Source name",
#                 "required": True,
#                 "allowMultiple": False,
#                 "paramType": "path",
#                 "dataType": "string"
#             },
#             {
#                 "name": "user_token",
#                 "description": "User API token",
#                 "paramType": "header",
#                 "type": "string",
#                 "required": True,
#                 "allowMultiple": False
#             },
#             {
#                 "name": "study_source",
#                 "description": 'Update the source associated with the Study.',
#                 "paramType": "body",
#                 "type": "string",
#                 "format": "application/json",
#                 "required": True,
#                 "allowMultiple": False
#             },
#             {
#                 "name": "save_audit_copy",
#                 "description": "Keep track of changes saving a copy of the unmodified files.",
#                 "paramType": "header",
#                 "type": "Boolean",
#                 "defaultValue": True,
#                 "format": "application/json",
#                 "required": False,
#                 "allowMultiple": False
#             }
#         ],
#         responseMessages=[
#             {
#                 "code": 200,
#                 "message": "OK."
#             },
#             {
#                 "code": 400,
#                 "message": "Bad Request. Server could not understand the request due to malformed syntax."
#             },
#             {
#                 "code": 401,
#                 "message": "Unauthorized. Access to the resource requires user authentication."
#             },
#             {
#                 "code": 403,
#                 "message": "Forbidden. Access to the study is not allowed for this user."
#             },
#             {
#                 "code": 404,
#                 "message": "Not found. The requested identifier is not valid or does not exist."
#             }
#         ]
#     )
#     @marshal_with(StudySource_api_model, envelope='source')
#     def put(self, study_id, source_name):
#         # param validation
#         if study_id is None:
#             abort(404)
#         if source_name is None:
#             abort(404)
#         # User authentication
#         user_token = None
#         if "user_token" in request.headers:
#             user_token = request.headers["user_token"]
#         # body content validation
#         if request.data is None or request.json is None:
#             abort(400)
#         data_dict = json.loads(request.data.decode('utf-8'))
#         json_updated_source = data_dict['source']
#         isa_updated_source = unserialize_study_source(json_updated_source)
#         # check for keeping copies
#         save_audit_copy = False
#         save_msg_str = "NOT be"
#         if "save_audit_copy" in request.headers and request.headers["save_audit_copy"].lower() == 'true':
#             save_audit_copy = True
#             save_msg_str = "be"
#
#         logger.info('Updating Study source for %s, using API-Key %s', study_id, user_token)
#         # check for access rights
#         if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
#             abort(403)
#         isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token)
#         isa_source_found = False
#         for index, source in enumerate(isa_study.sources):
#             if source.name == source_name:
#                 isa_source_found = True
#                 isa_study.sources[index].name = isa_updated_source.name
#                 isa_study.sources[index].characteristics = list(isa_updated_source.characteristics)
#                 isa_study.sources[index].comments = list(isa_updated_source.comments)
#                 break
#         if not isa_source_found:
#             abort(404)
#         logger.info('Got Study source %s - %s', study_id, source.name)
#
#         logging.info("A copy of the previous files will %s saved", save_msg_str)
#         iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy,
#                             save_audit_assays=True, save_audit_samples=True)
#         logger.info('Updated %s - %s', study_id, isa_updated_source.name)
#         return isa_updated_source
#
#
# class StudySamples(Resource):
#     @swagger.operation(
#         summary="Get all samples in a Study",
#         notes="Get the list of samples names associated with the Study.",
#         parameters=[
#             {
#                 "name": "study_id",
#                 "description": "MTBLS Identifier",
#                 "required": True,
#                 "allowMultiple": False,
#                 "paramType": "path",
#                 "dataType": "string"
#             },
#             {
#                 "name": "user_token",
#                 "description": "User API token",
#                 "paramType": "header",
#                 "type": "string",
#                 "required": False,
#                 "allowMultiple": False
#             }
#         ],
#         responseMessages=[
#             {
#                 "code": 200,
#                 "message": "OK."
#             },
#             {
#                 "code": 400,
#                 "message": "Bad Request. Server could not understand the request due to malformed syntax."
#             },
#             {
#                 "code": 401,
#                 "message": "Unauthorized. Access to the resource requires user authentication."
#             },
#             {
#                 "code": 403,
#                 "message": "Forbidden. Access to the study is not allowed for this user."
#             },
#             {
#                 "code": 404,
#                 "message": "Not found. The requested identifier is not valid or does not exist."
#             }
#         ]
#     )
#     def get(self, study_id):
#         # param validation
#         if study_id is None:
#             abort(404)
#         # User authentication
#         user_token = None
#         if "user_token" in request.headers:
#             user_token = request.headers["user_token"]
#
#         logger.info('Getting Study samples for %s, using API-Key %s', study_id, user_token)
#         # check for access rights
#         if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
#             abort(403)
#         isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token)
#         isa_samples_names = list()
#         for samples in isa_study.samples:
#             isa_samples_names.append({'name': samples.name})
#         logger.debug('Got %s', isa_samples_names)
#         return jsonify({"samples": isa_samples_names})
#
#
# class StudySample(Resource):
#     @swagger.operation(
#         summary="Get Study sample in a Study",
#         notes="Get Study sample, by name.",
#         parameters=[
#             {
#                 "name": "study_id",
#                 "description": "MTBLS Identifier",
#                 "required": True,
#                 "allowMultiple": False,
#                 "paramType": "path",
#                 "dataType": "string"
#             },
#             {
#                 "name": "sample_name",
#                 "description": "Sample name",
#                 "required": True,
#                 "allowMultiple": False,
#                 "paramType": "path",
#                 "dataType": "string"
#             },
#             {
#                 "name": "user_token",
#                 "description": "User API token",
#                 "paramType": "header",
#                 "type": "string",
#                 "required": False,
#                 "allowMultiple": False
#             }
#         ],
#         responseMessages=[
#             {
#                 "code": 200,
#                 "message": "OK."
#             },
#             {
#                 "code": 400,
#                 "message": "Bad Request. Server could not understand the request due to malformed syntax."
#             },
#             {
#                 "code": 401,
#                 "message": "Unauthorized. Access to the resource requires user authentication."
#             },
#             {
#                 "code": 403,
#                 "message": "Forbidden. Access to the study is not allowed for this user."
#             },
#             {
#                 "code": 404,
#                 "message": "Not found. The requested identifier is not valid or does not exist."
#             }
#         ]
#     )
#     @marshal_with(StudySample_api_model, envelope='sample')
#     def get(self, study_id, sample_name):
#         # param validation
#         if study_id is None:
#             abort(404)
#         if sample_name is None:
#             abort(404)
#         # User authentication
#         user_token = None
#         if "user_token" in request.headers:
#             user_token = request.headers["user_token"]
#
#         logger.info('Getting Study sample %s for %s, using API-Key %s', sample_name, study_id, user_token)
#         # check for access rights
#         if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
#             abort(403)
#         isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token)
#         isa_sample = ''
#         for sample in isa_study.samples:
#             if sample.name == sample_name:
#                 isa_sample = sample
#         if isa_sample == '':
#             abort(404)
#         logger.info('Got %s', isa_sample)
#         return isa_sample
#
#     @swagger.operation(
#         summary="Update sample in a Study",
#         notes="Update sample, by name.",
#         parameters=[
#             {
#                 "name": "study_id",
#                 "description": "MTBLS Identifier",
#                 "required": True,
#                 "allowMultiple": False,
#                 "paramType": "path",
#                 "dataType": "string"
#             },
#             {
#                 "name": "sample_name",
#                 "description": "Sample name",
#                 "required": True,
#                 "allowMultiple": False,
#                 "paramType": "path",
#                 "dataType": "string"
#             },
#             {
#                 "name": "user_token",
#                 "description": "User API token",
#                 "paramType": "header",
#                 "type": "string",
#                 "required": True,
#                 "allowMultiple": False
#             },
#             {
#                 "name": "study_sample",
#                 "description": 'Update the sample associated with the Study.',
#                 "paramType": "body",
#                 "type": "string",
#                 "format": "application/json",
#                 "required": True,
#                 "allowMultiple": False
#             },
#             {
#                 "name": "save_audit_copy",
#                 "description": "Keep track of changes saving a copy of the unmodified files.",
#                 "paramType": "header",
#                 "type": "Boolean",
#                 "defaultValue": True,
#                 "format": "application/json",
#                 "required": False,
#                 "allowMultiple": False
#             }
#         ],
#         responseMessages=[
#             {
#                 "code": 200,
#                 "message": "OK."
#             },
#             {
#                 "code": 400,
#                 "message": "Bad Request. Server could not understand the request due to malformed syntax."
#             },
#             {
#                 "code": 401,
#                 "message": "Unauthorized. Access to the resource requires user authentication."
#             },
#             {
#                 "code": 403,
#                 "message": "Forbidden. Access to the study is not allowed for this user."
#             },
#             {
#                 "code": 404,
#                 "message": "Not found. The requested identifier is not valid or does not exist."
#             }
#         ]
#     )
#     @marshal_with(StudySample_api_model, envelope='sample')
#     def put(self, study_id, sample_name):
#         # param validation
#         if study_id is None:
#             abort(404)
#         if sample_name is None:
#             abort(404)
#         # User authentication
#         user_token = None
#         if "user_token" in request.headers:
#             user_token = request.headers["user_token"]
#         # body content validation
#         if request.data is None or request.json is None:
#             abort(400)
#         data_dict = json.loads(request.data.decode('utf-8'))
#         json_updated_sample = data_dict['sample']
#         isa_updated_sample = unserialize_study_sample(json_updated_sample)
#         # check for keeping copies
#         save_audit_copy = False
#         save_msg_str = "NOT be"
#         if "save_audit_copy" in request.headers and request.headers["save_audit_copy"].lower() == 'true':
#             save_audit_copy = True
#             save_msg_str = "be"
#
#         logger.info('Updating Study sample for %s, using API-Key %s', study_id, user_token)
#         # check for access rights
#         if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
#             abort(403)
#         isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token)
#         isa_sample_found = False
#         for index, sample in enumerate(isa_study.samples):
#             if sample.name == sample_name:
#                 isa_sample_found = True
#                 isa_study.samples[index].name = isa_updated_sample.name
#                 isa_study.samples[index].characteristics = list(isa_updated_sample.characteristics)
#                 isa_study.samples[index].derives_from = list(isa_updated_sample.derives_from)
#                 isa_study.samples[index].factor_values = list(isa_updated_sample.factor_values)
#                 isa_study.samples[index].comments = list(isa_updated_sample.comments)
#                 break
#         if not isa_sample_found:
#             abort(404)
#         logger.info('Got Study sample %s - %s', study_id, sample.name)
#
#         logging.info("A copy of the previous files will %s saved", save_msg_str)
#         iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy,
#                             save_audit_assays=True, save_audit_samples=True)
#         logger.info('Updated %s - %s', study_id, isa_updated_sample.name)
#         return isa_updated_sample
#
#
# class StudyPublicationsMM(Resource):
#     @swagger.operation(
#         summary="Get Study Publications",
#         notes="Get the list of publications associated with the Study.",
#         parameters=[
#             {
#                 "name": "study_id",
#                 "description": "MTBLS Identifier",
#                 "required": True,
#                 "allowMultiple": False,
#                 "paramType": "path",
#                 "dataType": "string"
#             },
#             {
#                 "name": "user_token",
#                 "description": "User API token",
#                 "paramType": "header",
#                 "type": "string",
#                 "required": False,
#                 "allowMultiple": False
#             }
#         ],
#         responseMessages=[
#             {
#                 "code": 200,
#                 "message": "OK."
#             },
#             {
#                 "code": 400,
#                 "message": "Bad Request. Server could not understand the request due to malformed syntax."
#             },
#             {
#                 "code": 401,
#                 "message": "Unauthorized. Access to the resource requires user authentication."
#             },
#             {
#                 "code": 403,
#                 "message": "Forbidden. Access to the study is not allowed for this user."
#             },
#             {
#                 "code": 404,
#                 "message": "Not found. The requested identifier is not valid or does not exist."
#             }
#         ]
#     )
#     # @marshal_with(StudyPublications_api_model, envelope='publications')
#     def get(self, study_id):
#         # param validation
#         if study_id is None:
#             abort(404)
#         # User authentication
#         user_token = None
#         if "user_token" in request.headers:
#             user_token = request.headers["user_token"]
#
#         logger.info('Getting Study publications for %s, using API-Key %s', study_id, user_token)
#         # check for access rights
#         if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
#             abort(403)
#         isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)
#         isa_publications = isa_study.publications
#         mm_publications = list()
#         for pub in isa_publications:
#             mm_publications.append(PublicationMM(
#                 pubmed_id=pub.pubmed_id,
#                 doi=pub.doi,
#                 author_list=pub.author_list,
#                 title=pub.title,
#                 status=self.read_status(pub.status),
#                 comments=self.read_comments(pub.comments)
#             ))
#
#         return PublicationSchema().dump(mm_publications, many=True)
#
#     def read_status(self, isa_status):
#         status = OntologyAnnotationMm(
#             annotation_value=isa_status.term,
#             term_source=isa_status.term_source,
#             term_accession=isa_status.term_accession,
#             comments=self.read_comments(isa_status.comments)
#         )
#         return status
#
#     def read_comments(self, obj_list):
#         comments = list()
#         for comment in obj_list:
#             comments.append(CommentMm(name=comment.name,
#                                       value=comment.value))
#         return comments
