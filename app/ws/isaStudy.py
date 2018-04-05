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
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
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

        if obj_name:
            # return a single object
            obj = isa_study.get_prot(obj_name)
            if not obj:
                abort(404)
            logger.info('Got %s', obj.name)
            return ProtocolSchema().dump(obj)
        else:
            # return a list of objects
            logger.info('Got %s protocols', len(isa_study.protocols))
            return ProtocolSchema().dump(isa_study.protocols, many=True)

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
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
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
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
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

        if obj_name:
            # return a single object
            obj = isa_study.get_factor(obj_name)
            if not obj:
                abort(404)
            logger.info('Got %s', obj.name)
            return StudyFactorSchema().dump(obj)
        else:
            # return a list of objects
            logger.info('Got %s protocols', len(isa_study.factors))
            return StudyFactorSchema().dump(isa_study.factors, many=True)

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
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
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
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
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
                "name": "term",
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
        parser.add_argument('term', help='Design Descriptor value')
        obj_term = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_term = args['term']

        logger.info('Getting Study Design Descriptors for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=True)

        if not obj_term:
            # return a list of objs
            logger.info('Got %s descriptors', len(isa_study.design_descriptors))
            return StudyDesignDescriptorSchema().dump(isa_study.design_descriptors, many=True)
        else:
            # return a single obj
            found = False
            for index, obj in enumerate(isa_study.design_descriptors):
                if obj.term == obj_term:
                    found = True
                    break
            if not found:
                abort(404)
            logger.info('Got %s', obj.term)
            return StudyDesignDescriptorSchema().dump(obj)

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
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
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
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
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
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
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

        if obj_title is None:
            # return a list of publications
            logger.info('Got %s publications', len(isa_study.publications))
            return PublicationSchema().dump(isa_study.publications, many=True)
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
            return PublicationSchema().dump(obj)

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
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
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
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_audit_copy)
        logger.info('Updated %s', updated_publication.title)

        return PublicationSchema().dump(updated_publication)


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
        parser.add_argument('name', help='Study Source name')
        obj_name = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name']

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


class StudySources(Resource):

    # @swagger.operation(
    #     summary='Add new Study Source',
    #     notes='Add new Study Source to a Study.',
    #     parameters=[
    #         {
    #             "name": "study_id",
    #             "description": "MTBLS Identifier",
    #             "required": True,
    #             "allowMultiple": False,
    #             "paramType": "path",
    #             "dataType": "string"
    #         },
    #         {
    #             "name": "user_token",
    #             "description": "User API token",
    #             "paramType": "header",
    #             "type": "string",
    #             "required": True,
    #             "allowMultiple": False
    #         },
    #         {
    #             "name": "source",
    #             "description": 'Study Source in ISA-JSON format.',
    #             "paramType": "body",
    #             "type": "string",
    #             "format": "application/json",
    #             "required": True,
    #             "allowMultiple": False
    #         },
    #         {
    #             "name": "save_audit_copy",
    #             "description": "Keep track of changes saving a copy of the unmodified files.",
    #             "paramType": "header",
    #             "type": "Boolean",
    #             "defaultValue": True,
    #             "format": "application/json",
    #             "required": False,
    #             "allowMultiple": False
    #         }
    #     ],
    #     responseMessages=[
    #         {
    #             "code": 200,
    #             "message": "OK."
    #         },
    #         {
    #             "code": 400,
    #             "message": "Bad Request. Server could not understand the request due to malformed syntax."
    #         },
    #         {
    #             "code": 401,
    #             "message": "Unauthorized. Access to the resource requires user authentication."
    #         },
    #         {
    #             "code": 403,
    #             "message": "Forbidden. Access to the study is not allowed for this user."
    #         },
    #         {
    #             "code": 404,
    #             "message": "Not found. The requested identifier is not valid or does not exist."
    #         },
    #         {
    #             "code": 409,
    #             "message": "Conflict. The request could not be completed due to a conflict"
    #                        " with the current state of study. This is usually issued to prevent duplications."
    #         }
    #     ]
    # )
    def post(self, study_id):
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help="Study Source name")
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
            data = data_dict['source']
            # if partial=True missing fields will be ignored
            result = SourceSchema().load(data, partial=False)
            new_obj = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # Add new Study Source
        logger.info('Adding new Source %s to %s, using API-Key %s',
                    new_obj.name, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        obj_list = isa_study.sources
        # check for Study Source added already
        for index, obj in enumerate(obj_list):
            if obj.name == new_obj.name:
                abort(409)
        # add Study Source
        obj_list.append(new_obj)
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path,
                            save_investigation_copy=save_audit_copy,
                            save_samples_copy=True, save_assays_copy=True)
        logger.info('Added %s', new_obj.name)

        return SourceSchema().dump(new_obj)

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
        parser.add_argument('name', help='Study Source name')
        obj_name = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name']

        logger.info('Getting Study Sources for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        obj_list = isa_study.sources
        if obj_name is None:
            # return a list of objs
            logger.info('Got %s sources', len(obj_list))
            return SourceSchema().dump(obj_list, many=True)
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
            return SourceSchema().dump(obj)

    # @swagger.operation(
    #     summary='Delete Study Source',
    #     notes="""Delete Study Source.
    #           <br>
    #           Use source name as a query parameter to filter out.""",
    #     parameters=[
    #         {
    #             "name": "study_id",
    #             "description": "MTBLS Identifier",
    #             "required": True,
    #             "allowMultiple": False,
    #             "paramType": "path",
    #             "dataType": "string"
    #         },
    #         {
    #             "name": "name",
    #             "description": "Study Source name",
    #             "required": True,
    #             "allowEmptyValue": False,
    #             "allowMultiple": False,
    #             "paramType": "query",
    #             "dataType": "string"
    #         },
    #         {
    #             "name": "user_token",
    #             "description": "User API token",
    #             "paramType": "header",
    #             "type": "string",
    #             "required": True,
    #             "allowMultiple": False
    #         },
    #         {
    #             "name": "save_audit_copy",
    #             "description": "Keep track of changes saving a copy of the unmodified files.",
    #             "paramType": "header",
    #             "type": "Boolean",
    #             "defaultValue": True,
    #             "format": "application/json",
    #             "required": False,
    #             "allowMultiple": False
    #         }
    #     ],
    #     responseMessages=[
    #         {
    #             "code": 200,
    #             "message": "OK."
    #         },
    #         {
    #             "code": 400,
    #             "message": "Bad Request. Server could not understand the request due to malformed syntax."
    #         },
    #         {
    #             "code": 401,
    #             "message": "Unauthorized. Access to the resource requires user authentication."
    #         },
    #         {
    #             "code": 403,
    #             "message": "Forbidden. Access to the study is not allowed for this user."
    #         },
    #         {
    #             "code": 404,
    #             "message": "Not found. The requested identifier is not valid or does not exist."
    #         }
    #     ]
    # )
    def delete(self, study_id):
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        parser = reqparse.RequestParser()
        parser.add_argument('name', help="Study Source name", location="args")
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

        # delete source
        logger.info('Deleting Study Source %s for %s, using API-Key %s', obj_name, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        obj_list = isa_study.sources
        found = False
        for index, obj in enumerate(obj_list):
            if obj.name == obj_name:
                found = True
                # ToDo check if there are dependent objs (samples, other_materials, etc.)
                # delete Study Source
                del obj_list[index]
                break
        if not found:
            abort(404)
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path,
                            save_investigation_copy=save_audit_copy,
                            save_samples_copy=True, save_assays_copy=True)
        logger.info('Deleted %s', obj.name)

        return SourceSchema().dump(obj)

    # @swagger.operation(
    #     summary='Update Study Source',
    #     notes="""Update Study Source.
    #           <br>
    #           Use source name as a query parameter to filter out.""",
    #     parameters=[
    #         {
    #             "name": "study_id",
    #             "description": "MTBLS Identifier",
    #             "required": True,
    #             "allowMultiple": False,
    #             "paramType": "path",
    #             "dataType": "string"
    #         },
    #         {
    #             "name": "name",
    #             "description": "Study Source name",
    #             "required": True,
    #             "allowEmptyValue": False,
    #             "allowMultiple": False,
    #             "paramType": "query",
    #             "dataType": "string"
    #         },
    #         {
    #             "name": "user_token",
    #             "description": "User API token",
    #             "paramType": "header",
    #             "type": "string",
    #             "required": True,
    #             "allowMultiple": False
    #         },
    #         {
    #             "name": "protocol",
    #             "description": 'Study Source in ISA-JSON format.',
    #             "paramType": "body",
    #             "type": "string",
    #             "format": "application/json",
    #             "required": True,
    #             "allowMultiple": False
    #         },
    #         {
    #             "name": "save_audit_copy",
    #             "description": "Keep track of changes saving a copy of the unmodified files.",
    #             "paramType": "header",
    #             "type": "Boolean",
    #             "defaultValue": True,
    #             "format": "application/json",
    #             "required": False,
    #             "allowMultiple": False
    #         }
    #     ],
    #     responseMessages=[
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
    def put(self, study_id):
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
        for index, obj in enumerate(obj_list):
            if obj.name == obj_name:
                found = True
                # update source details
                obj_list[index] = updated_obj

                obj_list[index].name = updated_obj.name
                obj_list[index].characteristics = list(updated_obj.characteristics)
                obj_list[index].comments = list(updated_obj.comments)

                break
        if not found:
            abort(404)
        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path,
                            save_investigation_copy=save_audit_copy,
                            save_samples_copy=True, save_assays_copy=True)
        logger.info('Updated %s', updated_obj.name)

        return SourceSchema().dump(updated_obj)


class StudySamples(Resource):

    # @swagger.operation(
    #     summary='Add new Study Sample',
    #     notes='Add new Study Sample to a Study.',
    #     parameters=[
    #         {
    #             "name": "study_id",
    #             "description": "MTBLS Identifier",
    #             "required": True,
    #             "allowMultiple": False,
    #             "paramType": "path",
    #             "dataType": "string"
    #         },
    #         {
    #             "name": "user_token",
    #             "description": "User API token",
    #             "paramType": "header",
    #             "type": "string",
    #             "required": True,
    #             "allowMultiple": False
    #         },
    #         {
    #             "name": "sample",
    #             "description": 'Study Sample in ISA-JSON format.',
    #             "paramType": "body",
    #             "type": "string",
    #             "format": "application/json",
    #             "required": True,
    #             "allowMultiple": False
    #         },
    #         {
    #             "name": "save_audit_copy",
    #             "description": "Keep track of changes saving a copy of the unmodified files.",
    #             "paramType": "header",
    #             "type": "Boolean",
    #             "defaultValue": True,
    #             "format": "application/json",
    #             "required": False,
    #             "allowMultiple": False
    #         }
    #     ],
    #     responseMessages=[
    #         {
    #             "code": 200,
    #             "message": "OK."
    #         },
    #         {
    #             "code": 400,
    #             "message": "Bad Request. Server could not understand the request due to malformed syntax."
    #         },
    #         {
    #             "code": 401,
    #             "message": "Unauthorized. Access to the resource requires user authentication."
    #         },
    #         {
    #             "code": 403,
    #             "message": "Forbidden. Access to the study is not allowed for this user."
    #         },
    #         {
    #             "code": 404,
    #             "message": "Not found. The requested identifier is not valid or does not exist."
    #         },
    #         {
    #             "code": 409,
    #             "message": "Conflict. The request could not be completed due to a conflict"
    #                        " with the current state of study. This is usually issued to prevent duplications."
    #         }
    #     ]
    # )
    def post(self, study_id):
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
        new_obj = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['sample']
            # if partial=True missing fields will be ignored
            result = SampleSchema().load(data, partial=False)
            new_obj = result.data
        except (ValidationError, Exception) as err:
            abort(400)

        # Add new Study Sample
        logger.info('Adding new Sample %s to %s, using API-Key %s',
                    new_obj.name, study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_WRITE]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        obj_list = isa_study.samples
        # check for Study Sample added already
        for index, obj in enumerate(obj_list):
            if obj.name == new_obj.name:
                abort(409)

        # add Study Sample
        pre_source = isa_study.sources[0]
        source = Source(name='New-Source',
                        characteristics=pre_source.characteristics,
                        comments=[Comment(name='created', value='Created with MtblsWs-Py')]
                        )
        protocol = isa_study.get_prot('Sample collection')
        pre_sample = isa_study.samples[0]
        sample = Sample(name='New-Sample',
                        factor_values=pre_sample.factor_values,
                        characteristics=pre_sample.characteristics,
                        derives_from=source,
                        comments=[Comment(name='created', value='Created with MtblsWs-Py')]
                        )
        new_obj = Process(name='New-Process',
                          executes_protocol=protocol,
                          date_=None,
                          performer=None,
                          parameter_values=None,
                          inputs=[source],
                          outputs=[sample],
                          comments=[Comment(name='created', value='Created with MtblsWs-Py')]
                          )

        # obj_list.append(new_obj)
        isa_study.process_sequence.append(new_obj)

        logging.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path,
                            save_investigation_copy=save_audit_copy,
                            save_samples_copy=True, save_assays_copy=True)
        logger.info('Added %s', new_obj.name)

        return ProcessSchema().dump(new_obj)

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
        parser.add_argument('name', help='Study Sample name')
        obj_name = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name']

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


class StudyOtherMaterials(Resource):

    @swagger.operation(
        summary="Get Study Other Materials",
        notes="""Get Study Other Materials.
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
                "description": "Study Material name",
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
        parser.add_argument('name', help='Study Material name')
        obj_name = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name']

        logger.info('Getting Other Materials for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        if not wsc.get_permisions(study_id, user_token)[wsc.CAN_READ]:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token, skip_load_tables=False)

        obj_list = isa_study.other_material
        # Using context to avoid envelop tags in contained objects
        sch = MaterialSchema()
        sch.context['other_material'] = Material()
        if obj_name is None:
            # return a list of objs
            logger.info('Got %s Materials', len(obj_list))
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
