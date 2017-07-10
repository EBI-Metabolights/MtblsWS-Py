import logging
from flask import request, jsonify
from flask_restful import Resource, abort, marshal_with
from flask_restful_swagger import swagger
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.models import *

"""
ISA Study

Manage MTBLS studies from ISA-Tab files using ISA-API
"""

logger = logging.getLogger('wslog')
iac = IsaApiClient()
wsc = WsClient()


class StudyPubList(Resource):
    @swagger.operation(
        summary="Get All Public Studies",
        nickname="Get All Public Studies",
        notes="Get a list of all public Studies in MetaboLights.",
        responseMessages=[
            {
                "code": 200,
                "message": "OK."
            },
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax."
            }
        ]
    )
    def get(self):
        """
        Get all public Studies
        :return: a list with all the public Studies in MetaboLights
        """

        logger.info('Getting all public studies')
        pub_list = wsc.get_public_studies()
        logger.info('... found %d public studies', len(pub_list['content']))
        return jsonify(pub_list)


class Study(Resource):
    @swagger.operation(
        summary="Get ISA-JSON Study",
        nickname="Get ISA-JSON Study",
        notes="Get the MTBLS Study with {study_id} as ISA-JSON object.",
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
        """
        Get Study in ISA-JSON format
        :param study_id: MTBLS study identifier
        :return: an ISA-JSON representation of the Study
        """

        # param validation
        if study_id is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        wsc.is_study_public(study_id, user_token)

        logger.info('Getting JSON Study %s, using API-Key %s', study_id, user_token)
        isa_obj = iac.get_isa_json(study_id, user_token)
        logger.info('... found ISA-JSON obj: %s %s', isa_obj.get('title'), isa_obj.get('identifier'))
        return jsonify(isa_obj)


class StudyTitle(Resource):
    """Manage the Study title"""

    @swagger.operation(
        summary="Get MTBLS Study title",
        notes="Get the title of the MTBLS Study with {study_id} in JSON format.",
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

        wsc.is_study_public(study_id, user_token)

        logger.info('Getting Study title for %s, using API-Key %s', study_id, user_token)
        title = iac.get_study_title(study_id, user_token)
        logger.info('Got %s', title)
        return jsonify({"Study-title": title})

    @swagger.operation(
        summary='Update MTBLS Study title',
        notes="""Update the title of the MTBLS Study with {study_id}.
              Only the new title (in JSON format) must be provided in the body of the request.
              i.e.: { "title": "New Study title..." }""",
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
                "required": False,
                "allowMultiple": False
            },
            {
                "name": "title",
                "description": """New title in JSON format.</br>
                 i.e.: { "title": "New Study title..." }""",
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

        wsc.is_study_public(study_id, user_token)

        # body content validation
        if request.data is None or request.json is None:
            abort(400)
        data_dict = request.get_json(force=True)
        new_title = data_dict['title']

        # check for keeping copies
        save_audit_copy = True
        if "save_audit_copy" in request.headers:
            save_audit_copy = request.headers["save_audit_copy"].lower() == 'true'

        # update study title
        logger.info('Updating Study title for %s, using API-Key %s', study_id, user_token)
        if save_audit_copy:
            logging.warning("A copy of the previous file will be saved")
        else:
            logging.warning("A copy of the previous file will NOT be saved")
        iac.write_study_json_title(study_id, user_token, new_title, save_audit_copy)
        logger.info('Applied %s', new_title)

        return jsonify({"Study-title": new_title})


class StudyDescription(Resource):
    """Manage the Study description"""
    @swagger.operation(
        summary="Get MTBLS Study description",
        notes="Get the description of the MTBLS Study with {study_id} in JSON format.",
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

        wsc.is_study_public(study_id, user_token)

        logger.info('Getting Study description for %s, using API-Key %s', study_id, user_token)
        description = iac.get_study_description(study_id, user_token)
        logger.info('Got %s', description)
        return jsonify({"Study-description": description})

    @swagger.operation(
        summary='Update MTBLS Study description',
        notes="""Update the description of the MTBLS Study with {study_id}.
                  Only the new description (in JSON format) must be provided in the body of the request.
                  i.e.: { "description": "New Study description..." }""",
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
                "required": False,
                "allowMultiple": False
            },
            {
                "name": "description",
                "description": """New description in JSON format.</br>
                 i.e.: { "description": "New Study description..." }""",
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

        wsc.is_study_public(study_id, user_token)

        # body content validation
        if request.data is None or request.json is None:
            abort(400)
        data_dict = json.loads(request.data.decode('utf-8'))
        new_description = data_dict['description']

        # check for keeping copies
        save_audit_copy = True
        if "save_audit_copy" in request.headers:
            save_audit_copy = request.headers["save_audit_copy"].lower() == 'true'

        # update study description
        logger.info('Updating Study description for %s, using API-Key %s', study_id, user_token)
        if save_audit_copy:
            logging.warning("A copy of the previous file will be saved")
        else:
            logging.warning("A copy of the previous file will NOT be saved")
        iac.write_study_json_description(study_id, user_token, new_description, save_audit_copy)
        logger.info('Applied %s', new_description)

        return jsonify({"Study-description": new_description})


class StudyNew(Resource):
    @swagger.operation(
        summary="Create a new ISA-JSON Study",
        notes="Create a new MTBLS Study as ISA-JSON object.",
        nickname="New Study",
        parameters=[
            {
                "name": "study",
                "description": """Study in ISA-JSON format.</br>
                             i.e.: { "title": "New Study title..." }""",
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
                "required": False,
                "allowMultiple": False
            }
        ],
        responseMessages = [
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
            }
        ]
    )
    def post(self):
        """
        POST a new ISA-JSON Study 
        :return: an ISA-JSON representation of the Study
        """
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
            sub_date = data_dict['submission_date']
            pub_rel_date = data_dict['public_release_date']
        except Exception as inst:
            logger.warning('Malformed request. Some of the required fields are missing')
            abort(400)

        logger.info('Creating a new MTBLS Study as ISA-JSON object.')
        inv_obj = iac.create_new_study(title=title,
                                       description=description,
                                       sub_date=sub_date,
                                       pub_rel_date=pub_rel_date)
        logger.info('New MTBLS Study, title: %s, desc.: %s, pub.rel.date: %s',
                       title, description, pub_rel_date)

        return inv_obj


class StudyProtocols(Resource):
    """Manage the Study protocols"""

    @swagger.operation(
        summary="Get MTBLS Study protocols",
        notes="Get the list of protocols of the MTBLS Study with {study_id} in JSON format.",
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
    @marshal_with(Protocol_api_model, envelope='StudyProtocols')
    def get(self, study_id):
        # param validation
        if study_id is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        wsc.is_study_public(study_id, user_token)

        logger.info('Getting Study protocols for %s, using API-Key %s', study_id, user_token)
        isa_protocols = iac.get_study_protocols(study_id, user_token)
        str_protocols = json.dumps({'StudyProtocols': isa_protocols}, default=serialize_protocol, sort_keys=True)
        logger.info('Got: %s', str_protocols)

        return isa_protocols

    @swagger.operation(
        summary='Update MTBLS Study protocols',
        notes='Update the list of protocols of the MTBLS Study with {study_id}.',
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
                "required": False,
                "allowMultiple": False
            },
            {
                "name": "protocols",
                "description": 'Updated list of protocols in JSON format.',
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
    @marshal_with(Protocol_api_model, envelope='StudyProtocols')
    def put(self, study_id):
        # param validation
        if study_id is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        wsc.is_study_public(study_id, user_token)

        # body content validation
        if request.data is None or request.json is None:
            abort(400)
        data_dict = json.loads(request.data.decode('utf-8'))
        json_protocols = data_dict['StudyProtocols']

        isa_protocols = list()
        for json_protocol in json_protocols:
            isa_protocol = unserialize_protocol(json_protocol)
            isa_protocols.append(isa_protocol)

        # check for keeping copies
        save_audit_copy = True
        if "save_audit_copy" in request.headers:
            save_audit_copy = request.headers["save_audit_copy"].lower() == 'true'

        # update study protocols
        logger.info('Updating Study protocols for %s, using API-Key %s', study_id, user_token)
        if save_audit_copy:
            logging.warning("A copy of the previous file will be saved")
        else:
            logging.warning("A copy of the previous file will NOT be saved")

        iac.write_study_json_protocols(study_id, user_token, isa_protocols, save_audit_copy)
        logger.info('Applied %s', json_protocols)

        # return jsonify({"Study-protocols": json_protocols})
        return isa_protocols


class StudyContacts(Resource):
    """A person/contact that can be attributed to an Investigation or Study.

        Attributes:
            last_name (str, NoneType): The last name of a person associated with the investigation.
            first_name (str, NoneType): The first name of a person associated with the investigation.
            mid_initials (str, NoneType): The middle initials of a person associated with the investigation.
            email (str, NoneType): The email address of a person associated with the investigation.
            phone (str, NoneType): The telephone number of a person associated with the investigation.
            fax (str, NoneType): The fax number of a person associated with the investigation.
            address (str, NoneType): The address of a person associated with the investigation.
            affiliation (str, NoneType): The organization affiliation for a person associated with the investigation.
            roles (list, NoneType): OntologyAnnotations to classify the role(s) performed by this person in the context of
            the investigation, which means that the roles reported here need not correspond to roles held withing their
            affiliated organization.
            comments (list, NoneType): Comments associated with instances of this class.
        """

    @swagger.operation(
        summary="Get MTBLS Study Contacts",
        notes="Get the list of People/contacts associated with the Study.",
        # responseClass=StudyContact.__name__, multiValuedResponse=True, responseContainer="List",
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
    @marshal_with(Person_api_model, envelope='StudyContacts')
    def get(self, study_id):

        # param validation
        if study_id is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        wsc.is_study_public(study_id, user_token)

        logger.info('Getting Study contacts for %s, using API-Key %s', study_id, user_token)
        isa_contacts = iac.get_study_contacts(study_id, user_token)
        str_contacts = json.dumps({'StudyContacts': isa_contacts}, default=serialize_person, sort_keys=True)
        logger.info('Got %s', str_contacts)

        return isa_contacts

    @swagger.operation(
        summary='Update MTBLS Study contacts',
        notes='Update the list of People/contacts associated with the Study.',
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
                "required": False,
                "allowMultiple": False
            },
            {
                "name": "contacts",
                "description": 'Updated list of People/contacts in JSON format.',
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
    @marshal_with(Person_api_model, envelope='StudyContacts')
    def put(self, study_id):
        # param validation
        if study_id is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        wsc.is_study_public(study_id, user_token)

        # body content validation
        if request.data is None or request.json is None:
            abort(400)
        data_dict = json.loads(request.data.decode('utf-8'))
        json_contacts = data_dict['StudyContacts']

        isa_contacts = list()
        for json_contact in json_contacts:
            isa_contact = unserialize_person(json_contact)
            isa_contacts.append(isa_contact)

        # check for keeping copies
        save_audit_copy = True
        if "save_audit_copy" in request.headers:
            save_audit_copy = request.headers["save_audit_copy"].lower() == 'true'

        # update study contacts
        logger.info('Updating Study contacts for %s, using API-Key %s', study_id, user_token)
        if save_audit_copy:
            logging.warning("A copy of the previous file will be saved")
        else:
            logging.warning("A copy of the previous file will NOT be saved")

        iac.write_study_json_contacts(study_id, user_token, isa_contacts, save_audit_copy)
        logger.info('Applied %s', json_contacts)

        return isa_contacts


class StudyFactors(Resource):
    @swagger.operation(
        summary="Get MTBLS Study Factors",
        notes="Get the list of independent variables (factors) associated with the Study.",
        # responseClass=StudyFactor_api_model, multiValuedResponse=True, responseContainer="List",
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
    @marshal_with(StudyFactor_api_model, envelope='StudyFactors')
    def get(self, study_id):

        # param validation
        if study_id is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        wsc.is_study_public(study_id, user_token)

        logger.info('Getting Study factors for %s, using API-Key %s', study_id, user_token)
        isa_factors = iac.get_study_factors(study_id, user_token)
        str_factors = json.dumps({'StudyFactors': isa_factors}, default=serialize_study_factor, sort_keys=True)
        logger.info('Got %s', str_factors)

        return isa_factors

    @swagger.operation(
        summary='Update MTBLS Study factors',
        notes='Update the list of independent variables (factors) associated with the Study.',
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
                "required": False,
                "allowMultiple": False
            },
            {
                "name": "contacts",
                "description": 'Updated list of independent variables (factors) in JSON format.',
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
    @marshal_with(StudyFactor_api_model, envelope='StudyFactors')
    def put(self, study_id):
        # param validation
        if study_id is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        wsc.is_study_public(study_id, user_token)

        # body content validation
        if request.data is None or request.json is None:
            abort(400)
        data_dict = json.loads(request.data.decode('utf-8'))
        json_factors = data_dict['StudyFactors']

        isa_factors = list()
        for json_factor in json_factors:
            isa_factor = unserialize_study_factor(json_factor)
            isa_factors.append(isa_factor)

        # check for keeping copies
        save_audit_copy = True
        if "save_audit_copy" in request.headers:
            save_audit_copy = request.headers["save_audit_copy"].lower() == 'true'

        # update study factors
        logger.info('Updating Study factors for %s, using API-Key %s', study_id, user_token)
        if save_audit_copy:
            logging.warning("A copy of the previous file will be saved")
        else:
            logging.warning("A copy of the previous file will NOT be saved")

        iac.write_study_json_factors(study_id, user_token, isa_factors, save_audit_copy)
        logger.info('Applied %s', json_factors)

        return isa_factors


class StudyDescriptors(Resource):
    @swagger.operation(
        summary="Get MTBLS Study Descriptors",
        notes="Get the list of design descriptors associated with the Study.",
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
    @marshal_with(OntologyAnnotation_api_model, envelope='StudyDescriptors')
    def get(self, study_id):

        # param validation
        if study_id is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        wsc.is_study_public(study_id, user_token)

        logger.info('Getting Study design descriptors for %s, using API-Key %s', study_id, user_token)
        isa_descriptors = iac.get_study_descriptors(study_id, user_token)
        str_descriptors = json.dumps({'StudyDescriptors': isa_descriptors}, default=serialize_ontology_annotation,
                                     sort_keys=True)
        logger.info('Got %s', str_descriptors)

        return isa_descriptors

    @swagger.operation(
        summary='Update MTBLS Study descriptors',
        notes='Update the list of design descriptors associated with the Study.',
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
                "required": False,
                "allowMultiple": False
            },
            {
                "name": "contacts",
                "description": 'Update the list of design descriptors associated with the Study.',
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
    @marshal_with(OntologyAnnotation_api_model, envelope='StudyDescriptors')
    def put(self, study_id):
        # param validation
        if study_id is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        wsc.is_study_public(study_id, user_token)

        # body content validation
        if request.data is None or request.json is None:
            abort(400)
        data_dict = json.loads(request.data.decode('utf-8'))
        json_descriptors = data_dict['StudyDescriptors']

        isa_descriptors = list()
        for json_descriptor in json_descriptors:
            isa_descriptor = unserialize_ontology_annotation(json_descriptor)
            isa_descriptors.append(isa_descriptor)

        # check for keeping copies
        save_audit_copy = True
        if "save_audit_copy" in request.headers:
            save_audit_copy = request.headers["save_audit_copy"].lower() == 'true'

        # update study factors
        logger.info('Updating Study descriptors for %s, using API-Key %s', study_id, user_token)
        if save_audit_copy:
            logging.warning("A copy of the previous file will be saved")
        else:
            logging.warning("A copy of the previous file will NOT be saved")

        iac.write_study_json_descriptors(study_id, user_token, isa_descriptors, save_audit_copy)
        logger.info('Applied %s', json_descriptors)

        return isa_descriptors
