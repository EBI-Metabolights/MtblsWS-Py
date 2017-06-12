import json
import logging
from flask import request, jsonify
from flask_restful import Resource, abort, marshal_with, fields
from flask_restful_swagger import swagger
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from isatools.model.v1 import *

"""
ISA Study

Manage MTBLS studies from ISA-Tab files using ISA-API

author: jrmacias@ebi.ac.uk
date: 2017-02-23
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
                "message": "OK. The a list of Studies is returned."
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
                "message": "OK. The Study is returned, ISA-JSON format."
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
                "message": "OK. The Study title is returned, JSON format."
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
                "message": "OK. The Study title is returned, JSON format."
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
                "message": "OK. The Study description is returned, JSON format."
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
                "message": "OK. The Study description is returned, JSON format."
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
                "message": "OK. The Study description is returned, JSON format."
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


# TODO ...
def unserialize_protocols(json_prot_list):
    protocol_list = list()
    for json_protocol in json_prot_list:

        if json_protocol['protocol_type'] is not None:
            protocol_type = OntologyAnnotation(term=json_protocol['protocol_type']['term'])
        else:
            protocol_type = OntologyAnnotation()

        parameters = list()
        if len(json_protocol['parameters']) > 0:
            for json_parameter in json_protocol['parameters']:
                parameters.append(ProtocolParameter(parameter_name=json_parameter['parameter_name']))

        components = list()
        if len(json_protocol['components']) > 0:
            for comp in json_protocol['components']:
                components.append(ProtocolComponent(name=comp['name']))

        comments = list()
        if len(json_protocol['comments']) > 0:
            for comment in json_protocol['comments']:
                comments.append(comment)
        else:
            comments = []

        new_protocol = Protocol(
            name=json_protocol['name'],
            protocol_type=protocol_type,
            description=json_protocol['description'],
            uri=json_protocol['uri'],
            version=json_protocol['version'],
            parameters=parameters,
            components=components,
            comments=comments
        )
        protocol_list.append(new_protocol)
    return protocol_list


def serialize_protocol(obj):
    assert isinstance(obj, Protocol)

    # name (str):
    # protocol_type (OntologyAnnotation):
    # description (str):
    # uri (str):
    # version (str):
    # parameters (list, ProtocolParameter):
    # components (list, OntologyAnnotation):
    # comments (list, str):
    return {
        'name': obj.name,
        'protocol_type': json.loads(json.dumps(obj.protocol_type, default=serialize_OntologyAnn, sort_keys=True)),
        'description': obj.description,
        'uri': obj.uri,
        'version': obj.version,
        'parameters': json.loads(json.dumps(obj.parameters, default=serialize_ProtocolParameter, sort_keys=True)),
        'components': json.loads(json.dumps(obj.components, default=serialize_OntologyAnn, sort_keys=True)),
        'comments': obj.comments
    }


def serialize_ProtocolParameter(obj):
    assert isinstance(obj, ProtocolParameter)

    # name (OntologyAnnotation): A parameter name as a term
    # unit (OntologyAnnotation): A unit, if applicable
    # comments (list, NoneType):

    unit = ''
    if hasattr(obj, 'unit') and obj.unit is not None:
        unit = serialize_OntologyAnn(obj.unit),
    return {
        'name': serialize_OntologyAnn(obj.parameter_name),
        'unit': unit,
        'comments': obj.comments
    }


def serialize_OntologySource(obj):
    assert isinstance(obj, OntologySource)

    # name (str):
    # file (str):
    # version (str):
    # description (str):
    # comments (list,):
    return {
        'name': obj.name,
        'file': obj.file,
        'version': obj.version,
        'description': obj.description,
        'comments': obj.comments
    }


def serialize_OntologyAnn(obj):
    assert isinstance(obj, OntologyAnnotation)

    # term (str, NoneType):
    # term_source (OntologySource, NoneType):
    # term_accession (str, NoneType):
    # comments (list, NoneType):
    return {
        'term': obj.term,
        'term_source': obj.term_source,
        'term_accession': obj.term_accession,
        'comments': obj.comments
    }


class StudyProtocols(Resource):
    """Manage the Study protocols"""

    @swagger.operation(
        summary="Get MTBLS Study protocols",
        notes="Get the protocols of the MTBLS Study with {study_id} in JSON format.",
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
                "message": "OK. The Study protocols is returned, JSON format."
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

        logger.info('Getting Study protocols for %s, using API-Key %s', study_id, user_token)
        protocols = iac.get_study_protocols(study_id, user_token)
        str_protocols = json.dumps({'Study-protocols': protocols}, default=serialize_protocol, sort_keys=True)
        logger.info('Got: %s', str_protocols)

        return json.loads(str_protocols)


    @swagger.operation(
        summary='Update MTBLS Study protocols',
        notes="""Update the protocols of the MTBLS Study with {study_id}.
                      Only the new protocols (in JSON format) must be provided in the body of the request.
                      i.e.: { "protocols": "Updated Study protocols..." }""",
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
                "description": """New protocols in JSON format.</br>
             i.e.: { "protocols": "New Study protocols..." }""",
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
                "message": "OK. The Study description is returned, JSON format."
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
        new_protocols = data_dict['Study-protocols']

        prots_list = unserialize_protocols(new_protocols)

        # check for keeping copies
        save_audit_copy = True
        if "save_audit_copy" in request.headers:
            save_audit_copy = request.headers["save_audit_copy"].lower() == 'true'

        # update study description
        logger.info('Updating Study protocols for %s, using API-Key %s', study_id, user_token)
        if save_audit_copy:
            logging.warning("A copy of the previous file will be saved")
        else:
            logging.warning("A copy of the previous file will NOT be saved")

        # TODO ---
        # iac.write_study_json_protocols(study_id, user_token, prots_list, save_audit_copy)
        logger.info('Applied %s', new_protocols)

        # return jsonify({"Study-protocols": new_protocols})
        return jsonify({"Study-protocols": prots_list})

