import json
import logging
from flask import request, jsonify
from flask_restful import Resource, abort, marshal_with, fields
from flask_restful_swagger import swagger
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from json import JSONEncoder
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


def encode_json(obj):
    if isinstance(obj, Protocol):
        # protocol_type
        prot_type = '{'
        prot_type += "'term':" + obj.protocol_type.term + ","
        if obj.protocol_type.term_source is not None:
            prot_type += "'term_source':" + obj.protocol_type.term_source + ","
        if len(obj.protocol_type.term_accession) > 0:
            prot_type += "'term_accession':" + obj.protocol_type.term_accession
        prot_type += '}'
        # parameters
        parameters = ''
        if len(obj.parameters) > 0:
            parameters += '{['
            for param in obj.parameters:
                parameters = "'parameter_name':" + param.parameter_name.term + ','
                if param.parameter_name.term_source is not None:
                    parameters += "'term_source':" + obj.protocol_type.term_source + ","
                if len(param.parameter_name.term_accession) > 0:
                    parameters += "'term_accession':" + obj.protocol_type.term_accession
            parameters += ']}'

        # components
        components = ''
        if len(obj.components) > 0:
            components += '{['
            for comp in obj.components:
                components = "'parameter_name':" + comp.term + ','
                if comp.term_source is not None:
                    components += "'term_source':" + comp.term_source + ","
                if len(comp.term_accession) > 0:
                    components += "'term_accession':" + comp.term_accession
            components += ']}'

        # comments
        comments = ''
        if len(obj.comments) > 0:
            comments += '{['
            for com in obj.comments:
                comments = "'parameter_name':" + com.term + ','
                comments += ']}'

        return {
            # 'id': obj.id,
            'name': obj.name,
            'protocol_type': prot_type,
            'description': obj.description,
            'uri': obj.uri,
            'version': obj.version,
            'parameters': parameters,
            'components': components,
            'comments': comments
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
        responseMessages = [
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
        logger.info('Got: %s', protocols)

        return jsonify({'Study-protocols': json.dumps(protocols, default=encode_json)})
