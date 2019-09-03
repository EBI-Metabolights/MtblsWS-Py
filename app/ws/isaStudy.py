#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-May-23
#  Modified by:   kenneth
#
#  Copyright 2019 EMBL - European Bioinformatics Institute
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

from flask import request, abort, jsonify
from flask_restful import Resource, marshal_with, reqparse
from marshmallow import ValidationError
from app.ws import utils
from app.ws.isaApiClient import IsaApiClient
from app.ws.mm_models import *
from app.ws.mtblsWSclient import WsClient
from app.ws.models import *
from flask_restful_swagger import swagger
from app.ws.utils import log_request, add_ontology_to_investigation, read_tsv
from app.ws.db_connection import study_submitters, update_release_date
import logging
import os
import datetime

logger = logging.getLogger('wslog')
iac = IsaApiClient()
wsc = WsClient()


def extended_response(data=None, errs=None, warns=None):
    ext_resp = {"data": data if data else list(),
                "errors": errs if errs else list(),
                "warnings": warns if warns else list()}
    return ext_resp


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

        logger.info('Getting ISA-JSON Study %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        isa_obj = iac.get_isa_json(study_id, user_token, study_location=study_location)
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
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info('Getting Study title for %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
        study_status = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

        title = isa_study.title
        logger.info('Got %s', title)
        return jsonify({"title": title})

    @swagger.operation(
        summary='Update Study Title',
        notes="""Update the title of a Study.</p><pre><code> 
{ 
    \"title\": \"New title of your study. Use publication title if possible\" 
}

</code></pre>
              """,
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
        logger.info('Updating Study title for %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)
        isa_study.title = new_title
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        status, message = wsc.reindex_study(study_id, user_token)
        logger.info('Applied %s', new_title)
        return jsonify({"title": new_title})


class StudyReleaseDate(Resource):

    @swagger.operation(
        summary='Update study release date',
        notes="""Update the release date of a study.</p><pre><code> 
{ 
    \"release_date\": \"2019-05-15\" 
}

</code></pre>
              """,
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
                "name": "release_date",
                "description": "Release date",
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
        new_date = data_dict['release_date']

        try:
            datetime.datetime.strptime(new_date, '%Y-%m-%d')
        except ValueError:
            abort(406, "Incorrect date format, please use YYYY-MM-DD")

        # check for keeping copies
        save_audit_copy = False
        save_msg_str = "NOT be"
        if "save_audit_copy" in request.headers and request.headers["save_audit_copy"].lower() == 'true':
            save_audit_copy = True
            save_msg_str = "be"

        # update study title
        logger.info('Updating Study title for %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)
        isa_inv.public_release_date = new_date
        isa_study.public_release_date = new_date
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        # update database
        update_release_date(study_id, new_date)
        status, message = wsc.reindex_study(study_id, user_token)
        logger.info('Applied %s', new_date)
        return jsonify({"release_date": new_date})


class StudyMetaInfo(Resource):
    @swagger.operation(
        summary="Get Study Release Date and Status",
        notes="Get Study Release Date and Status.",
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
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info('Getting Study details for %s, using API-Key %s', study_id, user_token)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)
        return jsonify({"data": ["status:"+study_status, "release-date:"+release_date]})


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
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info('Getting Study description for %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)
        description = isa_study.description
        logger.info('Got %s', description)
        return jsonify({"description": description})

    @swagger.operation(
        summary='Update Study Description',
        notes="""Update the description of a Study.</p><pre><code> 
{ 
    \"description\": \"The description of your study. Please use the abstract from your paper if possible\" 
}

</code></pre>""",
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
        logger.info('Updating Study description for %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)
        isa_study.description = new_description
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        status, message = wsc.reindex_study(study_id, user_token)
        logger.info('Applied %s', new_description)
        return jsonify({"description": new_description})


def roles_to_contacts(isa_inv, new_contact):

    # Check that the ontology is referenced in the investigation
    isa_inv, efo = add_ontology_to_investigation(isa_inv, 'EFO', '132',
                                                 'http://data.bioontology.org/ontologies/EFO',
                                                 'Experimental Factor Ontology')
    new_role = OntologyAnnotation(
        term_accession='http://purl.obolibrary.org/obo/NCIT_C51826', term='Investigator', term_source=efo)

    append_role = False

    if len(new_contact.roles) < 1:  # the role is missing, default to Investigator
        append_role = True
        logger.warning("Role was not defined, defaulting to 'Investigator' for " +
                       new_contact.first_name + " " + new_contact.last_name)
    elif new_contact.roles:  # We have the role
        role = new_contact.roles[0]
        if not role.term or len(role.term) <= 2:  # the annotation value is missing
            del new_contact.roles[0]  # Remove role with missing annotation
            append_role = True

    if append_role:
        new_contact.roles.append(new_role)
    return isa_inv, new_contact


class StudyContacts(Resource):
    @swagger.operation(
        summary='Add new Study Contact',
        notes='''Add new Contact to a Study. <pre><code>
{ 
  "contacts": [
    {
      "firstName": "Joe",
      "lastName": "Blogs",
      "email": "joe.blogs@cam.ac.uk",
      "affiliation": "University of Cambridge",
      "address": "The Department of Biochemistry, The Sanger Building, 80 Tennis Court Road, Cambridge, CB2 1GA, UK.",
      "fax": "01223123456",
      "midInitials": "A",
      "phone": "01223234567",
      "roles": [
        {
          "termAccession": "http://purl.obolibrary.org/obo/NCIT_C51826",
          "annotationValue": "Grant Principal Investigator",
          "termSource": {
            "file": "http://data.bioontology.org/ontologies/EFO",
            "name": "",
            "version": "132"
          }
        }
      ]
    } 
  ]
}

</code></pre>''',
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

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

        contact_persons = []

        for contact in isa_study.contacts:
            contact_persons.append((contact.first_name+contact.last_name).lower())

        # body content validation
        new_contacts = []
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['contacts']
            # if partial=True missing fields will be ignored

            for contact in data:
                # Add new contact
                result = PersonSchema().load(contact, partial=False)
                new_contact = result.data
                logger.info('Adding new Contact %s for %s', new_contact.first_name, study_id)
                if (new_contact.first_name+new_contact.last_name).lower() not in contact_persons:
                    # Check that the ontology is referenced in the investigation
                    isa_inv, new_contact = roles_to_contacts(isa_inv, new_contact)
                    term_anno = new_contact.roles[0]
                    term_source = term_anno.term_source
                    new_contacts.append(new_contact)
                    add_ontology_to_investigation(isa_inv, term_source.name, term_source.version,
                                                  term_source.file, term_source.description)
        except (ValidationError, Exception) as e:
            logger.error(e)
            abort(400)

        # add contact
        isa_study.contacts = isa_study.contacts + new_contacts

        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        status, message = wsc.reindex_study(study_id, user_token)

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
        summary="Get Study Contacts",
        notes="""Get Contacts associated with a Study.
              <br>
              Use contact's email or name as parameter to get a specific contact.""",
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
                "required": True,
                "allowMultiple": False
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
                "name": "full_name",
                "description": "Contact's first and last name, concatenated without any extra characters",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
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

        parser = reqparse.RequestParser()
        parser.add_argument('email', help="Contact's email")
        parser.add_argument('full_name', help="Contact's first and last name")
        email = None
        full_name = None
        if request.args:
            args = parser.parse_args(req=request)
            email = args['email']
            full_name = args['full_name']

        logger.info('Getting Contacts %s for Study %s', email, study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

        obj_list = isa_study.contacts
        # Using context to avoid envelop tags in contained objects
        sch = PersonSchema()
        sch.context['contact'] = Person()
        if email is None and full_name is None:
            # return a list of objs
            logger.info('Got %s contacts', len(obj_list))
            return sch.dump(obj_list, many=True)
        else:
            # return a single obj
            logger.info('Contact full_name' + full_name)
            found = False
            for index, obj in enumerate(obj_list):
                if obj.email == email or obj.first_name + obj.last_name == full_name:
                    found = True
                    break

            if not found:
                abort(404)
            logger.info('Got %s', obj.email)
            return sch.dump(obj)

    @swagger.operation(
        summary='Update Study Contact',
        notes='''Update Contact associated with a Study.
              <br>
              <b>Use contact's email or full name as a parameter to update a single contact.</b><pre><code>
{ 
  "contacts": [
      {
        "firstName": "Joe",
        "lastName": "Blogs",
        "email": "joe.blogs@cam.ac.uk",
        "affiliation": "University of Cambridge",
        "address": "The Department of Biochemistry, The Sanger Building, 80 Tennis Court Road, Cambridge, CB2 1GA, UK.",
        "fax": "01223123456",
        "midInitials": "A",
        "phone": "01223234567",
        "roles": [
          {
            "termAccession": "http://purl.obolibrary.org/obo/NCIT_C51826",
            "annotationValue": "Grant Principal Investigator",
            "termSource": {
              "file": "http://data.bioontology.org/ontologies/EFO",
              "name": "",
              "version": "132"
            }
          }
        ]
      }
  ] 
}

</code></pre>''',
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
                "name": "email",
                "description": "Contact's email",
                "required": False,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "full_name",
                "description": "Contact's first and last name, concatenated without any extra characters",
                "required": False,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
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
        parser.add_argument('full_name', help="Contact's first and last name")
        args = parser.parse_args()
        email = args['email']
        full_name = args['full_name']
        if email is None and full_name is None:
            abort(404)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            # user token is required
            abort(401)

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        # check for keeping copies
        save_audit_copy = False
        save_msg_str = "NOT be"
        if "save_audit_copy" in request.headers and \
                request.headers["save_audit_copy"].lower() == 'true':
            save_audit_copy = True
            save_msg_str = "be"

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

        # body content validation
        updated_contact = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['contacts']
            for contact in data:
                # if partial=True missing fields will be ignored
                result = PersonSchema().load(contact, partial=True)
                updated_contact = result.data

                # Check that the ontology is referenced in the investigation
                isa_inv, updated_contact = roles_to_contacts(isa_inv, updated_contact)
                term_anno = updated_contact.roles[0]
                term_source = term_anno.term_source
                add_ontology_to_investigation(isa_inv, term_source.name, term_source.version,
                                              term_source.file, term_source.description)

        except (ValidationError, Exception):
            abort(400)

        # update contact details
        logger.info('Updating Contact details for %s', study_id)

        person_found = False
        if (email and len(email) > 3) or (full_name and len(full_name) > 3):
            for index, person in enumerate(isa_study.contacts):
                if person.email == email or person.first_name + person.last_name == full_name:
                    person_found = True
                    # update person details
                    isa_study.contacts[index] = updated_contact
                    break

            if not person_found:
                abort(404)
            logger.info("A copy of the previous files will %s saved", save_msg_str)
            iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
            status, message = wsc.reindex_study(study_id, user_token)
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "email",
                "description": "Contact's email",
                "required": False,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "full_name",
                "description": "Contact's first and last name, concatenated without any extra characters",
                "required": False,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
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
        parser.add_argument('full_name', help="Contact's first and last name")
        args = parser.parse_args()
        email = args['email']
        full_name = args['full_name']
        if email is None and full_name is None:
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
        logger.info('Deleting contact %s for %s', email, study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)
        person_found = False
        for index, person in enumerate(isa_study.contacts):
            if person.email == email or person.first_name + person.last_name == full_name:
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
        notes='''Add a new Protocol to a Study.
<pre><code>
{
  "protocol": {
      "name": "Chromatography",
      "protocolType": {
        "annotationValue": "Chromatography",
        "termSource": null,
        "termAccession": ""
      },
      "description": "Describe your chromatography.....",
      "version": "",
      "parameters": [
        {
          "parameterName": {
            "annotationValue": "Chromatography Instrument",
            "termSource": null,
            "termAccession": ""
          }
        },
        {
          "parameterName": {
            "annotationValue": "Column type",
            "termSource": null,
            "termAccession": ""
          }
        },
        {
          "parameterName": {
            "annotationValue": "Column model",
            "termSource": null,
            "termAccession": ""
          }
        }
      ]
  }
}
</pre></code>''',
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
        obj_name = args['name'].lower() if args['name'] else None
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

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

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
        except (ValidationError, Exception):
            abort(400)

        # TODO, use new utils.add_protcol method
        # Add new protocol
        logger.info('Adding new Protocol %s for %s', new_obj.name, study_id)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "name",
                "description": "Protocol name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
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
            obj_name = args['name'].lower() if args['name'] else None

        logger.info('Getting Study protocols for %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

        obj_list = isa_study.protocols
        for objProt in obj_list:
            logger.info(objProt.name)
        # Using context to avoid envelop tags in contained objects
        sch = ProtocolSchema()
        sch.context['protocol'] = Protocol()
        if not obj_name:
            # return a list of objs
            logger.info('Got %s protocols', len(obj_list))
            return sch.dump(obj_list, many=True)
        else:
            # return a single obj
            found = False
            for index, obj in enumerate(obj_list):
                if obj.name.lower() == obj_name:
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
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
                "name": "force",
                "description": "Remove even if referenced in any assays",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "type": "Boolean",
                "defaultValue": False,
                "default": False
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
        parser.add_argument('force', help="Force remove protocol", location="args")

        force_remove_protocols = True
        args = parser.parse_args()
        force_remove = args['force']
        force_remove_protocols = False if force_remove.lower() != 'true' else True
        prot_name = args['name'] if args['name'] else None

        if not prot_name:
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
        logger.info('Deleting protocol %s for %s', prot_name, study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

        protocol = isa_study.get_prot(prot_name)
        if not protocol:
            abort(404)

        # Check if the protocol is used in any assays
        can_remove_protocol = True
        if not force_remove_protocols:
            for assay in isa_study.assays:
                if protocol.name.lower() == "sample collection":
                    can_remove_protocol = False
                    break
                try:
                    assay_df = read_tsv(os.path.join(study_location, assay.filename))
                except FileNotFoundError:
                    assay_df = None
                if protocol.name in assay_df:
                    can_remove_protocol = False
                    break

        if can_remove_protocol:
            # remove object
            isa_study.protocols.remove(protocol)
            logger.info("A copy of the previous files will %s saved", save_msg_str)
            iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
            logger.info('Deleted %s', protocol.name)
        else:
            abort(406, "The protocol is referenced in one or more assays")

        return jsonify(success=True)

    @swagger.operation(
        summary='Update Study Protocol',
        notes='''Update Study Protocol.
              <br>
              Use protocol name as a query parameter to update a specific protocol.<pre><code>
{
  "protocol": {
    "name": "Extraction",
    "protocolType": {
      "annotationValue": "Extraction",
      "termSource": null,
      "termAccession": ""
    },
    "description": "Please describe how the sample was extracted",
    "uri": "",
    "version": "",
    "parameters": [
      {
        "parameterName": {
          "annotationValue": "Post Extraction",
          "termSource": null,
          "termAccession": ""
        }
      },
      {
        "parameterName": {
          "annotationValue": "Derivatization",
          "termSource": null,
          "termAccession": ""
        }
      }
    ]
  }
}
</pre></code>''',
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
                "name": "name",
                "description": "Protocol name",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
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
        obj_name = args['name'].lower() if args['name'] else None
        if not obj_name:
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
        except (ValidationError, Exception):
            abort(400)

        # update protocol details
        logger.info('Updating Protocol details for %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)
        found = False
        for index, protocol in enumerate(isa_study.protocols):
            if protocol.name.lower() == obj_name:
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
        notes='''Add new Factor to a Study.<pre><code>
{
  "factor": {
    "factorName": "Gender",
    "factorType": {
      "annotationValue": "Gender",
      "termSource": {
        "name": "NCIT",
        "file": "http://data.bioontology.org/ontologies/NCIT",
        "version": "34",
        "description": "National Cancer Institute Thesaurus"
      },
      "termAccession": "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl"
    }
  }
}</pre></code>''',
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
        obj_name = args['name'].lower() if args['name'] else None
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
        except (ValidationError, Exception):
            abort(400)

        # Add new Study Factor
        logger.info('Adding new Study Factor %s for %s', new_obj.name, study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

        # check for factor added already
        obj = isa_study.get_factor(obj_name)
        if obj:
            abort(409)
        # add obj

        if new_obj.name:
            isa_study.factors.append(new_obj)
            logger.info("A copy of the previous files will %s saved", save_msg_str)

            # Check that the ontology is referenced in the investigation
            factor_type = new_obj.factor_type
            term_source = factor_type.term_source
            add_ontology_to_investigation(isa_inv, term_source.name, term_source.version,
                                          term_source.file, term_source.description)

            iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
            logger.info('Added %s', new_obj.name)
        else:
            abort(406, "Please provide a name (factorName) for the factor")

        return StudyFactorSchema().dump(new_obj)

    @swagger.operation(
        summary="Get Study Factors",
        notes="""Get Study Factors.
              <br>
              Use factor name as a query parameter to filter on a specific factor.""",
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
                "name": "name",
                "description": "Factor name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
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
            obj_name = args['name'].lower() if args['name'] else None

        logger.info('Getting Study Factors for %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

        obj_list = isa_study.factors
        # Using context to avoid envelop tags in contained objects
        sch = StudyFactorSchema()
        sch.context['factor'] = StudyFactor()
        if not obj_name:
            # return a list of objs
            logger.info('Got %s factors', len(obj_list))
            return sch.dump(obj_list, many=True)
        else:
            # return a single obj
            found = False
            for index, obj in enumerate(obj_list):
                if obj.name.lower() == obj_name:
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
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
        obj_name = args['name'].lower() if args['name'] else None
        if not obj_name:
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
        logger.info('Deleting Study Factor %s for %s', obj_name, study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

        # obj = isa_study.get_factor(obj_name)
        obj = self.get_factor(isa_study.factors, obj_name)
        if not obj:
            abort(404)
        # remove object
        isa_study.factors.remove(obj)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Deleted %s', obj.name)

        sch = StudyFactorSchema()
        sch.context['factor'] = StudyFactor()
        try:
            resp = sch.dump(obj)
        except (ValidationError, Exception) as err:
            logger.warning("Bad Study Factor format", err)
        return extended_response(data=resp.data, errs=resp.errors)

    def get_factor(self, factor_list, factor_name):
        for factor in factor_list:
            if factor.name.lower() == factor_name.lower():
                return factor
        return None

    @swagger.operation(
        summary='Update Study Factor',
        notes='''Update Study Factor.
              <br>
              Use factor name as a query parameter to update specific factor.<pre><code>
{
  "factor": {
    "factorName": "Gender",
    "factorType": {
      "annotationValue": "Gender",
      "termSource": {
        "name": "NCIT",
        "file": "http://data.bioontology.org/ontologies/NCIT",
        "version": "34",
        "description": "National Cancer Institute Thesaurus"
      },
      "termAccession": "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl"
    }
  }
}</pre></code>''',
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
                "name": "name",
                "description": "Factor name",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
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
            },
            {
                "code": 412,
                "message": "The JSON provided can not be parsed properly."
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

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

        # body content validation
        updated_factor = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['factor']
            # if partial=True missing fields will be ignored
            try:
                result = StudyFactorSchema().load(data, partial=False)
                updated_factor = result.data

                # Check that the ontology is referenced in the investigation
                factor_type = updated_factor.factor_type
                term_source = factor_type.term_source
                add_ontology_to_investigation(isa_inv, term_source.name, term_source.version,
                                              term_source.file, term_source.description)

            except Exception:
                abort(412)

        except (ValidationError, Exception):
            abort(400)

        # update Study Factor details
        logger.info('Updating Study Factor details for %s', study_id)

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
        notes='''Add new Design Descriptor to a Study. <pre><code>
{
  "studyDesignDescriptor": {
    "annotationValue": "metabolomic profiling",
    "termSource": {
      "name": "EFO",
      "file": "http://data.bioontology.org/ontologies/EFO",
      "version": "113",
      "description": "Experimental Factor Ontology"
    },
    "termAccession": "http://www.ebi.ac.uk/efo/EFO_0000752"
  }
}</code></pre>''',
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
        except (ValidationError, Exception):
            abort(400)

        # Add new Study Descriptor
        logger.info('Adding new Study Design Descriptor %s for %s', new_obj.term, study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

        # check for Study Descriptor added already
        for index, obj in enumerate(isa_study.design_descriptors):
            if obj.term == new_obj.term:
                abort(409)

        # Check that the ontology is referenced in the investigation
        term_source = new_obj.term_source
        if term_source:
            add_ontology_to_investigation(isa_inv, term_source.name, term_source.version,
                                          term_source.file, term_source.description)
        else:
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "annotationValue",
                "description": "Design Descriptor annotation value",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
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

        logger.info('Getting Study Design Descriptors for %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
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
        logger.info('Deleting Study Design Descriptor %s for %s', obj_term, study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

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
        notes='''Update Study Design Descriptor.
              <br>
              Use descriptor annotation value as a query parameter to filter on specific descriptor<pre><code>
{
  "studyDesignDescriptor": {
    "annotationValue": "metabolomic profiling",
    "termSource": {
      "name": "EFO",
      "file": "http://data.bioontology.org/ontologies/EFO",
      "version": "113",
      "description": "Experimental Factor Ontology"
    },
    "termAccession": "http://www.ebi.ac.uk/efo/EFO_0000752"
  }
}</code></pre>''',
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
                "name": "term",
                "description": "Design Descriptor annotation value",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
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
        except (ValidationError, Exception):
            abort(400)

        # update Study Design Descriptor details
        logger.info('Updating Study Design Descriptor details for %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)
        found = False
        for index, descriptor in enumerate(isa_study.design_descriptors):
            if descriptor.term == descriptor_term:
                found = True
                # update protocol details
                isa_study.design_descriptors[index] = updated_descriptor
                break
        if not found:
            abort(404, 'The descriptor %s was not found in this study, can not update.', descriptor_term)
        logger.info("A copy of the previous files will %s saved", save_msg_str)

        # Check that the ontology is referenced in the investigation
        term_source = updated_descriptor.term_source
        add_ontology_to_investigation(isa_inv, term_source.name, term_source.version,
                                      term_source.file, term_source.description)

        iac.write_isa_study(isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy)
        logger.info('Updated %s', updated_descriptor.term)

        return StudyDesignDescriptorSchema().dump(updated_descriptor)


class StudyPublications(Resource):

    @swagger.operation(
        summary='Add new Study Publication',
        notes='''Add new Publication to a Study.<pre><code>
{
  "publication": {
    "title": "Publication title",
    "authorList": "Author1, Author2",
    "doi": "10.1093/nar/gks1004",
    "pubMedID": "",
    "status": {
      "termAccession": "http://www.ebi.ac.uk/efo/EFO_0001796",
      "annotationValue": "Published",
      "termSource": null
    }
  }
}</pre></code>''',
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
        except (ValidationError, Exception):
            abort(400)

        # Add new Publication
        logger.info('Adding new Publication %s for %s', new_publication.title, study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

        # Check that the ontology is referenced in the investigation
        new_status = new_publication.status
        term_source = new_status.term_source
        if term_source:
            add_ontology_to_investigation(isa_inv, term_source.name, term_source.version,
                                          term_source.file, term_source.description)

        exists = False
        # check for Publication added already
        for index, publication in enumerate(isa_study.publications):
            if publication.title.strip().rstrip('\n') == new_publication.title.strip().rstrip('\n'):
                exists = True
        # add Study Publication
        if not exists:
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "title",
                "description": "Publication title",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
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

        logger.info('Getting Study Publications for %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
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
        logger.info('Deleting Study Publication %s for %s', publication_title, study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)
        found = False
        for index, publication in enumerate(isa_study.publications):
            if publication.title.strip().rstrip('\n') == publication_title.strip().rstrip('\n'):
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
        notes='''Update Study Publication.
              <br>
              Use publication title as a query parameter to update the correct publication.<pre><code>
{
  "publication": {
    "title": "Updated study title......",
    "authorList": "Author1, Author2",
    "doi": "10.1093/nar/gks1004",
    "pubMedID": "",
    "status": {
      "termAccession": "http://www.ebi.ac.uk/efo/EFO_0001796",
      "annotationValue": "Published",
      "termSource": null
    }
  }
}</pre></code>''',
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
                "description": "Publication title",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
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
        except (ValidationError, Exception):
            abort(400)

        # update Study Publication details
        logger.info('Updating Study Publication details for %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=True,
                                                         study_location=study_location)

        # Check that the ontology is referenced in the investigation
        new_status = updated_publication.status
        term_source = new_status.term_source
        if term_source:
            add_ontology_to_investigation(isa_inv, term_source.name, term_source.version,
                                          term_source.file, term_source.description)
        found = False
        for index, publication in enumerate(isa_study.publications):
            if publication.title.strip().rstrip('\n') == publication_title.strip().rstrip('\n'):
                found = True
                # update protocol details
                isa_study.publications[index] = updated_publication
                break
        if not found:
            abort(404, 'Could not find the publication title you tried to update')
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
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
        list_only = True
        obj_name = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name'].lower() if args['name'] else None
            list_only = False if args['list_only'].lower() != 'true' else True

        logger.info('Getting Study Sources for %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=False,
                                                         study_location=study_location)

        obj_list = isa_study.sources
        # Using context to avoid envelop tags in contained objects
        sch = SourceSchema()
        sch.context['source'] = Source()
        if not obj_name:
            # return a list of objs
            logger.info('Got %s sources', len(obj_list))
            if list_only:
                sch = SourceSchema(only=['name'])
                sch.context['source'] = Source()
            return sch.dump(obj_list, many=True)
        else:
            # return a single obj
            found = False
            for index, obj in enumerate(obj_list):
                if obj.name.lower() == obj_name:
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
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
        obj_name = args['name'].lower() if args['name'] else None
        if not obj_name:
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
        except (ValidationError, Exception):
            abort(400)

        # update Study Source details
        logger.info('Updating Study Source details for %s', study_id, user_token)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=False,
                                                         study_location=study_location)

        obj_list = isa_study.sources
        found = False
        for i, proc in enumerate(isa_study.process_sequence):
            for index, src in enumerate(proc.inputs):
                if isinstance(src, Source):
                    if src.name.lower() == obj_name:
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
                            save_samples_copy=True,
                            save_assays_copy=True)
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
        obj_name = args['name'].lower() if args['name'] else None
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
        new_samples = None
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['samples']
            result = SampleSchema().load(data, partial=False, many=True)
            new_samples = result.data
        except (ValidationError, Exception):
            abort(400)

        # Add new Study Sample
        logger.info('Adding new Samples to %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=False,
                                                         study_location=study_location)

        added_process = list()
        added_samples = list()
        for ndx, sample in enumerate(new_samples):
            # existing names will be ignored
            if not self.get_sample(isa_study.samples, sample.name):
                # add sources
                # there should be only one, but just in case
                for source in sample.derives_from:
                    if not self.get_source(isa_study.sources, source.name):
                        isa_study.sources.append(source)
                # add protocol
                protocol = self.get_protocol(isa_study.protocols, 'Sample collection')
                # if not found, create a new one
                if not protocol:
                    protocol = Protocol(name='Sample collection',
                                        protocol_type=OntologyAnnotation(term='Sample collection'))
                # add sample
                isa_study.samples.append(sample)

                # add processSequence
                process = Process(name=sample.name,
                                  executes_protocol=protocol,
                                  inputs=sample.derives_from,
                                  outputs=[sample],
                                  comments=sample.comments)
                isa_study.process_sequence.append(process)
                added_process.append(process)

                added_samples.append(sample)
                logger.info('Added %s', sample.name)

        # check if all samples were added
        warn = ''
        if len(added_samples) != len(new_samples):
            warn = 'Some of the samples were not added. ' \
                    'Added ' + str(len(added_samples)) + ' out of ' + str(len(new_samples))
            logger.warning(warn)

        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path,
                            save_investigation_copy=save_audit_copy,
                            save_samples_copy=True,
                            save_assays_copy=True)

        sch = ProcessSchema(many=True)
        return extended_response(data={'processSequence': sch.dump(added_process).data},
                                 warns=warn)

    def get_source(self, source_list, source_name):
        for source in source_list:
            if source.name.lower() == source_name.lower():
                return source
        return None

    def get_sample(self, sample_list, sample_name):
        for sample in sample_list:
            if sample.name.lower() == sample_name.lower():
                return sample
        return None

    def get_protocol(self, protocol_list, protocol_name):
        for protocol in protocol_list:
            if protocol.name.lower() == protocol_name.lower():
                return protocol
        return None

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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
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
        list_only = True
        obj_name = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name'].lower() if args['name'] else None
            list_only = False if args['list_only'].lower() != 'true' else True

        logger.info('Getting Samples for %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=False,
                                                         study_location=study_location)

        obj_list = isa_study.samples
        # Using context to avoid envelop tags in contained objects
        sch = SampleSchema()
        sch.context['sample'] = Sample()
        if not obj_name:
            # return a list of objs
            logger.info('Got %s samples', len(obj_list))
            if list_only:
                sch = SampleSchema(only=['name'])
                sch.context['sample'] = Sample()
            return sch.dump(obj_list, many=True)
        else:
            # return a single obj
            found = False
            for index, obj in enumerate(obj_list):
                if obj.name.lower() == obj_name:
                    found = True
                    break
            if not found:
                abort(404)
            logger.info('Got %s', obj.name)
            return sch.dump(obj)

    @swagger.operation(
        summary='Update Study Samples',
        notes="""Update a list of Study Samples. Only existing Samples will be updated, unknown will be ignored. 
        To change name, only one sample can be processed at a time.""",
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
                "name": "name",
                "description": "Study Sample name. Leave empty if updating more than one sample.",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "samples",
                "description": 'Study Sample list in ISA-JSON format.',
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
        parser.add_argument('name', help='Study Sample name')
        obj_name = None
        parser.add_argument('list_only', help='List names only')
        list_only = True
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name'].lower() if args['name'] else None
            list_only = True if args['list_only'].lower() == 'true' else False
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
        # updated_obj = None
        new_samples = list()
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['samples']
            # if partial=True missing fields will be ignored
            result = SampleSchema().load(data, many=True, partial=False)
            # updated_obj = result.data
            new_samples = result.data
            if len(new_samples) == 0:
                logger.warning("No valid data provided.")
                abort(400)
        except (ValidationError, Exception) as err:
            logger.warning("Bad format JSON request.", err)
            abort(400)

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=False,
                                                         study_location=study_location)

        updated_samples = list()
        # single sample
        if obj_name:
            # param name should be used only to update name for a single study
            if len(new_samples) > 1:
                logger.warning("Requesting name update for more than one sample")
                abort(400)

            logger.info('Updating Study Samples details for %s,', study_id)
            new_sample = new_samples[0]
            if self.update_sample(isa_study, obj_name, new_sample):
                updated_samples.append(new_sample.name)

        # multiple samples
        else:
            logger.info('Updating details for %d Study Samples', len(new_samples))
            for i, new_sample in enumerate(new_samples):
                if self.update_sample(isa_study, new_sample.name.lower(), new_sample):
                    updated_samples.append(new_sample)

        # check if all samples were updated
        warns = ''
        if len(updated_samples) != len(new_samples):
            warns = 'Some of the samples were not updated. ' \
                    'Updated ' + str(len(updated_samples)) + ' out of ' + str(len(new_samples))
            logger.warning(warns)

        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path,
                            save_investigation_copy=save_audit_copy,
                            save_samples_copy=save_audit_copy,
                            save_assays_copy=save_audit_copy)

        sch = SampleSchema(many=True)
        if list_only:
            sch = SampleSchema(only=('name',), many=True)
        return extended_response(data={'samples': sch.dump(updated_samples).data}, warns=warns)

    def update_sample(self, isa_study, sample_name, new_sample):
        for i, process in enumerate(isa_study.process_sequence):
            for ii, sample in enumerate(process.outputs):
                if isinstance(sample, Sample) and sample.name.lower() == sample_name:
                    process.outputs[ii] = Sample(name=new_sample.name,
                                                 characteristics=new_sample.characteristics,
                                                 factor_values=new_sample.factor_values,
                                                 derives_from=new_sample.derives_from,
                                                 comments=new_sample.comments)
                    logger.info('Updated sample: %s', new_sample.name)
                    return True
        return False

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
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        # delete samples
        logger.info('Deleting all marked Samples for %s', study_id)
        location =  study_location  # wsc.get_study_location(study_id, user_token)
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
        obj_name = args['name'].lower() if args['name'] else None
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
        except (ValidationError, Exception):
            abort(400)

        # Add new Study Material
        logger.info('Adding new Material %s to %s', new_material.name, study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=False,
                                                         study_location=study_location)

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
                            save_samples_copy=True,
                            save_assays_copy=True)
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
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
        list_only = True
        obj_name = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name'].lower() if args['name'] else None
            list_only = False if args['list_only'].lower() != 'true' else True

        logger.info('Getting Other Materials for %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=False,
                                                         study_location=study_location)

        obj_list = isa_study.other_material
        # Using context to avoid envelop tags in contained objects
        sch = OtherMaterialSchema()
        sch.context['other_material'] = Material()
        if not obj_name:
            # return a list of objs
            logger.info('Got %s Materials', len(obj_list))
            if list_only:
                sch = OtherMaterialSchema(only=['name'])
                sch.context['other_material'] = Material()
            return sch.dump(obj_list, many=True)
        else:
            # return a single obj
            found = False
            for index, obj in enumerate(obj_list):
                if obj.name.lower() == obj_name:
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
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
        obj_name = args['name'].lower() if args['name'] else None
        if not obj_name:
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
        logger.info('Deleting Study Material %s for %s', obj_name, study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=False,
                                                         study_location=study_location)

        obj_list = isa_study.other_material
        found = False
        for index, material in enumerate(obj_list):
            if material.name.lower() == obj_name:
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
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
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
        obj_name = args['name'].lower() if args['name'] else None
        if not obj_name:
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
        except (ValidationError, Exception):
            abort(400)

        # update Study Material details
        logger.info('Updating Study Material details for %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=False,
                                                         study_location=study_location)

        obj_list = isa_study.other_material
        found = False
        for index, material in enumerate(obj_list):
            if material.name.lower() == obj_name:
                found = True
                # update study
                isa_study.other_material[index] = updated_obj
                break
        if not found:
            abort(404)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(isa_inv, user_token, std_path,
                            save_investigation_copy=save_audit_copy,
                            save_samples_copy=True,
                            save_assays_copy=True)
        logger.info('Updated %s', updated_obj.name)

        return SampleSchema().dump(updated_obj)


class StudyProcesses(Resource):
    @swagger.operation(
        summary="Get Study Process Sequence",
        notes="""Get Study Process Sequence.
                  <br>
                  Use process or protocol name as query parameter for specific searching.""",
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
                "description": "Process name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "prot_name",
                "description": "Protocol name",
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
        parser.add_argument('prot_name', help='Protocol name')
        parser.add_argument('list_only', help='List names only')
        list_only = True
        obj_name = None
        prot_name = None
        if request.args:
            args = parser.parse_args(req=request)
            obj_name = args['name'].lower() if args['name'] else None
            prot_name = args['prot_name'].lower() if args['prot_name'] else None
            list_only = False if args['list_only'].lower() != 'true' else True

        logger.info('Getting Study Processes for %s', study_id)
        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(study_id, user_token,
                                                         skip_load_tables=False,
                                                         study_location=study_location)

        obj_list = isa_study.process_sequence
        found = list()
        if not obj_name and not prot_name:
            found = obj_list
        else:
            for index, proto in enumerate(obj_list):
                if proto.name.lower() == obj_name \
                        or proto.executes_protocol.name.lower() == prot_name:
                    found.append(proto)
        if not len(found) > 0:
            abort(404)
        logger.info('Found %d protocols', len(found))

        sch = ProcessSchema(many=True)
        if list_only:
            sch = ProcessSchema(only=('name', 'executes_protocol.name',), many=True)
        return extended_response(data={'processSequence': sch.dump(found).data})


class StudySubmitters(Resource):
    @swagger.operation(
        summary='Add new Study Submitters',
        notes='''Add new Submitter (owner) to a Study. The submitter must already exist in the MetaboLights database.  
        Due to GDPR data protection issues with confirming if an email address exists in MetaboLights, we will always indicate a successful update<pre><code>
    { 
      "submitters": [
        {
          "email": "joe.blogs@university.ac.uk"
        },
        {
          "email": "jane.blogs@university.ac.uk"
        } 
      ]
    }
    </code></pre>''',
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
                "name": "submitters",
                "description": 'details for submitters.',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            abort(401)

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['submitters']
            email = "not yet set"

            for submitter in data:
                email = submitter.get('email')
                study_submitters(study_id, email, 'add')
                try:
                    wsc.reindex_study(study_id, user_token)
                except:
                    logger.error("Could not index study " + study_id + " whilst adding user " + submitter)

        except:
            logger.error("Could not add user " + email + " to study " + study_id)

        return jsonify({"submitters": "Successfully added"})

    @swagger.operation(
        summary='Delete a Study Submitter',
        notes='''Delete an existing Submitter (owner) from a Study. The submitter must already exist in the MetaboLights database. 
        Due to data protection issues with confirming if an email address exists in MetaboLights, we will always indicate a successful deletion<pre><code>
    { 
      "submitters": [
        { 
          "email": "joe.blogs@university.ac.uk"
        },
        {
          "email": "jane.blogs@university.ac.uk"
        } 
      ]
    }
    </code></pre>''',
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
                "name": "submitters",
                "description": 'details for submitters.',
                "paramType": "body",
                "type": "string",
                "format": "application/json",
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
            },
            {
                "code": 409,
                "message": "Conflict. The request could not be completed due to a conflict"
                           " with the current state of study. This is usually issued to prevent duplications."
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
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['submitters']
            email = "not yet set"

            for submitter in data:
                email = submitter.get('email')
                study_submitters(study_id, email, 'delete')
                try:
                    wsc.reindex_study(study_id, user_token)
                except:
                    logger.error("Could not index study " + study_id + " whilst adding user " + submitter)

        except:
            logger.error("Could not delete user " + email + " from study " + study_id)

        return jsonify({"submitters": "Successfully deleted"})



