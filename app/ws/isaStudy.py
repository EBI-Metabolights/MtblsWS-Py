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
import datetime
import json
import os
import re

from flask import request, jsonify
from flask_restful import Resource, marshal_with, reqparse, abort
from flask_restful_swagger import swagger
from marshmallow import ValidationError
from app.config import get_settings
from app.tasks.common_tasks.basic_tasks.elasticsearch import reindex_study

from app.ws import utils
from app.ws.db_connection import study_submitters, update_release_date
from app.ws.isaApiClient import IsaApiClient
from app.ws.mm_models import *
from email_validator import validate_email, EmailNotValidError
from app.ws.mm_models import PersonSchema
from app.ws.models import Investigation_api_model, serialize_investigation
from app.ws.mtblsWSclient import WsClient
from app.ws.study.user_service import UserService
from app.ws.utils import (
    delete_column_from_tsv_file,
    log_request,
    add_ontology_to_investigation,
    read_tsv,
    update_ontolgies_in_isa_tab_sheets,
    write_tsv,
)
import logging

logger = logging.getLogger("wslog")
iac = IsaApiClient()
wsc = WsClient()


def extended_response(data=None, errs=None, warns=None):
    ext_resp = {
        "data": data if data else list(),
        "errors": errs if errs else list(),
        "warnings": warns if warns else list(),
    }
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
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    @marshal_with(Investigation_api_model, envelope="investigation")
    def get(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        logger.info("Getting ISA-JSON Study %s", study_id)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        isa_obj = iac.get_isa_json(study_id, user_token, study_location=study_location)
        str_inv = json.dumps(
            {"investigation": isa_obj}, default=serialize_investigation, sort_keys=True
        )
        logger.info(
            "... found Study: %s %s", isa_obj.get("title"), isa_obj.get("identifier")
        )
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
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
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

        logger.info("Getting Study title for %s", study_id)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )

        title = isa_study.title
        logger.info("Got %s", title)
        return jsonify({"title": title})

    @swagger.operation(
        summary="Update Study Title",
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
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "title",
                "description": "New title",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
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
        new_title = None
        try:
            data_dict = json.loads(request.data.decode("utf-8"))
            new_title = data_dict["title"]
        except Exception:
            abort(400, message="invalid input")
        # check for keeping copies
        save_audit_copy = False
        save_msg_str = "NOT be"
        if (
            "save_audit_copy" in request.headers
            and request.headers["save_audit_copy"].lower() == "true"
        ):
            save_audit_copy = True
            save_msg_str = "be"

        # update study title
        logger.info("Updating Study title for %s", study_id)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )
        isa_study.title = new_title
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(
            isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy
        )
        inputs = {"user_token": user_token, "study_id": study_id}
        reindex_task = reindex_study.apply_async(kwargs=inputs, expires=60)
        logger.info("Applied %s", new_title)
        return jsonify({"title": new_title})


class StudyReleaseDate(Resource):
    @swagger.operation(
        summary="Update study release date",
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
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "release_date",
                "description": "Release date",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
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
        data_dict = json.loads(request.data.decode("utf-8"))
        new_date = data_dict["release_date"]

        try:
            datetime.datetime.strptime(new_date, "%Y-%m-%d")
        except ValueError:
            abort(406, message="Incorrect date format, please use YYYY-MM-DD")

        # check for keeping copies
        save_audit_copy = False
        save_msg_str = "NOT be"
        if (
            "save_audit_copy" in request.headers
            and request.headers["save_audit_copy"].lower() == "true"
        ):
            save_audit_copy = True
            save_msg_str = "be"

        # update study title
        logger.info("Updating Study title for %s", study_id)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )
        isa_inv.public_release_date = new_date
        isa_study.public_release_date = new_date
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(
            isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy
        )
        # update database
        update_release_date(study_id, new_date)
        inputs = {"user_token": user_token, "study_id": study_id}
        reindex_task = reindex_study.apply_async(kwargs=inputs, expires=60)
        logger.info("Applied %s", new_date)
        return jsonify({"release_date": new_date})


class StudyMetaInfo(Resource):
    @swagger.operation(
        summary="[Deprecated] Get Study Release Date and Status",
        notes="Get Study Release Date and Status.",
        parameters=[
            {
                "name": "study_id",
                "description": "Study Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
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

        logger.info(
            "Getting Study details for %s, using API-Key %s", study_id, user_token
        )
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)
        return jsonify(
            {"data": ["status:" + study_status, "release-date:" + release_date]}
        )


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
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
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

        logger.info("Getting Study description for %s", study_id)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )
        description = isa_study.description
        logger.info("Got %s", description)
        return jsonify({"description": description})

    @swagger.operation(
        summary="Update Study Description",
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
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "description",
                "description": "New description",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
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

        logger.debug("Request headers   : %s", request.headers)
        logger.debug("Request data      : %s", request.data)
        logger.debug("Request data-utf8 : %s", request.data.decode("utf-8"))

        # body content validation
        if request.data is None or request.json is None:
            abort(400)
        new_description = None
        try:
            data_dict = json.loads(request.data.decode("utf-8"))
            new_description = data_dict["description"]
        except Exception:
            abort(400, message="invalid input")

        # check for keeping copies
        save_audit_copy = False
        save_msg_str = "NOT be"
        if (
            "save_audit_copy" in request.headers
            and request.headers["save_audit_copy"].lower() == "true"
        ):
            save_audit_copy = True
            save_msg_str = "be"

        # update study description
        logger.info("Updating Study description for %s", study_id)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )
        isa_study.description = new_description.replace('"', "'").replace(
            "#", ""
        )  # ISA-API can not deal with these
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(
            isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy
        )
        inputs = {"user_token": user_token, "study_id": study_id}
        reindex_task = reindex_study.apply_async(kwargs=inputs, expires=60)
        logger.info(
            "Applied %s and reindex task is triggered for %s",
            new_description,
            reindex_task.id,
        )
        return jsonify({"description": isa_study.description})


def roles_to_contacts(isa_inv, new_contact):
    # # Check that the ontology is referenced in the investigation
    # isa_inv, efo = add_ontology_to_investigation(
    #     isa_inv,
    #     "EFO",
    #     "132",
    #     "http://data.bioontology.org/ontologies/EFO",
    #     "Experimental Factor Ontology",
    # )
    # new_role = OntologyAnnotation(
    #     term_accession="http://purl.obolibrary.org/obo/NCIT_C51826",
    #     term="Investigator",
    #     term_source=efo,
    # )

    if new_contact and new_contact.roles:
        valid_roles = []
        for role in new_contact.roles:
            if role.term and role.term.strip():
                valid_roles.append(role)

    return isa_inv, new_contact


class StudyContacts(Resource):
    @swagger.operation(
        summary="Add new Study Contact",
        notes="""Add new Contact to a Study. <pre><code>
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
            ],
            "comments": [
                {
                    "name": "Study Person ORCID",
                    "value": "0000-0011-1163-1497"
                }
            ]
        }
    ]
}

</code></pre>""",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "contact",
                "description": "details for contact in ISA-JSON format.",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
            {
                "code": 409,
                "message": "Conflict. The request could not be completed due to a conflict"
                " with the current state of study. This is usually issued to prevent duplications.",
            },
        ],
    )
    def post(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation

        email = request.args.get("email")
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
        if (
            "save_audit_copy" in request.headers
            and request.headers["save_audit_copy"].lower() == "true"
        ):
            save_audit_copy = True
            save_msg_str = "be"

        study_root_path = (
            get_settings().study.mounted_paths.study_metadata_files_root_path
        )
        study_location = os.path.join(study_root_path, study_id)
        UserService.get_instance().validate_user_has_write_access(user_token, study_id)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )

        contact_persons = {}
        for contact in isa_study.contacts:
            contact_name_key = (contact.first_name + contact.last_name).lower()
            contact_persons[contact_name_key] = contact

        # body content validation
        new_contacts = []
        errors = {}
        data_dict = {}
        try:
            data_dict = json.loads(request.data.decode("utf-8"))
        except Exception as e:
            abort(400, error=f"input data is not valid json: {e}")

        try:
            data = data_dict.get("contacts", [])
            # if partial=True missing fields will be ignored

            for contact in data:
                # Add new contact
                result = PersonSchema().load(contact, partial=False)
                new_contact: PersonSchema = result.data
                logger.info(
                    "Adding new Contact %s for %s", new_contact.first_name, study_id
                )
                contact_key = (new_contact.first_name + new_contact.last_name).lower()
                full_name = f"{new_contact.first_name} {new_contact.last_name}"
                if contact_key not in contact_persons:
                    # Check that the ontology is referenced in the investigation
                    validation_result = self.validate_contact(new_contact)
                    if validation_result:
                        errors[full_name] = validation_result
                    isa_inv, new_contact = roles_to_contacts(isa_inv, new_contact)
                    for role in new_contact.roles:
                        term_anno = role
                        term_source = term_anno.term_source
                        new_contacts.append(new_contact)
                        add_ontology_to_investigation(
                            isa_inv,
                            term_source.name,
                            term_source.version,
                            term_source.file,
                            term_source.description,
                        )
                else:
                    raise Exception(
                        f"Contact '{new_contact.first_name} {new_contact.last_name}' is already defined."
                    )
        except Exception as e:
            import traceback

            traceback.print_exc()
            logger.error(e)
            abort(400, error=str(e))

        if errors:
            abort(400, errors=errors)

        # add contact
        isa_study.contacts = isa_study.contacts + new_contacts

        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(
            isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy
        )
        inputs = {"user_token": user_token, "study_id": study_id}
        reindex_study.apply_async(kwargs=inputs, expires=60)

        obj_list = isa_study.contacts
        # Using context to avoid envelop tags in contained objects
        sch = PersonSchema()
        sch.context["contact"] = Person()
        if email is None:
            # return a list of objs
            logger.info("Got %s contacts", len(obj_list))
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
            logger.info("Got %s", obj.email)
            return sch.dump(obj)

    def validate_contact(self, new_contact: PersonSchema):
        errors = []
        comments = {x.name: x for x in new_contact.comments}
        if not new_contact.first_name or len(new_contact.first_name) < 2:
            errors.append("First name is not valid")
        if not new_contact.last_name or len(new_contact.last_name) < 2:
            errors.append("Last name is not valid")
        if not new_contact.roles:
            errors.append("Contact role is not defined")
        roles = new_contact.roles
        pi_roles = [
            x for x in roles if x.term and "principal investigator" in x.term.lower()
        ]
        has_pi_role = len(pi_roles) > 0

        ror_id_comment = comments.get("Study Person Affiliation ROR ID")
        orcid_comment = comments.get("Study Person ORCID")
        alternative_email_comment = comments.get("Study Person Alternative Email")
        ror_id = ror_id_comment.value if ror_id_comment else None
        orcid = orcid_comment.value if orcid_comment else None
        alternative_email = (
            alternative_email_comment.value if alternative_email_comment else None
        )
        if orcid:
            orcid_pattern = r"^[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{3}[X0-9]$"
            if not re.match(orcid_pattern, orcid):
                errors.append(f"Invalid ORCID '{orcid}'")
        if ror_id:
            ror_id_pattern = r"^(https://ror.org/[0-9a-z]{9}|https://www.wikidata.org/wiki/Q[1-9][0-9]{0,19})$"
            if not re.match(ror_id_pattern, ror_id):
                errors.append(
                    f"Invalid affiliation ROR ID or affiliation wikidata id '{ror_id}'"
                )

        if new_contact.email:
            try:
                valid = validate_email(new_contact.email)
                new_contact.email = valid.email  # Normalized form
            except EmailNotValidError as e:
                errors.append(f"Invalid email {new_contact.email}")
        if alternative_email and alternative_email:
            try:
                valid = validate_email(alternative_email)
                alternative_email_comment.value = valid.email  # Normalized form
            except EmailNotValidError as e:
                errors.append(f"Invalid alternative email {alternative_email}")

        if new_contact.affiliation and len(new_contact.affiliation) < 10:
            errors.append("Affiliation must be equal or greater than 10 characters.")

        if has_pi_role:
            if not new_contact.email:
                errors.append("Principal Investigator email is empty")
            if not new_contact.affiliation:
                errors.append("Principal Investigator affiliation is empty")
            if not ror_id:
                errors.append("Affiliation ROR ID is empty")
            if not orcid:
                errors.append("ORCID is empty")
        return errors

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
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "email",
                "description": "Contact's email",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "full_name",
                "description": "Contact's first and last name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
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
        email = None
        full_name = None
        if request.args:
            email = request.args.get("email")
            full_name = request.args.get("full_name", "").replace(" ", "").lower()

        logger.info("Getting Contacts %s for Study %s", email, study_id)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )

        obj_list = isa_study.contacts
        # Using context to avoid envelop tags in contained objects
        sch = PersonSchema()
        sch.context["contact"] = Person()
        if email is None and full_name is None:
            # return a list of objs
            logger.info("Got %s contacts", len(obj_list))
            return sch.dump(obj_list, many=True)
        else:
            # return a single obj
            if full_name:
                logger.info("Contact full_name " + full_name)
            if email:
                logger.info("Email " + email)
            found = False
            for index, obj in enumerate(obj_list):
                if email and obj.email == email:
                    found = True
                    break
                if full_name and obj.first_name + obj.last_name == full_name:
                    found = True
                    break

            if not found:
                abort(404)
            logger.info("Got %s", obj.email)
            return sch.dump(obj)

    @swagger.operation(
        summary="Update Study Contact",
        notes="""Update Contact associated with a Study.
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
            ],
            "comments": [
                {
                    "name": "Study Person ORCID",
                    "value": "0000-0011-1163-1497"
                }
            ]
        }
    ]
}

</code></pre>""",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "contact_index",
                "description": "Contact's index in the contact list, starting from 0.",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "number",
            },
            {
                "name": "contact",
                "description": "details for contact in ISA-JSON format.",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404, errors=["study_id must be provided."])
        # query validation
        contact_index = int(request.args.get("contact_index", -1))
        if contact_index < 0:
            abort(400, errors=["contact_index must be provided."])

        # User authentication
        user_token = request.headers.get("user_token")
        if not user_token:
            abort(401, errors=["user_token must be provided."])

        UserService.get_instance().validate_user_has_write_access(user_token, study_id)

        study_root_path = (
            get_settings().study.mounted_paths.study_metadata_files_root_path
        )
        study_location = os.path.join(study_root_path, study_id)
        # check for keeping copies
        save_audit_copy = False
        save_msg_str = "NOT be"
        if (
            "save_audit_copy" in request.headers
            and request.headers["save_audit_copy"].lower() == "true"
        ):
            save_audit_copy = True
            save_msg_str = "be"

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )

        if contact_index >= len(isa_study.contacts):
            abort(404, errors=[f"person not found at index {contact_index}"])

        errors = []
        # body content validation
        updated_contact = None
        try:
            data_dict = json.loads(request.data.decode("utf-8"))
            contacts = data_dict.get("contacts", [])
            if not contacts:
                abort(400, ["No contact provided"])
            contact = contacts[0]
            # if partial=True missing fields will be ignored
            result = PersonSchema().load(contact, partial=True)
            updated_contact = result.data
            errors = self.validate_contact(updated_contact)

            isa_inv, updated_contact = roles_to_contacts(isa_inv, updated_contact)
            for role in updated_contact.roles:
                term_anno = role
                term_source = term_anno.term_source
                add_ontology_to_investigation(
                    isa_inv,
                    term_source.name,
                    term_source.version,
                    term_source.file,
                    term_source.description,
                )
        except (ValidationError, Exception) as ex:
            abort(400, errors=[str(ex)])
        if errors:
            abort(400, errors=errors)
        # update contact details
        logger.info("Updating Contact details for %s", study_id)

        isa_study.contacts[contact_index] = updated_contact

        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(
            isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy
        )
        inputs = {"user_token": user_token, "study_id": study_id}
        reindex_study.apply_async(kwargs=inputs, expires=60)
        logger.info("Updated %s", updated_contact.email)

        return PersonSchema().dump(updated_contact)

    @swagger.operation(
        summary="Delete Study Contact",
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
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "contact_index",
                "description": "Contact's email",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "int",
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def delete(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404, errors=["study_id must be provided."])
        # query validation
        contact_index = request.args.get("contact_index")
        if contact_index is None:
            abort(400, errors=["contact_index must be defined."])
        if isinstance(contact_index, str):
            contact_index = int(contact_index) if contact_index.isnumeric() else -1
        if isinstance(contact_index, int) and int(contact_index) < 0:
            abort(400, errors=["contact_index must be a positive integer."])

        # User authentication
        user_token = request.headers.get("user_token")
        if not user_token:
            abort(401, errors=["user_token must be provided."])

        # check for keeping copies
        save_audit_copy = False
        save_msg_str = "NOT be"
        if (
            "save_audit_copy" in request.headers
            and request.headers["save_audit_copy"].lower() == "true"
        ):
            save_audit_copy = True
            save_msg_str = "be"
        UserService.get_instance().validate_user_has_write_access(user_token, study_id)
        study_root_path = (
            get_settings().study.mounted_paths.study_metadata_files_root_path
        )
        study_location = os.path.join(study_root_path, study_id)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )
        if contact_index >= len(isa_study.contacts):
            abort(404, errors=[f"Contact not found at {contact_index}."])
        person = isa_study.contacts.pop(int(contact_index))

        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(
            isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy
        )
        logger.info("%s Contact at %s deleted ", study_id, contact_index)

        return PersonSchema().dump(person)


class StudyProtocols(Resource):
    @swagger.operation(
        summary="Add new Study Protocol",
        notes="""Add a new Protocol to a Study.
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
</pre></code>""",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "protocol",
                "description": "Protocol in ISA-JSON format.",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
            {
                "code": 409,
                "message": "Conflict. The request could not be completed due to a conflict"
                " with the current state of study. This is usually issued to prevent duplications.",
            },
        ],
    )
    def post(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation

        obj_name = (
            request.args.get("name").lower() if request.args.get("name") else None
        )
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
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        # check for keeping copies
        save_audit_copy = False
        save_msg_str = "NOT be"
        if (
            "save_audit_copy" in request.headers
            and request.headers["save_audit_copy"].lower() == "true"
        ):
            save_audit_copy = True
            save_msg_str = "be"

        # body content validation
        new_obj = None
        try:
            data_dict = json.loads(request.data.decode("utf-8"))
            data = data_dict["protocol"]
            # if partial=True missing fields will be ignored
            result = ProtocolSchema().load(data, partial=False)
            new_obj = result.data
        except (ValidationError, Exception):
            abort(400)

        # TODO, use new utils.add_protcol method
        # Add new protocol
        logger.info("Adding new Protocol %s for %s", new_obj.name, study_id)
        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )

        # check for protocol added already
        obj = isa_study.get_prot(obj_name)
        if obj:
            abort(409)
        # add obj
        isa_study.protocols.append(new_obj)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(
            isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy
        )
        logger.info("Added %s", new_obj.name)

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
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "name",
                "description": "Protocol name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
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
        # query validation

        obj_name = None
        if request.args:
            obj_name = (
                request.args.get("name").lower() if request.args.get("name") else None
            )

        logger.info("Getting Study protocols for %s", study_id)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )

        obj_list = isa_study.protocols
        for objProt in obj_list:
            logger.info(objProt.name)
        # Using context to avoid envelop tags in contained objects
        sch = ProtocolSchema()
        sch.context["protocol"] = Protocol()
        if not obj_name:
            # return a list of objs
            logger.info("Got %s protocols", len(obj_list))
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
            logger.info("Got %s", obj.name)
            return sch.dump(obj)

    @swagger.operation(
        summary="Delete Study Protocol",
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
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "name",
                "description": "Protocol name",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
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
                "default": False,
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def delete(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation
        force_remove_protocols = True

        force_remove = request.args.get("force")
        force_remove_protocols = False if force_remove.lower() != "true" else True
        prot_name = request.args.get("name") if request.args.get("name") else None

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
        if (
            "save_audit_copy" in request.headers
            and request.headers["save_audit_copy"].lower() == "true"
        ):
            save_audit_copy = True
            save_msg_str = "be"

        # delete protocol
        logger.info("Deleting protocol %s for %s", prot_name, study_id)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )

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
            iac.write_isa_study(
                isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy
            )
            logger.info("Deleted %s", protocol.name)
        else:
            abort(406, message="The protocol is referenced in one or more assays")

        return jsonify(success=True)

    @swagger.operation(
        summary="Update Study Protocol",
        notes="""Update Study Protocol.
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
</pre></code>""",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "name",
                "description": "Protocol name",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "protocol",
                "description": "Protocol in ISA-JSON format.",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation

        obj_name = (
            request.args.get("name").lower() if request.args.get("name") else None
        )
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
        if (
            "save_audit_copy" in request.headers
            and request.headers["save_audit_copy"].lower() == "true"
        ):
            save_audit_copy = True
            save_msg_str = "be"

        # body content validation
        updated_protocol = None
        try:
            data_dict = json.loads(request.data.decode("utf-8"))
            data = data_dict["protocol"]
            # if partial=True missing fields will be ignored
            result = ProtocolSchema().load(data, partial=False)
            updated_protocol = result.data
        except (ValidationError, Exception):
            abort(400)

        # update protocol details
        logger.info("Updating Protocol details for %s", study_id)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )
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
        iac.write_isa_study(
            isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy
        )
        logger.info("Updated %s", updated_protocol.name)

        return ProtocolSchema().dump(updated_protocol)


class StudyFactors(Resource):
    @swagger.operation(
        summary="Add new Study Factor",
        notes="""Add new Factor to a Study.<pre><code>
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
}</pre></code>""",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "protocol",
                "description": "Study Factor in ISA-JSON format.",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
            {
                "code": 409,
                "message": "Conflict. The request could not be completed due to a conflict"
                " with the current state of study. This is usually issued to prevent duplications.",
            },
        ],
    )
    def post(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation

        obj_name = (
            request.args.get("name").lower() if request.args.get("name") else None
        )
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
        if (
            "save_audit_copy" in request.headers
            and request.headers["save_audit_copy"].lower() == "true"
        ):
            save_audit_copy = True
            save_msg_str = "be"

        # body content validation
        new_obj = None
        try:
            data_dict = json.loads(request.data.decode("utf-8"))
            data = data_dict["factor"]
            # if partial=True missing fields will be ignored
            result = StudyFactorSchema().load(data, partial=False)
            new_obj = result.data
        except (ValidationError, Exception):
            abort(400)

        # Add new Study Factor
        logger.info("Adding new Study Factor %s for %s", new_obj.name, study_id)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )

        # check for factor added already
        obj = isa_study.get_factor(obj_name)
        if obj:
            abort(409)
        # add obj

        if new_obj.name:
            for current in isa_study.factors:
                if current.name.strip() == new_obj.name:
                    abort(401, message=f"Factor name '{new_obj.name}' already exists.")
            isa_study.factors.append(new_obj)
            logger.info("A copy of the previous files will %s saved", save_msg_str)

            # Check that the ontology is referenced in the investigation
            factor_type = new_obj.factor_type
            term_source = factor_type.term_source
            add_ontology_to_investigation(
                isa_inv,
                term_source.name,
                term_source.version,
                term_source.file,
                term_source.description,
            )

            iac.write_isa_study(
                isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy
            )
            logger.info("Added %s", new_obj.name)
        else:
            abort(406, message="Please provide a name (factorName) for the factor")

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
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "name",
                "description": "Factor name",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
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
        # query validation

        obj_name = None
        if request.args:
            obj_name = (
                request.args.get("name").lower() if request.args.get("name") else None
            )

        logger.info("Getting Study Factors for %s", study_id)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )

        obj_list = isa_study.factors
        # Using context to avoid envelop tags in contained objects
        sch = StudyFactorSchema()
        sch.context["factor"] = StudyFactor()
        if not obj_name:
            # return a list of objs
            logger.info("Got %s factors", len(obj_list))
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
            logger.info("Got %s", obj.name)
            return sch.dump(obj)

    @swagger.operation(
        summary="Delete Study Factor",
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
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "name",
                "description": "Factor name",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def delete(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation

        obj_name = request.args.get("name") if request.args.get("name") else None
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
        if (
            "save_audit_copy" in request.headers
            and request.headers["save_audit_copy"].lower() == "true"
        ):
            save_audit_copy = True
            save_msg_str = "be"

        # delete Study Factor
        logger.info("Deleting Study Factor %s for %s", obj_name, study_id)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)
        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )

        # obj = isa_study.get_factor(obj_name)
        obj = self.get_factor(isa_study.factors, obj_name)
        if not obj:
            abort(404)
        # remove object
        isa_study.factors.remove(obj)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(
            isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy
        )
        logger.info("Deleted %s", obj.name)

        sch = StudyFactorSchema()
        sch.context["factor"] = StudyFactor()
        try:
            resp = sch.dump(obj)
            sample_file = os.path.join(study_location, isa_study.filename)
            file_df = read_tsv(sample_file)
            factor_name = f"Factor Value[{obj_name}]"
            try:
                delete_column_from_tsv_file(file_df, factor_name)
                write_tsv(file_df, sample_file)
            except Exception as e:
                logger.error(
                    "Could not remove column '" + factor_name + "' from sample file "
                )
                logger.error(str(e))
                return {"Success": "Failed to remove column(s) from sample file"}
        except (ValidationError, Exception) as err:
            logger.warning("Bad Study Factor format " + str(err))
        return extended_response(data=resp.data, errs=resp.errors)

    def get_factor(self, factor_list, factor_name):
        for factor in factor_list:
            if (
                factor
                and factor.name is not None
                and str(factor.name).lower() == factor_name.lower()
            ):
                return factor
        return None

    @swagger.operation(
        summary="Update Study Factor",
        notes="""Update Study Factor.
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
}</pre></code>""",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "name",
                "description": "Factor name",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "data",
                "description": "Factor in ISA-JSON format.",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
            {"code": 412, "message": "The JSON provided can not be parsed properly."},
        ],
    )
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation

        factor_name = request.args.get("name")
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
        if (
            "save_audit_copy" in request.headers
            and request.headers["save_audit_copy"].lower() == "true"
        ):
            save_audit_copy = True
            save_msg_str = "be"

        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )

        # body content validation
        updated_factor = None
        try:
            data_dict = json.loads(request.data.decode("utf-8"))
            data = data_dict["factor"]
            # if partial=True missing fields will be ignored
            try:
                result = StudyFactorSchema().load(data, partial=False)
                updated_factor = result.data

                # Check that the ontology is referenced in the investigation
                factor_type = updated_factor.factor_type
                term_source = factor_type.term_source
                add_ontology_to_investigation(
                    isa_inv,
                    term_source.name,
                    term_source.version,
                    term_source.file,
                    term_source.description,
                )

            except Exception:
                abort(412)

        except (ValidationError, Exception):
            abort(400)

        # update Study Factor details
        logger.info("Updating Study Factor details for %s", study_id)

        found = False
        old_factor = ""
        for idx, factor in enumerate(isa_study.factors):
            if (
                factor
                and factor.name is not None
                and str(factor.name).lower() == factor_name.lower()
            ):
                found = True
                old_factor = factor.name
                # update factor details
                isa_study.factors[idx] = updated_factor
                break
        if not found:
            abort(404, message="The factor was not found")

        if found:
            update_ontolgies_in_isa_tab_sheets(
                "factor", old_factor, updated_factor.name, study_location, isa_study
            )

        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(
            isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy
        )
        logger.info("Updated %s", updated_factor.name)

        return StudyFactorSchema().dump(updated_factor)


class StudyDescriptors(Resource):
    @swagger.operation(
        summary="Add new Study Design Descriptor",
        notes="""Add new Design Descriptor to a Study. <pre><code>
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
}</code></pre>""",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "studyDesignDescriptor",
                "description": "Study Design Descriptor in ISA-JSON format.",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
            {
                "code": 409,
                "message": "Conflict. The request could not be completed due to a conflict"
                " with the current state of study. This is usually issued to prevent duplications.",
            },
        ],
    )
    def post(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation

        obj_term = request.args.get("term")
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
        if (
            "save_audit_copy" in request.headers
            and request.headers["save_audit_copy"].lower() == "true"
        ):
            save_audit_copy = True
            save_msg_str = "be"

        # body content validation
        new_obj = None
        try:
            data_dict = json.loads(request.data.decode("utf-8"))
            data = data_dict["studyDesignDescriptor"]
            # if partial=True missing fields will be ignored
            result = StudyDesignDescriptorSchema().load(data, partial=False)
            new_obj = result.data
        except (ValidationError, Exception):
            abort(400)

        # Add new Study Descriptor
        logger.info(
            "Adding new Study Design Descriptor %s for %s", new_obj.term, study_id
        )
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )

        # check for Study Descriptor added already
        for obj in isa_study.design_descriptors:
            if obj.term == new_obj.term:
                abort(409, message=f"Descriptor name '{new_obj.term}' already exists.")

        # Check that the ontology is referenced in the investigation
        term_source = new_obj.term_source
        if term_source:
            add_ontology_to_investigation(
                isa_inv,
                term_source.name,
                term_source.version,
                term_source.file,
                term_source.description,
            )
        else:
            abort(409)

        # add Study Descriptor
        isa_study.design_descriptors.append(new_obj)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(
            isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy
        )
        logger.info("Added %s", new_obj.term)

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
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "annotationValue",
                "description": "Design Descriptor annotation value",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
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
        # query validation

        obj_term = None
        if request.args:
            obj_term = request.args.get("annotationValue")

        logger.info("Getting Study Design Descriptors for %s", study_id)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )

        obj_list = isa_study.design_descriptors
        # Using context to avoid envelop tags in contained objects
        sch = StudyDesignDescriptorSchema()
        sch.context["descriptor"] = StudyDescriptors()
        if obj_term is None:
            # return a list of objs
            logger.info("Got %s descriptors", len(obj_list))
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
            logger.info("Got %s", obj.term)
            return sch.dump(obj)

    @swagger.operation(
        summary="Delete Study Design Descriptor",
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
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "term",
                "description": "Design Descriptor annotation value",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def delete(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation

        obj_term = request.args.get("term")
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
        if (
            "save_audit_copy" in request.headers
            and request.headers["save_audit_copy"].lower() == "true"
        ):
            save_audit_copy = True
            save_msg_str = "be"

        # delete Study Design Descriptor
        logger.info("Deleting Study Design Descriptor %s for %s", obj_term, study_id)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )

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
        iac.write_isa_study(
            isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy
        )
        logger.info("Deleted %s", obj.term)

        return StudyDesignDescriptorSchema().dump(obj)

    @swagger.operation(
        summary="Update Study Design Descriptor",
        notes="""Update Study Design Descriptor.
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
}</code></pre>""",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "term",
                "description": "Design Descriptor annotation value",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "protocol",
                "description": "Design Descriptor in ISA-JSON format.",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation

        descriptor_term = request.args.get("term")
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
        if (
            "save_audit_copy" in request.headers
            and request.headers["save_audit_copy"].lower() == "true"
        ):
            save_audit_copy = True
            save_msg_str = "be"

        # body content validation
        updated_descriptor = None
        try:
            data_dict = json.loads(request.data.decode("utf-8"))
            data = data_dict["studyDesignDescriptor"]
            # if partial=True missing fields will be ignored
            result = StudyDesignDescriptorSchema().load(data, partial=False)
            updated_descriptor = result.data
        except (ValidationError, Exception):
            abort(400)

        # update Study Design Descriptor details
        logger.info("Updating Study Design Descriptor details for %s", study_id)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )
        found = False
        for index, descriptor in enumerate(
            isa_study.design_descriptors
        ):  # ToDo, fails with "+" in the term
            if descriptor.term == descriptor_term:
                found = True
                # update protocol details
                isa_study.design_descriptors[index] = updated_descriptor
                break
        if not found:
            abort(
                404,
                message=f"The descriptor %s was not found in this study, can not update. {descriptor_term}",
            )
            logger.info("A copy of the previous files will %s saved. " + save_msg_str)

        # Check that the ontology is referenced in the investigation
        term_source = updated_descriptor.term_source
        add_ontology_to_investigation(
            isa_inv,
            term_source.name,
            term_source.version,
            term_source.file,
            term_source.description,
        )

        iac.write_isa_study(
            isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy
        )
        logger.info("Updated %s", updated_descriptor.term)

        return StudyDesignDescriptorSchema().dump(updated_descriptor)


class StudyPublications(Resource):
    @swagger.operation(
        summary="Add new Study Publication",
        notes="""Add new Publication to a Study.<pre><code>
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
}</pre></code>""",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "publication",
                "description": "Study Publication in ISA-JSON format.",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
            {
                "code": 409,
                "message": "Conflict. The request could not be completed due to a conflict"
                " with the current state of study. This is usually issued to prevent duplications.",
            },
        ],
    )
    def post(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation

        publication_title = request.args.get("title")
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
        if (
            "save_audit_copy" in request.headers
            and request.headers["save_audit_copy"].lower() == "true"
        ):
            save_audit_copy = True
            save_msg_str = "be"

        # body content validation
        new_publication = None
        try:
            data_dict = json.loads(request.data.decode("utf-8"))
            data = data_dict["publication"]
            # if partial=True missing fields will be ignored
            result = PublicationSchema().load(data, partial=False)
            new_publication = result.data
        except (ValidationError, Exception):
            abort(400)

        # Add new Publication
        logger.info("Adding new Publication %s for %s", new_publication.title, study_id)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )

        # Check that the ontology is referenced in the investigation
        # new_status = new_publication.status
        # term_source = new_status.term_source
        # if term_source:
        #     add_ontology_to_investigation(isa_inv, term_source.name, term_source.version,
        #                                   term_source.file, term_source.description)

        exists = False
        current_publication = None
        # check for Publication added already
        for index, publication in enumerate(isa_study.publications):
            if publication.title.strip().strip(
                "."
            ) == new_publication.title.strip().strip("."):
                exists = True
                current_publication = publication
                break
        # add Study Publication
        updated = False

        if not exists:
            isa_study.publications.append(new_publication)
            updated = True
            logger.info("Added %s", new_publication.title)
        else:
            for field in ["title", "author_list", "doi", "pubmed_id"]:
                current_val = getattr(current_publication, field)
                new_val = getattr(new_publication, field)
                if current_val != new_val:
                    setattr(
                        current_publication,
                        field,
                        new_val if new_val is not None else "",
                    )
                    updated = True
            for field in ["term", "term_source", "term_accession"]:
                current_val = getattr(current_publication.status, field)
                new_val = getattr(new_publication.status, field)
                if current_val != new_val:
                    setattr(
                        current_publication.status,
                        field,
                        new_val if new_val is not None else "",
                    )
                    updated = True

            if updated:
                logger.info("Updated %s", new_publication.title)
        if updated:
            iac.write_isa_study(
                isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy
            )
            # logger.info("A copy of the previous files will %s saved", save_msg_str)

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
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "title",
                "description": "Publication title",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
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
        # query validation
        # ToDo add authors, PubMedID and DOI filters

        obj_title = None
        if request.args:
            obj_title = request.args.get("title")

        logger.info("Getting Study Publications for %s", study_id)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not read_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )

        obj_list = isa_study.publications
        # Using context to avoid envelop tags in contained objects
        sch = PublicationSchema()
        sch.context["publication"] = Publication()
        if obj_title is None:
            # return a list of publications
            logger.info("Got %s publications", len(isa_study.publications))
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
            logger.info("Got %s", obj.title)
            return sch.dump(obj)

    @swagger.operation(
        summary="Delete Study Publication",
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
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "title",
                "description": "Publication title",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def delete(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation

        publication_title = request.args.get("title")
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
        if (
            "save_audit_copy" in request.headers
            and request.headers["save_audit_copy"].lower() == "true"
        ):
            save_audit_copy = True
            save_msg_str = "be"

        # delete publication
        logger.info("Deleting Study Publication %s for %s", publication_title, study_id)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )
        found = False
        for index, publication in enumerate(isa_study.publications):
            if publication.title.strip().rstrip(
                "\n"
            ) == publication_title.strip().rstrip("\n"):
                found = True
                # delete Study Publication
                del isa_study.publications[index]
                break
        if not found:
            abort(404, message="Requested publication title does not exist.")
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(
            isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy
        )
        logger.info("Deleted %s", publication.title)

        return PublicationSchema().dump(publication)

    @swagger.operation(
        summary="Update Study Publication",
        notes="""Update Study Publication.
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
}</pre></code>""",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "title",
                "description": "Publication title",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string",
            },
            {
                "name": "publication",
                "description": "Publication in ISA-JSON format.",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "save-audit-copy",
                "description": "Keep track of changes saving a copy of the unmodified files.",
                "paramType": "header",
                "type": "Boolean",
                "defaultValue": True,
                "format": "application/json",
                "required": False,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
        ],
    )
    def put(self, study_id):
        log_request(request)
        # param validation
        if study_id is None:
            abort(404)
        # query validation

        publication_title = request.args.get("title")
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
        if (
            "save_audit_copy" in request.headers
            and request.headers["save_audit_copy"].lower() == "true"
        ):
            save_audit_copy = True
            save_msg_str = "be"

        # body content validation
        updated_publication = None
        try:
            data_dict = json.loads(request.data.decode("utf-8"))
            data = data_dict["publication"]
            # if partial=True missing fields will be ignored
            result = PublicationSchema().load(data, partial=False)
            updated_publication = result.data
        except (ValidationError, Exception):
            abort(400)

        # update Study Publication details
        logger.info("Updating Study Publication details for %s", study_id)
        # check for access rights
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        isa_study, isa_inv, std_path = iac.get_isa_study(
            study_id, user_token, skip_load_tables=True, study_location=study_location
        )

        # Check that the ontology is referenced in the investigation
        new_status = updated_publication.status
        # term_source = new_status.term_source

        found = False
        for index, publication in enumerate(isa_study.publications):
            if publication.title.strip().strip(".") == publication_title.strip().rstrip(
                "."
            ):
                found = True
                # update protocol details
                isa_study.publications[index] = updated_publication
                break
        if not found:
            abort(404)
        # if term_source:
        #     add_ontology_to_investigation(isa_inv, term_source.name, term_source.version,
        #                                   term_source.file, term_source.description)
        logger.info("A copy of the previous files will %s saved", save_msg_str)
        iac.write_isa_study(
            isa_inv, user_token, std_path, save_investigation_copy=save_audit_copy
        )
        logger.info("Updated %s", updated_publication.title)

        return PublicationSchema().dump(updated_publication)


class StudySubmitters(Resource):
    @swagger.operation(
        summary="Add new Study Submitters",
        notes="""Add new Submitter (owner) to a Study. The submitter must already exist in the MetaboLights database.  
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
    </code></pre>""",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "submitters",
                "description": "details for submitters.",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
            {
                "code": 409,
                "message": "Conflict. The request could not be completed due to a conflict"
                " with the current state of study. This is usually issued to prevent duplications.",
            },
        ],
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
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        try:
            data_dict = json.loads(request.data.decode("utf-8"))
            data = data_dict["submitters"]
            email = "not yet set"

            for submitter in data:
                email = submitter.get("email")
                study_submitters(study_id, email, "add")
                inputs = {"user_token": user_token, "study_id": study_id}
                reindex_task = reindex_study.apply_async(kwargs=inputs, expires=60)
        except:
            logger.error("Could not add user " + email + " to study " + study_id)

        return jsonify({"submitters": "Successfully added"})

    @swagger.operation(
        summary="Delete a Study Submitter",
        notes="""Delete an existing Submitter (owner) from a Study. The submitter must already exist in the MetaboLights database. 
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
    </code></pre>""",
        parameters=[
            {
                "name": "study_id",
                "description": "MTBLS Identifier",
                "required": True,
                "allowMultiple": False,
                "paramType": "path",
                "dataType": "string",
            },
            {
                "name": "user-token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False,
            },
            {
                "name": "submitters",
                "description": "details for submitters.",
                "paramType": "body",
                "type": "string",
                "format": "application/json",
                "required": True,
                "allowMultiple": False,
            },
        ],
        responseMessages=[
            {"code": 200, "message": "OK."},
            {
                "code": 400,
                "message": "Bad Request. Server could not understand the request due to malformed syntax.",
            },
            {
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication.",
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed for this user.",
            },
            {
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist.",
            },
            {
                "code": 409,
                "message": "Conflict. The request could not be completed due to a conflict"
                " with the current state of study. This is usually issued to prevent duplications.",
            },
        ],
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
        (
            is_curator,
            read_access,
            write_access,
            obfuscation_code,
            study_location,
            release_date,
            submission_date,
            study_status,
        ) = wsc.get_permissions(study_id, user_token)
        if not write_access:
            abort(403)

        try:
            data_dict = json.loads(request.data.decode("utf-8"))
            data = data_dict["submitters"]
            email = "not yet set"

            for submitter in data:
                email = submitter.get("email")
                study_submitters(study_id, email, "delete")
                inputs = {"user_token": user_token, "study_id": study_id}
                reindex_task = reindex_study.apply_async(kwargs=inputs, expires=60)
        except:
            logger.error("Could not delete user " + email + " from study " + study_id)

        return jsonify({"submitters": "Successfully deleted"})
