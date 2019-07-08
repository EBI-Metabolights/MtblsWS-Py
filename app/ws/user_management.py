#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Jul-08
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

from flask import request, abort
from flask_restful import Resource, reqparse
from marshmallow import ValidationError
from app.ws.mm_models import *
from flask_restful_swagger import swagger
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from flask import current_app as app
from app.ws.db_connection import create_user, update_user
from app.ws.utils import *
import logging
import json
import uuid
import base64
import string
import random


logger = logging.getLogger('wslog')
iac = IsaApiClient()
wsc = WsClient()


class UserManagement(Resource):
    @swagger.operation(
        summary='Add a new MetaboLights user account',
        notes='''Add a new MetaboLights user account<pre><code>
    { 
         "user": 
            {
                "firstName": "Joe",
                "lastName": "Blogs",
                "email": "joe.blogs@cam.ac.uk",
                "affiliation": "University of Cambridge",
                "affiliation_url": "https://www.bioc.cam.ac.uk",
                "address": "The Department of Biochemistry, The Sanger Building, 80 Tennis Court Road, Cambridge, CB2 1GA, UK.",
                "orcid" : "0000-0003-3168-4149",
                "metaspace_api_key": "adfasd-123123-dfsdf-sasdfa"
            }
    }</pre></code>
    </p>
    Username will be set to the same as the email. A password will be emailed to the email address''',
        parameters=[
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "user",
                "description": 'user definition',
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
            }
        ]
    )
    def post(self,):
        log_request(request)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            # user token is required
            abort(401)

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions('MTBLS1', user_token)
        if not read_access:
            abort(403)

        firstName = None
        lastName = None
        email = None
        affiliation = None
        affiliation_url = None
        address = None
        orcid = None
        metaspace_api_key = None

        # body content validation
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['user']
            try:
                firstName = data['firstName']
                lastName = data['lastName']
                email = data['email']
                affiliation = data['affiliation']
                affiliation_url = data['affiliation_url']
                address = data['address']
                orcid = data['orcid']
                metaspace_api_key = data['metaspace_api_key']
            except Exception as e:
                abort(412, str(e))
        except (ValidationError, Exception):
            abort(400, 'Incorrect JSON provided')

        api_token = uuid.uuid1()
        password = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(8))
        password_encoded = base64.b64encode(password.encode("utf-8"))
        password_encoded = str(password_encoded, 'utf-8')

        status, message = create_user(firstName, lastName, email, affiliation, affiliation_url,
                    address, orcid, api_token, password_encoded, metaspace_api_key)

        if status:
            return {"user_name": email, "api_token": str(api_token), "password": str(password)}
        else:
            return {"Error": message}

    @swagger.operation(
        summary='Update a MetaboLights user account',
        notes='''Update a MetaboLights user account<pre><code>
    { 
         "user": 
            {
                "firstName": "Joe",
                "lastName": "Blogs",
                "email": "new.job.blogs@somewhere.org",
                "affiliation": "University of Cambridge",
                "affiliation_url": "https://www.bioc.cam.ac.uk",
                "address": "The Department of Biochemistry, The Sanger Building, 80 Tennis Court Road, Cambridge, CB2 1GA, UK.",
                "orcid" : "0000-0003-3168-4149",
                "metaspace_api_key": "adfasd-123123-dfsdf-sasdfa"

            }
    }</pre></code>
    </p>
    Username will be set to the same as the email.''',
        parameters=[
            {
                "name": "user_token",
                "description": "User API token",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "existing_user_name",
                "description": "Existing username in MetaboLights",
                "paramType": "header",
                "type": "string",
                "required": True,
                "allowMultiple": False
            },
            {
                "name": "user",
                "description": 'user definition',
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
            }
        ]
    )
    def put(self, ):
        log_request(request)

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]
        else:
            # user token is required
            abort(401)

        existing_user_name = None
        if "existing_user_name" in request.headers:
            existing_user_name = request.headers["existing_user_name"]
        else:
            # user id is required
            abort(401)

        # check for access rights
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, study_status = \
            wsc.get_permissions('MTBLS1', user_token)
        if not read_access:
            abort(403)

        firstName = None
        lastName = None
        email = None
        affiliation = None
        affiliation_url = None
        address = None
        orcid = None

        # body content validation
        try:
            data_dict = json.loads(request.data.decode('utf-8'))
            data = data_dict['user']
            try:
                firstName = data['firstName']
                lastName = data['lastName']
                email = data['email']
                affiliation = data['affiliation']
                affiliation_url = data['affiliation_url']
                address = data['address']
                orcid = data['orcid']
                user_name = email
                metaspace_api_key = data['metaspace_api_key']
            except Exception as e:
                abort(412, str(e))
        except (ValidationError, Exception):
            abort(400, 'Incorrect JSON provided')

        api_token = uuid.uuid1()
        password = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(8))
        password_encoded = base64.b64encode(password.encode("utf-8"))
        password_encoded = str(password_encoded, 'utf-8')

        status, message = update_user(firstName, lastName, email, affiliation, affiliation_url,
                                      address, orcid, api_token, password_encoded, existing_user_name,
                                      is_curator, metaspace_api_key)

        if status:
            return {"user_name": email, "api_token": str(api_token), "password": str(password)}
        else:
            return {"Error": message}
