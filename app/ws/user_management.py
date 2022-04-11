#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Feb-13
#  Modified by:   kenneth
#
#  Copyright 2020 EMBL - European Bioinformatics Institute
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

import json
import logging

from flask import request, abort, jsonify
from flask_restful import Resource
from flask_restful_swagger import swagger
from marshmallow import ValidationError

from app.ws.db_connection import create_user, update_user, get_user
from app.ws.misc_utilities.request_parsers import RequestParsers
from app.ws.utils import val_email, get_new_password_and_api_token
from app.ws.misc_utilities import ws_utils, response_messages as resp

logger = logging.getLogger('wslog')


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
            resp.HTTP_200, resp.HTTP_400, resp.HTTP_401, resp.HTTP_403, resp.HTTP_404
        ]
    )
    def post(self,):
        ws_utils.validate_restricted_ws_request(request, 'MTBLS1')

        first_name = None
        last_name = None
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
                first_name = data['firstName']
                last_name = data['lastName']
                email = data['email']
                affiliation = data['affiliation']
                affiliation_url = data['affiliation_url']
                address = data['address']
                orcid = data['orcid']
                metaspace_api_key = data['metaspace_api_key']
            except Exception as e:
                abort(400, str(e))
        except (ValidationError, Exception):
            abort(400, 'Incorrect JSON provided')

        password, password_encoded, api_token = get_new_password_and_api_token()

        val_email(email)
        status, message = create_user(first_name, last_name, email, affiliation, affiliation_url,
                                      address, orcid, api_token, password_encoded, metaspace_api_key)

        if status:
            return {"user_name": email, "api_token": str(api_token), "password": str(password)}
        else:
            return {"Error": message}

    @swagger.operation(
        summary='Update a MetaboLights user account',
        notes='''Update/change a MetaboLights user/submitter account<pre><code>
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
            resp.HTTP_200, resp.HTTP_400, resp.HTTP_401, resp.HTTP_403, resp.HTTP_404
        ]
    )
    def put(self, ):

        validation_result = ws_utils.validate_restricted_ws_request(request, 'MTBLS1')

        existing_user_name = None
        if "existing_user_name" in request.headers:
            existing_user_name = request.headers["existing_user_name"]
        else:
            # user id is required
            abort(401)

        first_name = None
        last_name = None
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
                first_name = data['firstName']
                last_name = data['lastName']
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

        password, password_encoded, api_token = get_new_password_and_api_token()

        status, message = update_user(first_name, last_name, email, affiliation, affiliation_url,
                                      address, orcid, api_token, password_encoded, existing_user_name,
                                      validation_result.is_curator, metaspace_api_key)

        if status:
            return {"user_name": email, "api_token": str(api_token), "password": str(password)}
        else:
            return {"Error": message}

    @swagger.operation(
        summary="Get all the information associated with a single user.",
        notes="Currently only supports retrieving a user by username. "
              "Also only supports retrieving just one user at a time. ",
        parameters=[
            {
                "name": "username",
                "description": "Username for the user whose details are being retrieved.",
                "required": True,
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
            }
        ],
        responseMessages=[
            resp.HTTP_200, resp.HTTP_400, resp.HTTP_404, resp.HTTP_500

        ]
    )
    def get(self):
        """
        Return a single user by username. Checks the validity of the param, retrieves the API token from the header and
        checks its validity and what permissions are available to the bearer of the token.
        """

        validation_result = ws_utils.validate_restricted_ws_request(request, 'MTBLS1')

        # pull username from query params.
        username = None

        user_parser = RequestParsers.username_parser()
        if request.args:
            args = user_parser.parse_args(req=request)
            username = args['username']

        # username has not been properly provided, abort with code 400 (bad request).
        if username is None:
            abort(400)

        # query the database for the user, and return the result of the query.
        return jsonify(get_user(username))

