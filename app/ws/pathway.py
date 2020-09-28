#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Sep-28
#  Modified by:   Jiakang
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

import json

import pandas as pd
from flask import jsonify
from flask import request
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger

from app.ws.db_connection import *
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.ontology_info import *
from app.ws.utils import log_request

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


class keggid(Resource):
    @swagger.operation(
        summary="Mapping CHEBI IDs with KEGG IDs",
        notes='''Get matched CHEBI IDs / KEGG IDs.
              <br>
              <pre><code>
{
  "ids": {
    "CHEBIID": ["CHEBI:123","CHEBI:2234"],
    "KEGGID": ["KEGG:123","KEGG:2234"]
  }
}</code></pre>''',
        parameters=[
            {
                "name": "studyID",
                "description": "Metabolights studyID",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },

            {
                "name": "kegg_only",
                "description": "only return kegg IDs",
                "required": False,
                "allowEmptyValue": True,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },
            {
                "name": "data",
                "description": 'list of matching chebi / kegg ids',
                "paramType": "body",
                "type": "string",
                # "format": "application/json",
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
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def post(self):
        log_request(request)
        parser = reqparse.RequestParser()

        parser.add_argument('studyID', help='Metabolights studyID')
        studyID = None
        if request.args:
            args = parser.parse_args(req=request)
            studyID = args['studyID']
            if studyID:
                studyID = studyID.strip().upper()

        parser.add_argument('kegg_only', help="only return kegg IDs")
        kegg_only = False
        if request.args:
            args = parser.parse_args(req=request)
            kegg_only = args['kegg_only']
            if kegg_only.lower() in ['true', '1']:
                kegg_only = True
            elif kegg_only.lower() in ['false', '0']:
                kegg_only = False

        if len(request.data.decode('utf-8')) > 0:
            chebiID = []
            keggID = []
            result = {}

            try:
                data_dict = json.loads(request.data.decode('utf-8'))['ids']
                chebiID = data_dict['CHEBIID']
                keggID = data_dict['KEGGID']
            except Exception as e:
                logger.info(e)
                print(e)
                abort(400)

        if studyID:
            query = '''SELECT DISTINCT DATABASE_IDENTIFIER FROM MAF_INFO WHERE ACC = '{studyID}' AND (DATABASE_IDENTIFIER <> '') IS NOT FALSE'''.format(
                studyID=studyID)

            postgresql_pool, conn, cursor = get_connection()
            cursor.execute(query)
            # d= cursor.fetchall()
            chebiID = [r[0] for r in cursor.fetchall()]

            result = match_chebi_kegg(chebiID, [])

        elif len(chebiID) > 0 or len(keggID) > 0:

            result = match_chebi_kegg(chebiID, keggID)

        if kegg_only:
            res = [x.lstrip('cpd:').upper() for x in list(result.values())]
            return jsonify(res)
        else:
            return jsonify(result)


def match_chebi_kegg(chebiID, KeggID):
    df = pd.read_csv('./resources/chebi_kegg.tsv', sep='\t')
    df['CHEBIID_c'] = df['CHEBIID'].map(lambda x: x.lstrip('chebi:'))
    df['KEGGID_c'] = df['KEGGID'].map(lambda x: x.lstrip('cpd:'))

    chebiID = [x.lower().lstrip('chebi:') for x in chebiID]
    KeggID = [x.lower().lstrip('kegg:').upper() for x in KeggID]

    res = df[df['CHEBIID_c'].isin(chebiID) | df['KEGGID_c'].isin(KeggID)]
    return dict(zip(res.CHEBIID, res.KEGGID))
