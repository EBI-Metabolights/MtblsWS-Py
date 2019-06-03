#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Apr-05
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

# MapStudies
# Created by JKChang
# 21/11/2018, 15:04
# Tag:
# Description: map metabolights_zooma.tsv terms with studies

import json
import logging

import numpy as np
import pandas as pd
from flask import request, abort, current_app as app
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from owlready2 import urllib

from app.ws.mtblsWSclient import WsClient
from app.ws.utils import log_request

wsc = WsClient()


class getStudyInfo():

    def __init__(self, studyID, user_token):
        try:
            url = 'https://www.ebi.ac.uk/metabolights/webservice/study/' + studyID
            request = urllib.request.Request(url)
            request.add_header('user_token', user_token)
            response = urllib.request.urlopen(request)
            content = response.read().decode('utf-8')
            self.study_content = json.loads(content)
        except:
            print('cant find study', studyID)

    def getFactors(self):
        try:
            res = []
            for ele in self.study_content['content']['factors']:
                res.append(ele['name'])
            return res
        except:
            return None

    def getOrganismName(self):
        try:
            res = []
            for ele in self.study_content['content']['organism']:
                res.append(ele['organismName'])
            return res
        except:
            return None

    def getOrganismPart(self):
        try:
            res = []
            for ele in self.study_content['content']['organism']:
                res.append(ele['organismPart'])
            return res
        except:
            return None

    def getOrganismPair(self):
        try:
            return self.study_content['content']['organism']
        except:
            return None


def searchStudies(query, user_token, feature='factor'):
    # list of all studies
    url = 'https://www.ebi.ac.uk/metabolights/webservice/study/list'
    request = urllib.request.Request(url)
    request.add_header('user_token', user_token)
    response = urllib.request.urlopen(request)
    content = response.read().decode('utf-8')
    j_content = json.loads(content)

    import re

    def atoi(text):
        return int(text) if text.isdigit() else text

    def natural_keys(text):
        return [atoi(c) for c in re.split('(\d+)', text)]

    res = []
    for studyID in j_content['content']:
        print('searching', studyID)
        info = getStudyInfo(studyID, user_token)
        if feature.casefold() == 'factor'.casefold():
            fea = info.getFactors()
        elif feature.casefold() == 'organism'.casefold():
            fea = info.getOrganismName()
        elif feature.casefold() == 'organismPart'.casefold():
            fea = info.getOrganismPart()
        # elif feature == ''
        else:
            fea = None

        if fea != None and query.casefold() in (f.casefold() for f in fea):
            print('adding', studyID)
            res.append(studyID)

        # if(len(res) >=3):
        #     break

    res.sort(key=natural_keys)
    return res


class MapStudies(Resource):
    @swagger.operation(
        summary="Map terms with studies (curator only)",
        notes='''Map terms with all public/private MTBLS studies''',
        parameters=[
            {
                "name": "term",
                "description": "query term",
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
                "code": 401,
                "message": "Unauthorized. Access to the resource requires user authentication."
            },
            {
                "code": 403,
                "message": "Forbidden. Access to the study is not allowed. Please provide a valid user token"
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

        parser.add_argument('term', help="Ontology term")
        term = None
        if request.args:
            args = parser.parse_args(req=request)
            term = args['term']

        # User authentication
        user_token = None
        if "user_token" in request.headers:
            user_token = request.headers["user_token"]

        if user_token is None:
            abort(403)

        # Need to check that the user is actually an active user, ie the user_token exists
        is_curator, read_access, write_access, obfuscation_code, study_location, release_date, submission_date, \
            study_status = wsc.get_permissions('MTBLS1', user_token)
        if not is_curator:
            abort(403)

        logger = logging.getLogger('wslog')
        try:
            file_name = app.config.get('MTBLS_ZOOMA_FILE')
            table_df = pd.read_csv(file_name, sep="\t", encoding='utf-8')
            table_df = table_df.replace(np.nan, '', regex=True)

            if term:
                try:
                    temp = table_df[table_df['PROPERTY_VALUE'].str.contains(term, na=False, case=False)]
                    l = temp.index.values.tolist()
                    for i in l:
                        query = table_df.iloc[i]['PROPERTY_VALUE']
                        attribute_name = 'factor'
                        res = ','.join(searchStudies(query, user_token, feature=attribute_name))
                        table_df.iloc[i]['STUDY'] = res
                        table_df.to_csv(file_name, sep='\t', index=False, encoding='utf-8')
                except Exception as e:
                    logger.error('Fail to find term in the spreadsheet' + term + str(e))


            else:
                for i in range(len(table_df)):
                    query = table_df.iloc[i]['PROPERTY_VALUE']
                    attribute_name = 'factor'
                    res = ','.join(searchStudies(query, user_token, feature=attribute_name))
                    table_df.iloc[i]['STUDY'] = res
                    table_df.to_csv(file_name, sep='\t', index=False, encoding='utf-8')
        except Exception as e:
            logger.error('Fail to load metabolights-zooma.tsv' + str(e))
