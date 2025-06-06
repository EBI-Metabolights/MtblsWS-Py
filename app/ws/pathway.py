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
import logging
from urllib import request as urllib_request

import pandas as pd
import requests
from flask import request, jsonify
from flask_restful import Resource, reqparse, abort
from flask_restful_swagger import swagger
from app.config import get_settings
from app.config.utils import get_host_internal_url

from app.study_folder_utils import convert_relative_to_real_path
from app.ws.cluster_jobs import submit_job
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import log_request, get_connection

logger = logging.getLogger('wslog')
iac = IsaApiClient()
wsc = WsClient()


class keggid(Resource):
    @swagger.operation(
        summary="Mapping CHEBI IDs with KEGG IDs",
        notes='''Get matched CHEBI IDs / KEGG IDs.''',
        #               <br>
        #               <pre><code>
        # {
        #     "CHEBIID": ["CHEBI:123","CHEBI:2234"],
        #     "KEGGID": ["KEGG:123","KEGG:2234"]
        # }</code></pre>''',
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
            }
            # ,
            # {
            #     "name": "data",
            #     "description": 'list of matching chebi / kegg ids',
            #     "paramType": "body",
            #     "type": "string",
            #     # "format": "application/json",
            #     "required": False,
            #     "allowMultiple": False
            # }

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
    def get(self):
        log_request(request)
        

        
        studyID = None
        if request.args:
            
            studyID = request.args.get('studyID')
            if studyID:
                studyID = studyID.strip().upper()

        
        if request.args:
            
            kegg = request.args.get('kegg_only')
            if not kegg:
                kegg_only = False
            elif kegg and kegg.lower() in ['true', '1']:
                kegg_only = True
            elif kegg and kegg.lower() in ['false', '0']:
                kegg_only = False
            else:
                abort(400)

        # chebiID = []
        # keggID = []
        result = {}
        # if len(request.data.decode('utf-8')) > 0:
        #     try:
        #         data_dict = json.loads(request.data.decode('utf-8'))
        #         chebiID = data_dict['CHEBIID']
        #         keggID = data_dict['KEGGID']
        #     except Exception as e:
        #         logger.info(e)
        #         print(e)
        #         abort(400)

        if studyID:
            uni_organism = uniqueOrganism(studyID)
            if len(uni_organism) > 1:
                res = {org: [] for org in uni_organism}

                # get list of ISA files
                try:
                    assay_file, investigation_file, sample_file, maf_file = getFileList(studyID)
                except:
                    assay_file, investigation_file, sample_file, maf_file = '', '', '', ''
                    print('Fail to load study ', studyID)

                # sample
                sample = get_sample_file(studyID=studyID, sample_file_name=sample_file)
                sample = sample[['Sample Name', 'Characteristics[Organism]']]
                organisms = list(sample['Characteristics[Organism]'].unique())

                # maf
                from collections import defaultdict
                result = defaultdict(list, {key: [] for key in organisms})
                for maf_name in maf_file:
                    res = maf_reader(studyID, maf_name, sample_df=sample)
                    for i, j in res.items():
                        result[i].extend(j)

                result = dict(result)
            elif len(uni_organism) == 1:
                query = '''SELECT DISTINCT DATABASE_IDENTIFIER FROM MAF_INFO WHERE ACC = '{studyID}' AND (DATABASE_IDENTIFIER <> '') IS NOT FALSE'''.format(
                    studyID=studyID)

                postgresql_pool, conn, cursor = get_connection()
                cursor.execute(query)
                # d= cursor.fetchall()
                ID = [r[0] for r in cursor.fetchall()]
                result = {uni_organism[0]: ID}
            else:
                abort(400)

            for org, ids in result.items():
                pair1 = match_chebi_kegg([x for x in ids if 'chebi' in x.lower()], [])
                pair2 = match_hmdb_kegg([x for x in ids if 'hmdb' in x.lower()], [])

                result[org] = {**pair1, **pair2}

        # elif len(chebiID) > 0 or len(keggID) > 0:
        #     result['input_ids'] = match_chebi_kegg(chebiID, keggID)

        if kegg_only:
            try:
                res = {k: [x.lstrip('cpd:').upper() for x in list(v.values())] for k, v in result.items() if len(v) > 0}
                result = {}
                for k in res.keys():
                    new_key = get_kegg_organism_abbr(k)
                    result[new_key] = res[k]
                return jsonify(result)
            except:
                return []
        else:
            return jsonify(result)


class fellaPathway(Resource):
    @swagger.operation(
        summary="generate study pathway",
        notes="Generate study pathway by fella",
        parameters=[
            {
                "name": "studyID",
                "description": "Metabolights studyID",
                "required": True,
                "allowEmptyValue": False,
                "allowMultiple": False,
                "paramType": "query",
                "dataType": "string"
            },

            {
                "name": "organism",
                "description": "study organism",
                "required": True,
                "allowEmptyValue": False,
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
                "code": 404,
                "message": "Not found. The requested identifier is not valid or does not exist."
            }
        ]
    )
    def put(self):
        log_request(request)
        

        
        studyID = None
        if request.args:
            
            studyID = request.args.get('studyID')
            if studyID:
                studyID = studyID.strip().upper()
            else:
                abort(400)

        

        org = 'hsa'
        if request.args:
            
            organism = request.args.get('organism')
            if organism:
                try:
                    org = get_kegg_organism_abbr(organism)
                except Exception as e:
                    logger.info("Can't find organism {organism} in KEGG".format(organism=organism))
                    return "Can't find organism {organism} in KEGG".format(organism=organism)
            else:
                abort(400)
        # module = "module load r-3.6.3-gcc-9.3.0-yb5n44y; module load pandoc-2.7.3-gcc-9.3.0-gctut72;"
        script = get_settings().hpc_cluster.configuration.fella_pathway_script_path
        para = '-s {studyID} -o {organism}'.format(studyID=studyID, organism=org)

        command = script + ' ' + para

        logger.info("Starting cluster job for FELLA pathway: " + command)
        status, message, job_out, job_err, log_file = submit_job(True, account=None, job_cmd=command, job_params=None, log=False)


        if status:
            return {"success": message, "message": job_out, "errors": job_err}
        else:
            return {"error": message, "message": job_out, "errors": job_err}


def match_chebi_kegg(chebiID, KeggID):
    df = pd.read_csv(convert_relative_to_real_path('resources/chebi_kegg.tsv'), sep='\t')
    df['CHEBIID_c'] = df['CHEBIID'].map(lambda x: x.lstrip('chebi:'))
    df['KEGGID_c'] = df['KEGGID'].map(lambda x: x.lstrip('cpd:'))

    chebiID = [x.lower().lstrip('chebi:') for x in chebiID]
    KeggID = [x.lower().lstrip('kegg:').upper() for x in KeggID]

    res = df[df['CHEBIID_c'].isin(chebiID) | df['KEGGID_c'].isin(KeggID)]
    return dict(zip(res.CHEBIID, res.KEGGID))


def match_hmdb_kegg(hmdbID, KeggID):
    df = pd.read_csv(convert_relative_to_real_path('resources/hmdb_kegg.tsv'), sep='\t')
    df['HMDBID_c'] = df['HMDBID'].map(lambda x: x.lstrip('hmdb:'))
    df['KEGGID_c'] = df['KEGGID'].map(lambda x: x.lstrip('cpd:'))

    # chebiID = [x.lower().lstrip('chebi:') for x in chebiID]
    KeggID = [x.lower().lstrip('kegg:').upper() for x in KeggID]

    res = df[df['HMDBID_c'].isin(hmdbID) | df['KEGGID_c'].isin(KeggID)]
    return dict(zip(res.HMDBID, res.KEGGID))


def maf_reader(studyID, maf_file_name, sample_df):
    '''
    get maf file

    :param studyID:  study ID
    :param maf_file_name: active maf file name
    :return:  dict{chebiID:[sampleNames]
    '''

    url = get_host_internal_url() + '/ws/studies/{studyID}/{maf_file_name}'.format(studyID=studyID,
                                                                                                     maf_file_name=maf_file_name)
    response = requests.get(url, headers={'user_token': get_settings().auth.service_account.api_token})
    jsonResponse = response.json()

    # get sample columns in maf
    maf_samples = list(jsonResponse['header'].keys())
    sample_list = sample_df['Sample Name'].tolist()
    maf_samples = [x for x in maf_samples if x in sample_list]

    # setup {organism1:[sampleName],organism2:[sanpleName]}
    sample_organism = dict(zip(sample_df['Sample Name'], sample_df['Characteristics[Organism]']))

    res = {}

    for row in jsonResponse['data']['rows']:
        db_id = row['database_identifier']

        for s in maf_samples:
            if row[s] != '':
                org = sample_organism[s]
                if org in res:
                    if db_id in res[org]:
                        continue
                    else:
                        res[org].append(db_id)
                else:
                    res[org] = [db_id]
    return res


def get_sample_file(studyID, sample_file_name):
    '''
    get sample file

    :param studyID: study ID
    :param sample_file_name: active sample file name
    :return:  DataFrame
    '''
    import io
    try:
        host = get_settings().server.service.mtbls_ws_host + ':' + str(get_settings().server.service.rest_api_port)
        context_path = get_settings().server.service.resources_path
        ws_url = f'{host}{context_path}/studies/{studyID}/sample'

        resp = requests.get(ws_url, headers={'user_token': get_settings().auth.service_account.api_token},
                            params={'sample_filename': sample_file_name})
        data = resp.text
        content = io.StringIO(data)
        df = pd.read_csv(content, sep='\t')
        return df
    except Exception as e:
        # logger.info(e)
        print(e)


def getFileList(studyID):
    url = get_host_internal_url() + '/ws/studies/{study_id}/files?include_raw_data=false'.format(
        study_id=studyID)
    request_obj = urllib_request.Request(url)
    request_obj.add_header('user_token', get_settings().auth.service_account.api_token)
    response = urllib_request.urlopen(request_obj)
    content = response.read().decode('utf-8')
    j_content = json.loads(content)

    assay_file, sample_file, investigation_file, maf_file = [], '', '', []
    for files in j_content['study']:
        if files['status'] == 'active' and files['type'] == 'metadata_assay':
            assay_file.append(files['file'])
            continue
        if files['status'] == 'active' and files['type'] == 'metadata_investigation':
            investigation_file = files['file']
            continue
        if files['status'] == 'active' and files['type'] == 'metadata_sample':
            sample_file = files['file']
            continue
        if files['status'] == 'active' and files['type'] == 'metadata_maf':
            maf_file.append(files['file'])
            continue

    if assay_file == []: print('Fail to load assay file from ', studyID)
    if sample_file == '': print('Fail to load sample file from ', studyID)
    if investigation_file == '': print('Fail to load investigation file from ', studyID)
    if maf_file == []: print('Fail to load maf file from ', studyID)

    return assay_file, investigation_file, sample_file, maf_file


def uniqueOrganism(studyID):
    '''
    get list of unique organism from study
    :param studyID: studyID
    :return: list of organisms
    '''
    try:
        host = get_settings().server.service.mtbls_ws_host + ':' + str(get_settings().server.service.rest_api_port)
        context_path = get_settings().server.service.resources_path
        url = f'{host}{context_path}/studies/{studyID}/organisms'
        resp = requests.get(url, headers={'user_token': get_settings().auth.service_account.api_token})
        data = resp.json()
        org = []
        for organism in data['organisms']:
            org.append(organism['Characteristics[Organism]'])
        return list(set(org))
    except:
        print('Fail to load organism from {study_id}'.format(study_id=studyID))
        return []


def get_kegg_organism_abbr(organism):
    df = pd.read_csv(convert_relative_to_real_path('resources/KEGG organism list.tsv'), sep='\t')
    try:
        res = df[df['Organisms.1'].str.lower().str.contains(organism.lower())]['Organisms'].iloc[0]
        return res
    except:
        return organism
