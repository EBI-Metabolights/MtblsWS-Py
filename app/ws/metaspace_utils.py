#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2020-Feb-28
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

import configparser
import csv
import json
import logging
import os
import sys
from collections import OrderedDict
from functools import lru_cache

import boto3
import requests
from flask import current_app as app
from app.config import get_settings

from app.ws.metaspace_isa_api_client import MetaSpaceIsaApiClient

logger = logging.getLogger('wslog')




class AwsCredentials(object):

    def __init__(self):
        self.aws_access_key_id = ''
        self.aws_secret_access_key = ''
        self.bucket = ''

        self.read_aws_credentials()
        return

    def read_aws_credentials(self):
        account = get_settings().metaspace.connection
        self.aws_access_key_id = account.access_key_id
        self.aws_secret_access_key = account.secret_access_key
        self.bucket = account.bucket

    @property
    def get_access_key(self):
        return self.aws_access_key_id

    @property
    def get_secret_access_key(self):
        return self.aws_secret_access_key

    @property
    def get_bucket(self):
        return self.bucket



@lru_cache(1)
def get_s3():
    aws_cred = AwsCredentials()
    session = boto3.Session(aws_cred.get_access_key, aws_cred.get_secret_access_key)
    s3 = session.resource('s3')
    bucket = s3.Bucket(aws_cred.get_bucket)
    return s3


def aws_download_file(bucket_name, aws_path, aws_file_name, data_type='binary'):
    aws_bucket = get_s3().Bucket(bucket_name)
    source = os.path.join(aws_path, aws_file_name)
    obj = aws_bucket.Object(source)
    logger.info("Downloading %s %s", bucket_name, source)
    body = None
    try:
        if data_type == 'utf-8':
            body = obj.get()['Body'].read().decode('utf-8')
        else:
            body = obj.get()['Body'].read()
    except Exception:
        logger.warning("Failed to download %s", source)

    return body


def print_need_additional_params(missing, options_help, exit_code=1):
    print()
    print('=> Missing required parameters:')
    for param in missing:
        print('\t', param)
    print()
    print('Usage: python ' + os.path.basename(sys.argv[0]) + options_help)
    exit(exit_code)


def print_mtspc_obj(mtspc_obj):
    for sample in mtspc_obj:
        for key, value in sample.items():
            print(key, value)
        print()


def aws_download_files(mtspc_obj, output_dir, extension, data_type='binary', use_path=False):
    for sample in mtspc_obj:
        aws_bucket, aws_path, file_name = get_filename_parts(sample, extension)
        logger.info("Getting file %s %s %s (%s)", aws_bucket, aws_path, file_name, data_type)
        path = os.path.join(output_dir, aws_path) if use_path else output_dir
        if not os.path.isfile(os.path.join(output_dir, file_name)):
            file = aws_download_file(aws_bucket, aws_path, file_name, data_type)
            if file:
                save_file(file, path, file_name, data_type)


def parse(filename):
    assert os.path.exists(filename), "Did not find json input file: %s" % filename
    with open(filename, 'r', encoding='utf-8') as data_file:
        json_data = json.load(data_file)
    return json_data


def get_filename_parts(sample_data, key):
    s3dir = sample_data['s3dir']
    value = s3dir[key]
    bucket_name = value.split('/')[0]
    file_name = value.split('/')[-1]
    aws_path = os.path.dirname(value).replace(bucket_name + '/', '')
    return bucket_name, aws_path, file_name


def save_file(content, path, filename, data_type='text'):
    if not os.path.exists(path):
        os.makedirs(path)
    mode = 'w'
    if data_type == 'binary':
        mode = 'wb'
    logger.info("Saving file %s %s (%s)", path, filename, data_type)
    with open(os.path.join(path, filename), mode) as data_file:
        data_file.write(content)


def aws_get_annotations(mtspc_obj, output_dir, database=None, fdr=None, sm_instance=None):
    settings = get_settings()
    if not database:
        database = settings.metaspace.configuration.metaspace_database
    if not fdr:
        fdr = settings.metaspace.configuration.metaspace_fdr

    filename = 'annotations'

    for sample in mtspc_obj:
        metaspace_options = sample['metaspace_options']
        ds_name = metaspace_options['Dataset_Name']
        ds = sm_instance.dataset(name=ds_name)
        # print('Dataset name: ', ds_name)
        # print('Dataset id: ', ds.id)
        # print('Dataset config: ', ds.config)
        # print('Dataset DBs: ', ds.databases)
        # print('Dataset adducts: ', ds.adducts)
        # print('Dataset metadata: ', ds.metadata.json)
        # print('Dataset polarity: ', ds.polarity)
        # print('Dataset results: ', ds.results())

        print()

        for an in ds.annotations(fdr=fdr, database=database):
            # print(an)

            # nms = db.names(an[0])
            # ids = db.ids(an[0])

            img = ds.isotope_images(sf=an[0], adduct=an[1])[0]  # get image for this molecule's principle peak
            mii = img[img > 0].mean()  # mean image intensity

            institution = sample['Submitted_By']['Institution']
            dataset_name = ds_name
            formula = an[0]
            adduct = ds.adducts[0]
            mz = ''
            msm = str(mii)
            fdr = ''
            rho_spatial = ''
            rho_spectral = ''
            rho_chaos = ''
            # molecule_names = nms

            annotations = OrderedDict([
                ('institution', institution),
                ('datasetName', dataset_name),
                ('formula', formula),
                ('adduct', adduct),
                ('mz', mz),
                ('msm', msm),
                ('fdr', fdr),
                ('rhoSpatial', rho_spatial),
                ('rhoSpectral', rho_spectral),
                ('rhoChaos', rho_chaos)  # ,('moleculeNames', molecule_names)
            ])

            # JSON file
            json_obj = json.dumps(annotations)
            save_file(json_obj, output_dir, filename + '.json', data_type='text')

            # Tab separated file
            with open(os.path.join(output_dir, filename + '.tsv'), "w") as f:
                writer = csv.writer(f, delimiter='\t')
                writer.writerow(annotations.keys())
                writer.writerow(annotations.values())
            f.close()

            return


def get_metadata(dataset_ids, output_dir, sm_instance=None):
    annotation_json = None
    info_json = None

    if dataset_ids and sm_instance:
        db = sm_instance
        annos = []
        infos = []
        metas = []
        dataset = None

        for ds_id in dataset_ids:
            try:
                dataset = db.dataset(id=ds_id)
            except:
                logger.error('Could not find dataset ' + ds_id + ' in the METASPACE database')
                continue
            settings = get_settings()
            fdr = settings.metaspace.configuration.metaspace_fdr
            db_name = settings.metaspace.configuration.metaspace_database
            annotation = db.get_annotations(fdr=fdr, db_name=db_name,
                                            datasetFilter={'ids': ds_id})
            annotation_json = annotation.to_json(orient='records')
            annos.append({ds_id: tidy_chars(annotation_json)})

            if dataset:
                info_json = json.dumps(dataset._info)
                infos.append({ds_id: tidy_chars(info_json)})

                if dataset.metadata:
                    meta_json = dataset.metadata.json  # json.dumps(dataset.metadata.json)
                    metas.append({ds_id: tidy_chars(meta_json)})

        # JSON files
        if annotation_json:
            save_json_files(annotation_json, output_dir, ds_id+'_annotations.json')

        if info_json:
            save_json_files(info_json, output_dir, ds_id+'_information.json')

        if dataset.metadata.json:
            save_json_files(dataset.metadata.json, output_dir, ds_id+'_metadata.json')

    return


def save_json_files(content, output_dir, file_name):
    # JSON files
    save_file(content, output_dir, file_name, data_type='text')


def tidy_chars(text):
    t2 = text.replace('"{', '{')
    t2 = t2.replace('}"', '},')
    t2 = t2.replace('\\', '')
    return t2


def aws_get_images(mtspc_obj, output_dir, use_path=False, sm_instance=None):
    # sm = SMInstance()

    for sample in mtspc_obj:
        metaspace_options = sample['metaspace_options']
        ds_name = metaspace_options['Dataset_Name']
        ds = sm_instance.dataset(name=ds_name)
        ds_img = ds._gqclient.getRawOpticalImage(ds.id)
        opt_im = ds_img['rawOpticalImage']

        img_name = None
        img_url = None

        if opt_im:
            path = opt_im['url']
            img_url = ds._baseurl + path
            img_folder = os.path.dirname(path)
            img_name = os.path.basename(path)
        if img_name and img_url and not img_name == 'null':
            logger.info("Getting file %s", img_url)
            img_data = requests.get(img_url).content
            if img_data:
                out_path = output_dir + img_folder if use_path else output_dir
                save_file(content=img_data,
                          path=out_path,
                          filename=img_name + '.jpg', data_type='binary')


def get_study_json(ds_ids, output_dir, std_title, sm_instance=None):
    std_json = []
    for ii, ds_id in enumerate(ds_ids):
        logger.info("Getting JSON information for %s", ds_id)
        try:
            ds = sm_instance.dataset(id=ds_id)
        except:
            logger.error('Could not find dataset ' + ds_id + ' in the METASPACE database')
            continue
        me = json.loads(ds.metadata.json)
        path = ds.s3dir[6:]  # strip s3a://
        bucket_name, ds_name = path.split('/', 1)
        bucket = get_s3().Bucket(bucket_name)
        me['metaspace_options'] = {}
        me['metaspace_options']['Dataset_Name'] = ds.name

        info = ds._info
        submitter = info.get('submitter')
        me['Submitted_By'] = {}
        me['Submitted_By']['Submitter'] = submitter
        me['Submitted_By']['Institution'] = info.get('group')

        pi = info.get('principalInvestigator')
        if not pi:
            pi = submitter

        me['Submitted_By']['Principal_Investigator'] = pi
        me['s3dir'] = {}
        for obj in bucket.objects.filter(Prefix=path.split('/')[1]):
            if obj.key.endswith('.imzML'):
                me['s3dir']['imzML'] = path + "/" + obj.key.split('/')[-1]
            if obj.key.endswith('.ibd'):
                me['s3dir']['ibd'] = path + "/" + obj.key.split('/')[-1]
        std_json.append(me)

    save_file(json.dumps(std_json), output_dir, std_title + '.json', data_type='text')
    return std_json


def get_all_files(ds_ids, file_types, output_dir, use_path=False, sm_instance=None):
    for ii, ds_id in enumerate(ds_ids):
        logger.info("Getting all files for %s", ds_id)
        try:
            ds = sm_instance.dataset(id=ds_id)
        except:
            logger.error('Could not find dataset ' + ds_id + ' in the METASPACE database')
            continue
        aws_path = ds.s3dir[6:]  # strip s3a://
        out_path = os.path.join(output_dir, aws_path) if use_path else output_dir
        bucket_name, ds_name = aws_path.split('/', 1)
        aws_bucket = get_s3().Bucket(bucket_name)
        pref_filter = ds_name
        for obj in aws_bucket.objects.filter(Prefix=pref_filter):
            for suffix in file_types:
                if obj.key.endswith(suffix):
                    file_name = obj.key.split('/')[-1]
                    if not os.path.isfile(os.path.join(out_path, file_name)):
                        file = aws_download_file(bucket_name, ds_name, file_name)
                        if file:
                            save_file(file, out_path, file_name, data_type='binary')


def annotate_metaspace(study_id=None, sm=None, metaspace_projects=None, metaspace_datasets=None):
    user_projects = []  # To store the users projects in METASPACE
    user_project_ids = []
    user_ds_ids = []
    user_ds_ids_provided = []
    if not metaspace_projects:
        metaspace_projects = []

    # Get user's projects from the METASPACE database
    user_projs = sm.projects.get_my_projects()

    # Get all datasets in project (including other users)
    # NOTE: "status='FINISHED'" filters out failed datasets - normally users don't see failed datasets
    # through the UI, so this should help prevent confusion.
    for project in user_projs:
        ms_id = project['id']
        user_project_ids.append(ms_id)
        user_projects.append({"id": ms_id, "name": project['name']})
    else:
        metaspace_projects = metaspace_projects.split(',')  # User provided string

    for project_id in metaspace_projects:
        for ms_projs in user_projects:
            ms_proj_id = ms_projs['id']
            ms_proj_name = ms_projs['name']
            if project_id in ms_proj_name:  # the user gives us the name, but we the internal id

                # Add a link from a project back to MetaboLights
                # Note that the user must be a project manager in the project to do this
                sm.projects.add_project_external_link(ms_proj_id, 'MetaboLights',
                                                      'https://www.ebi.ac.uk/metabolights/' + study_id)

                # Get current user's datasets in project
                # project_id = metaspace_projects[0]['id']
                # user_id = sm.current_user_id()
                # datasets = sm.datasets(project=project_id, submitter=user_id, status='FINISHED')
                # datasets = sm.datasets(project=ms_proj_id, status='FINISHED')

                # Get all current user's datasets for this project
                ms_datasets = sm.datasets(project=ms_proj_id, status='FINISHED')

                # Add a link from a dataset back to MetaboLights
                # Note that the user must be the submitter of the dataset to do this
                for data_set in ms_datasets:
                    if data_set.id:
                        ds_id = data_set.id
                        user_ds_ids.append(ds_id)
                        # Did the user give us this specific dataset id? If so let's annotate it
                        if metaspace_datasets and ds_id in metaspace_datasets:
                            user_ds_ids_provided.append(ds_id)
                            sm.add_dataset_external_link(ds_id, 'MetaboLights',
                                                         'https://www.ebi.ac.uk/metabolights/' + study_id)
        if user_ds_ids_provided:
            user_ds_ids = user_ds_ids_provided
    return user_project_ids, user_ds_ids


def import_metaspace(study_id=None, dataset_ids=None, study_location=None, user_token=None,
                     obfuscation_code=None, sm_instance=None):
    mtspc_obj = None
    input_folder = study_location
    # TODO: DOWNLOAD METASPACE FILES to correct path
    output_dir = study_location
    std_title = "Please update title of this METASPACE generated study " + study_id
    std_description = "Please update abstract of this METASPACE generated study " + study_id
    use_path = False
    get_study_json(dataset_ids, output_dir, study_id, sm_instance=sm_instance)
    input_file = os.path.join(input_folder, study_id + ".json")
    get_metadata(dataset_ids, output_dir, sm_instance=sm_instance)

    if os.path.isfile(input_file):
        mtspc_obj = parse(input_file)

    if mtspc_obj:
        aws_download_files(mtspc_obj, output_dir, 'imzML', data_type='utf-8', use_path=use_path)
        aws_download_files(mtspc_obj, output_dir, 'ibd', data_type='binary', use_path=use_path)
        aws_get_annotations(mtspc_obj, output_dir, sm_instance=sm_instance)
        aws_get_images(mtspc_obj, output_dir, use_path=use_path, sm_instance=sm_instance)

    get_all_files(dataset_ids, ['.imzML', '.ibd', '.jpg', '.jpeg', '.png'], output_dir,
                  use_path=use_path, sm_instance=sm_instance)
    # get_all_files(study_ids, ['.jpg', '.jpeg', '.png'], output_dir, use_path=use_path)

    iac = MetaSpaceIsaApiClient()
    inv = iac.new_study(std_title, std_description, mtspc_obj, output_dir,
                        study_id=study_id, user_token=user_token, obfuscation_code=obfuscation_code, persist=True)
    return inv
