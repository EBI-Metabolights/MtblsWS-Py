#  EMBL-EBI MetaboLights - https://www.ebi.ac.uk/metabolights
#  Metabolomics team
#
#  European Bioinformatics Institute (EMBL-EBI), European Molecular Biology Laboratory, Wellcome Genome Campus, Hinxton, Cambridge CB10 1SD, United Kingdom
#
#  Last modified: 2019-Aug-28
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

from flask_restful import Resource
from flask import abort
from app.ws.utils import *
from isatools.isatab import dump
from isatools.model import *
from app.ws.isaApiClient import IsaApiClient
from app.ws.mtblsWSclient import WsClient
import time
import errno

isa_api = IsaApiClient()
wsc = WsClient()


class MetaSpaceIsaApiClient(Resource):

    def __init__(self):
        self.inv_filename = "i_Investigation.txt"

        return

    def _write_study_json(self, inv_obj, std_path, skip_dump_tables=True):
        logger.info("Writing %s to %s", self.inv_filename, std_path)
        try:
            os.makedirs(std_path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
        inv = dump(inv_obj, std_path, i_file_name=self.inv_filename, skip_dump_tables=skip_dump_tables)

        return inv

    def new_study(self, std_title, std_description, mtspc_obj, output_dir,
                  study_id=None, user_token=None, obfuscation_code=None, persist=False):
        print("Creating ISA-Tab investigation file.")
        isa_study = None
        isa_inv = None
        ppal_inv = None
        ct_ppal_inv = None

        try:
            # status, message = convert_to_isa(output_dir, study_id)
            isa_study, isa_inv, std_path = isa_api.get_isa_study(study_id, user_token, skip_load_tables=True,
                                                                 study_location=output_dir)
        except Exception as e:
            logger.warning("Could not find ISA-Tab files, creating new study. " + str(e))

        if not isa_inv:
            try:
                study_path = app.config.get('STUDY_PATH')
                from_path = study_path + 'DUMMY'
                to_path = output_dir
                copy_files_and_folders(from_path, to_path, include_raw_data=True, include_investigation_file=True)
            except Exception as e:
                logger.error('Could not copy files from %s to %s, Error ', from_path, to_path, str(e))
                abort(409, "Something went wrong with copying the ISA-Tab templates to study " + str(study_id))

            # Create upload folder
            status = wsc.create_upload_folder(study_id, obfuscation_code, user_token)

            isa_study, isa_inv, std_path = isa_api.get_isa_study(study_id, user_token, skip_load_tables=True,
                                                                 study_location=output_dir)
            # Also make sure the sample file is in the standard format of 's_MTBLSnnnn.txt'
            isa_study, sample_file_name = update_correct_sample_file_name(isa_study, output_dir, study_id)
        else:
            # investigation file changes
            if not isa_inv.title:
                isa_inv.title = std_title
            if not isa_inv.description:
                isa_inv.description = std_description
            if not isa_inv.submission_date:
                isa_inv.submission_date = time.strftime("%d-%m-%Y")
            if not isa_inv.public_release_date:
                isa_inv.public_release_date = time.strftime("%d-%m-%Y")

            submittedby = mtspc_obj[0]['Submitted_By']
            ppal_inv = submittedby['Principal_Investigator']
            submitter = submittedby['Submitter']
            names = submitter.get('name').split()
            first_name = names[0]
            last_name = names[1]

            ct_ppal_inv = Person(first_name=first_name,
                                 last_name=last_name,
                                 affiliation=submittedby['Institution']['name'],
                                 # email=ppal_inv['Email'],
                                 roles=[OntologyAnnotation(term='submitter'),
                                        OntologyAnnotation(term='principal investigator role')])
            ct_submitter = Person(first_name=first_name,
                                  last_name=last_name,
                                  affiliation=submittedby['Institution']['name'],
                                  # email=submitter['Email'],
                                  roles=[OntologyAnnotation(term='submitter')])

        if isa_study:
            # study file
            if not isa_study.title:
                isa_study.title = std_title
            if not isa_study.description:
                isa_study.description = std_description
            if not isa_study.submission_date:
                isa_study.submission_date = time.strftime("%d-%m-%Y")
            if not isa_study.public_release_date:
                isa_study.public_release_date = time.strftime("%d-%m-%Y")

            # If different submitters, PI becomes submitter
            if ppal_inv['name'] != submitter['name']:
                isa_inv.contacts.append(ct_ppal_inv)
                isa_inv.contacts.append(ct_submitter)
                isa_study.contacts.append(ct_ppal_inv)
                isa_study.contacts.append(ct_submitter)
            else:
                isa_inv.contacts.append(ct_ppal_inv)
                isa_inv.contacts.append(ct_submitter)
                isa_study.contacts.append(ct_ppal_inv)
                isa_study.contacts.append(ct_submitter)

        # isa_inv.studies = isa_study

        # CONNECT TO METASPACE SERVICES
        # database = config.METASPACE_DATABASE
        # fdr = config.METASPACE_FDR
        # sm = SMInstance()  # connect to the main metaspace service
        # db = sm._moldb_client.getDatabase(database)  # connect to the molecular database service

        # for assay in isa_study.assay:
        #     # assay file
        #     for sample in mtspc_obj:
        #         metaspace_options = sample['metaspace_options']
        #         ds_name = metaspace_options['Dataset_Name']
        #         # ds = sm.dataset(name=ds_name)
        #         assay.samples.append(sample)

        if persist:
            isa_api.write_isa_study(isa_inv, user_token, output_dir,
                                    save_investigation_copy=True, save_samples_copy=True, save_assays_copy=True)
            # self._write_study_json(isa_inv, output_dir, skip_dump_tables=False)

        return isa_inv
