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

from flask import request, abort
from flask.json import jsonify
from flask_restful import Resource, reqparse
from flask_restful_swagger import swagger
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import *
from isatools.isatab import dump
from isatools.model import *
from metaspace.sm_annotation_utils import *
from collections import OrderedDict
import json
import config
import boto3
import time
import errno
import sys
import csv
import getopt
import getopt

class MetaSpaceIsaApiClient(Resource):

    def __init__(self):
        self.inv_filename = "i_Investigation.txt"

        return

    def _write_study_json(self, inv_obj, std_path, skip_dump_tables=True):

        # Using the new feature in isatools, implemented from issue #185
        # https://github.com/ISA-tools/isa-api/issues/185
        # isatools.isatab.dump() writes out the ISA as a string representation of the ISA-Tab,
        # skipping writing tables, i.e. only i_investigation.txt
        logger.info("Writing %s to %s", self.inv_filename, std_path)
        try:
            os.makedirs(std_path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
        inv = dump(inv_obj, std_path, i_file_name=self.inv_filename, skip_dump_tables=skip_dump_tables)

        return inv

    def new_study(self, std_title, std_description, mtspc_obj, output_dir, persist=False):
        print("Creating ISA-Tab investigation file.")

        # investigation file
        investigation = Investigation(filename="i_investigation.txt")
        investigation.title = ""
        investigation.description = ""
        investigation.submission_date = time.strftime("%d-%m-%Y")
        investigation.public_release_date = time.strftime("%d-%m-%Y")

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

        # study file
        study = Study(filename="s_study.txt")
        # study.identifier = "s1"
        study.title = std_title
        study.description = std_description
        study.submission_date = time.strftime("%d-%m-%Y")
        study.public_release_date = time.strftime("%d-%m-%Y")

        # If different submitters, PI becomes submitter
        if ppal_inv['name'] != submitter['name']:
            investigation.contacts.append(ct_ppal_inv)
            investigation.contacts.append(ct_submitter)
            study.contacts.append(ct_ppal_inv)
            study.contacts.append(ct_submitter)
        else:
            investigation.contacts.append(ct_ppal_inv)
            investigation.contacts.append(ct_submitter)
            study.contacts.append(ct_ppal_inv)
            study.contacts.append(ct_submitter)

        investigation.studies.append(study)

        # CONNECT TO METASPACE SERVICES
        database = config.DATABASE
        fdr = config.FDR
        sm = SMInstance()  # connect to the main metaspace service
        #  db = sm._moldb_client.getDatabase(database)  # connect to the molecular database service

        # assay file
        assay = Assay(filename="a_assay.txt")

        for sample in mtspc_obj:
            metaspace_options = sample['metaspace_options']
            ds_name = metaspace_options['Dataset_Name']
            ds = sm.dataset(name=ds_name)

            assay.samples.append(sample)


        # extraction_protocol = Protocol(name='extraction', protocol_type=OntologyAnnotation(term="material extraction"))
        # study.protocols.append(extraction_protocol)
        # sequencing_protocol = Protocol(name='sequencing', protocol_type=OntologyAnnotation(term="material sequencing"))
        # study.protocols.append(sequencing_protocol)
        study.assays.append(assay)

        if persist:
            self._write_study_json(investigation, output_dir, skip_dump_tables=False)

        return investigation