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

import glob
import os
import time
from flask_restful import abort
from isatools.convert import isatab2json
from isatools.isatab import load, dump
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import copy_file, new_timestamped_folder
from isatools.model import *
from flask import current_app as app

"""
MetaboLights ISA-API client

Use the Python-based ISA-API tools
"""

logger = logging.getLogger('wslog')


class IsaApiClient:

    def __init__(self):
        self.inv_filename = "i_Investigation.txt"
        self.wsc = WsClient()   # MetaboLights (Java-Based) WebService client

        return

    def get_isa_json(self, study_id, api_key, study_location=None):
        """
        Get an ISA-API Investigation object reading directly from the ISA-Tab files
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :param study_location: The filesystem location of the study
        :return: an ISA-API Investigation object
        """
        start = time.time()

        if study_location is None:
            logger.info("Study location is not set, will have load study from filesystem")
            path = self.wsc.get_study_location(study_id, api_key)
        else:
            logger.info("Study location is: " + study_location)
            path = study_location

        # try the new parser first
        isa_json = None
        try:
            isa_json = isatab2json.convert(path, validate_first=False, use_new_parser=True)
        except Exception:  # on failure, use the old one
            try:
                isa_json = isatab2json.convert(path, validate_first=False, use_new_parser=False)
            except Exception:
                # if it fails too
                if isa_json is None:
                    abort(500)
            else:
                logger.info('... get_isa_json() processing (I): %s sec.', time.time() - start)
                return isa_json
        else:
            logger.info('... get_isa_json() processing (II): %s sec.', time.time() - start)
            return isa_json

    @staticmethod
    def create_new_study(title, description, sub_date, pub_rel_date, mtbls_accession, technology):
        """
        Create a new MTBLS Study
        :param title: 
        :param description: 
        :param sub_date: Submission date (now)
        :param pub_rel_date: Public release date
        :param mtbls_accession: MTBLS id
        :param technology: MS or NMR
        :return: an ISA-JSON representation of the Study
        """
        inv_file_name = 'i_investigation.txt'
        study_file_name = 's_study.txt'
        assay_file_name = 'a_assay.txt'

        if mtbls_accession is not None:
            study_file_name = 's_' + mtbls_accession + '_' + technology + '.txt'
            assay_file_name = 'a_' + mtbls_accession + '_' + technology + '.txt'

        # investigation file
        investigation = Investigation(filename=inv_file_name)
        investigation.title = title
        investigation.description = description
        investigation.submission_date = sub_date
        investigation.public_release_date = pub_rel_date
        # study file
        study = Study(filename=study_file_name)
        study.identifier = mtbls_accession
        study.title = title
        study.description = description
        study.submission_date = sub_date
        study.public_release_date = pub_rel_date

        # investigation.studies.append(study)

        protocol = Protocol()

        # assay file
        assay = Assay(filename=assay_file_name)
        assay.technology_platform = technology
        # TODO, ontology term
        # assay.technology_type = technology
        study.assays.append(assay)

        # Add it all together
        investigation.studies.append(study)

        return investigation

    def get_isa_study(self, study_id, api_key, skip_load_tables=True, study_location=None):
        """
        Get an ISA-API Investigation object reading directly from the ISA-Tab files
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :param skip_load_tables: speed-up reading by skiping loading assay and sample tables
        :param study_location: filessystem location of the study
        :return: a tuple consisting in ISA-Study obj, ISA-Investigation obj
                and path to the Study in the file system
        """

        if study_location is None:
            logger.info("Study location is not set, will have load study from filesystem")
            std_path = self.wsc.get_study_location(study_id, api_key)
        else:
            logger.info("Study location is: " + study_location)
            std_path = study_location

        try:
            i_filename = glob.glob(os.path.join(std_path, "i_*.txt"))[0]
            fp = open(i_filename, encoding='utf-8', errors='ignore')
            # loading tables also load Samples and Assays
            isa_inv = load(fp, skip_load_tables)
            isa_study = isa_inv.studies[0]
        except IndexError as e:
            logger.exception("Failed to find Investigation file from %s", study_id, std_path)
            logger.error(str(e))
            abort(400)
        except Exception as e:
            logger.exception("Failed to find Investigation file from %s", study_id, std_path)
            logger.error(str(e))
            abort(400)
        else:
            return isa_study, isa_inv, std_path

    def write_isa_study(self, inv_obj, api_key, std_path,
                        save_investigation_copy=True, save_samples_copy=False, save_assays_copy=False):
        """
        Write back an ISA-API Investigation object directly into ISA-Tab files
        :param inv_obj: ISA-API Investigation object
        :param api_key: User API key for accession check
        :param std_path: file system path to destination folder
        :param save_investigation_copy: Keep track of changes saving a copy of the unmodified i_*.txt file
        :param save_samples_copy: Keep track of changes saving a copy of the unmodified s_*.txt file
        :param save_assays_copy: Keep track of changes saving a copy of the unmodified a_*.txt and m_*.tsv files
        :return:
        """
        # dest folder name is a timestamp
        update_path_suffix = app.config.get('UPDATE_PATH_SUFFIX')
        update_path = os.path.join(std_path, update_path_suffix)
        if save_investigation_copy or save_samples_copy or save_assays_copy:  # Only create audit folder when requested
            dest_path = new_timestamped_folder(update_path)

            # make a copy before applying changes
            if save_investigation_copy:
                src_file = os.path.join(std_path, self.inv_filename)
                dest_file = os.path.join(dest_path, self.inv_filename)
                logger.info("Copying %s to %s", src_file, dest_file)
                copy_file(src_file, dest_file)

            if save_samples_copy:
                for sample_file in glob.glob(os.path.join(std_path, "s_*.txt")):
                    sample_file_name = os.path.basename(sample_file)
                    src_file = sample_file
                    dest_file = os.path.join(dest_path, sample_file_name)
                    logger.info("Copying %s to %s", src_file, dest_file)
                    copy_file(src_file, dest_file)

            if save_assays_copy:
                for assay_file in glob.glob(os.path.join(std_path, "a_*.txt")):
                    assay_file_name = os.path.basename(assay_file)
                    src_file = assay_file
                    dest_file = os.path.join(dest_path, assay_file_name)
                    logger.info("Copying %s to %s", src_file, dest_file)
                    copy_file(src_file, dest_file)
                # Save the MAF
                for maf in glob.glob(os.path.join(std_path, "m_*.tsv")):
                    maf_file_name = os.path.basename(maf)
                    src_file = maf
                    dest_file = os.path.join(dest_path, maf_file_name)
                    logger.info("Copying %s to %s", src_file, dest_file)
                    copy_file(src_file, dest_file)

        logger.info("Writing %s to %s", self.inv_filename, std_path)
        i_file_name = self.inv_filename
        dump(inv_obj, std_path, i_file_name=i_file_name, skip_dump_tables=False)

        return
