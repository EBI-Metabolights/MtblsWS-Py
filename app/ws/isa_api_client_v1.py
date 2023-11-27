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
import logging
import os
import time

from flask_restful import abort
from app.ws.isa_tools.isatab import dump
from app.ws.settings.utils import get_study_settings

from app.ws.study import commons
from app.ws.utils import copy_file, new_timestamped_folder

"""
MetaboLights ISA-API client using latest ISA-Tools version (0.14.2)
"""

logger = logging.getLogger('wslog')


class IsaApiClientV1:

    def __init__(self):
        self.settings = get_study_settings()

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
        study_id = os.path.basename(std_path)
        settings = self.settings
        
        if save_investigation_copy or save_samples_copy or save_assays_copy:  # Only create audit folder when requested
            update_path = os.path.join(settings.mounted_paths.study_audit_files_root_path, study_id, settings.audit_folder_name)

            dest_path = new_timestamped_folder(update_path)

            # make a copy before applying changes
            if save_investigation_copy:
                src_file = os.path.join(std_path, settings.investigation_file_name)
                dest_file = os.path.join(dest_path, settings.investigation_file_name)
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

        logger.info("Writing %s to %s", settings.investigation_file_name, std_path)
        i_file_name = settings.investigation_file_name
        dump(inv_obj, std_path, i_file_name=i_file_name, skip_dump_tables=True)
        return