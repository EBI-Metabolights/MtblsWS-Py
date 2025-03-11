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
import io
import logging
import os
import time

from flask_restful import abort
from isatools.convert import isatab2json
from isatools.isatab import load, dump
from app.ws.settings.utils import get_study_settings

from app.ws.study import commons
from app.ws.utils import copy_file, new_timestamped_folder

"""
MetaboLights ISA-API client

Use the Python-based ISA-API tools
"""

logger = logging.getLogger('wslog')


class IsaApiClient:

    def __init__(self):
        self.settings = get_study_settings()

    @staticmethod
    def get_isa_json(study_id, api_key, study_location=None):
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
            path = commons.get_study_location(study_id, api_key)
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

    def get_isa_study(self, study_id, api_key, skip_load_tables=True, study_location=None, failing_gracefully=False):
        """
        Get an ISA-API Investigation object reading directly from the ISA-Tab files
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :param skip_load_tables: speed-up reading by skiping loading assay and sample tables
        :param study_location: filessystem location of the study
        :return: a tuple consisting in ISA-Study obj, ISA-Investigation obj
                and path to the Study in the file system
        """

        if skip_load_tables == 'false':
            skip_load_tables = False

        if study_location is None:
            logger.info("Study location is not set, will have load study from filesystem")
            std_path = commons.get_study_location(study_id, api_key)
        else:
            logger.info("Study location is: " + study_location)
            std_path = study_location

        try:
            i_filename = glob.glob(os.path.join(std_path, "i_*.txt"))[0]
            fp = open(i_filename, encoding='utf-8', errors='ignore')
            # loading tables also load Samples and Assays
            isa_inv = load(fp, skip_load_tables)
            # ToDo. Add MAF to isa_study
            isa_study = isa_inv.studies[0]
        except IndexError as e:
            logger.exception("Failed to find Investigation file %s from %s", study_id, std_path)
            logger.error(str(e))
            if failing_gracefully:
                return None, None, None
            else:
                abort(417, message=f"Error: {str(e)}")
        except AttributeError as e:
            logger.exception("AttributeError, possible corruption.")
            if self.mitigate_corrupted_inv_file(std_path, study_id, skip_load_tables):
                # if we get here it is because we have fixed the corruption and tested the load of the study
                isa_study, isa_inv = self.load_op(std_path, skip_load_tables)
                return isa_study, isa_inv, std_path
            else:
                if failing_gracefully:
                    return None, None, None
                else:
                    abort(417, message=f"Error: {str(e)}")

        except Exception as e:
            logger.exception("Failed to find Investigation file %s from %s", study_id, std_path)
            logger.error(str(e))
            if failing_gracefully:
                return None, None, None
            else:
                abort(417, message=f"Error: {str(e)}")
        else:
            return isa_study, isa_inv, std_path

    @staticmethod
    def load_op(std_path, skip_load_tables):
        i_filename = glob.glob(os.path.join(std_path, "i_*.txt"))[0]
        fp = open(i_filename, encoding='utf-8', errors='ignore')
        # loading tables also load Samples and Assays
        isa_inv = load(fp, skip_load_tables)
        # ToDo. Add MAF to isa_study
        isa_study = isa_inv.studies[0]
        return isa_study, isa_inv

    def mitigate_corrupted_inv_file(self, std_path, study_id, skip_load_tables) -> bool:
        logger.info("Attempting to correct corruption for %s", study_id)
        i_filename = self._get_investigation_filename(std_path)
        lines = self._read_file(i_filename)
        breakpoint = self._find_breakpoint(lines)

        if breakpoint == -1:
            logger.warning(f"No matching line found for study {study_id}. No changes made.")
            return False
        # Try fixing the corrupted file
        try:
            self._fix_corruption(std_path, i_filename, lines, breakpoint)
            # Assess the fix in-memory
            inmem_test = io.StringIO(''.join(lines[:breakpoint + 1]))
            self._test_fix(inmem_test, skip_load_tables)

            # If everything works, rename and update the files
            self._rename_files(i_filename, std_path)

            logger.info(f"Investigation file for study {study_id} has been successfully fixed.")
            return True

        except (IOError, ValueError) as e:
            logger.error(f"File error while processing investigation file for {study_id}: {str(e)}")
        except Exception as e:
            logger.exception(f"Unexpected error while processing investigation file for {study_id}: {str(e)}")

        return False

    def _get_investigation_filename(self, std_path: str) -> str:
        """Locate the first investigation file that matches the pattern."""
        i_filename = glob.glob(os.path.join(std_path, "i_*.txt"))
        if not i_filename:
            raise FileNotFoundError(f"No investigation file found in {std_path}")
        return i_filename[0]

    def _read_file(self, i_filename: str) -> list:
        """Read the contents of the investigation file."""
        try:
            with open(i_filename, mode='r', encoding='utf-8', errors='ignore') as inv:
                return inv.readlines()
        except IOError as e:
            raise IOError(f"Error reading file {i_filename}: {str(e)}")

    def _find_breakpoint(self, lines: list) -> int:
        """Find the line that starts with the given prefix."""
        for idx, line in enumerate(lines):
            if line.startswith('Study Person Roles Term Source REF'):
                return idx
        return -1

    def _fix_corruption(self, std_path: str, i_filename: str, lines: list, breakpoint: int):
        """Fix the corruption by creating a temp file and writing correct lines."""
        tmp_filename = os.path.join(std_path, f'tmp_{os.path.basename(i_filename)}')
        try:
            with open(tmp_filename, mode='w') as tmp_file:
                tmp_file.writelines(lines[:breakpoint + 1])
        except IOError as e:
            raise IOError(f"Failed to write temporary file {tmp_filename}: {str(e)}")

    def _test_fix(self, inmem_test: io.StringIO, skip_load_tables: bool):
        """Test if the fix was successful by loading the file in memory."""
        try:
            isa_inv = load(inmem_test, skip_load_tables)
        except ValueError as e:
            raise ValueError(f"Integrity test failed for the fixed file: {str(e)}")

    def _rename_files(self, i_filename: str, std_path: str):
        """Rename and replace the corrupted file."""
        try:
            tmp_filename = os.path.join(std_path, f'tmp_{os.path.basename(i_filename)}')
            os.rename(tmp_filename, i_filename)
        except OSError as e:
            raise OSError(f"Failed to rename or move files: {str(e)}")

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
        dump(inv_obj, std_path, i_file_name=i_file_name, skip_dump_tables=False)

        return
