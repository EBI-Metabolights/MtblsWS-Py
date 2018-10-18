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

    def get_isa_json(self, study_id, api_key):
        """
        Get an ISA-API Investigation object reading directly from the ISA-Tab files
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: an ISA-API Investigation object
        """
        start = time.time()

        path = self.wsc.get_study_location(study_id, api_key)
        # try the new parser first
        isa_json = None
        try:
            isa_json = isatab2json.convert(path, validate_first=False, use_new_parser=True)
        except Exception as inst:  # on failure, use the old one
            try:
                isa_json = isatab2json.convert(path, validate_first=False, use_new_parser=False)
            except Exception as inst:
                # if it fails too
                if isa_json is None:
                    abort(500)
            else:
                logger.info('... get_isa_json() processing (I): %s sec.', time.time() - start)
                return isa_json
        else:
            logger.info('... get_isa_json() processing (II): %s sec.', time.time() - start)
            return isa_json

    def create_new_study(self, title, description, sub_date, pub_rel_date):
        """
        Create a new MTBLS Study
        :param title: 
        :param description: 
        :param sub_date: 
        :param pub_rel_date: 
        :return: an ISA-JSON representation of the Study
        """
        # investigation file
        investigation = Investigation(filename="i_investigation.txt")
        investigation.title = title
        investigation.description = description
        investigation.submission_date = sub_date
        investigation.public_release_date = pub_rel_date
        # study file
        study = Study(filename="s_study.txt")
        study.identifier = "s1"
        study.title = title
        study.description = description
        study.submission_date = sub_date
        study.public_release_date = pub_rel_date
        investigation.studies.append(study)
        # assay file
        assay = Assay(filename="a_assay.txt")
        study.assays.append(assay)

        return investigation

    def get_isa_study(self, study_id, api_key, skip_load_tables=False):
        """
        Get an ISA-API Investigation object reading directly from the ISA-Tab files
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :param skip_load_tables: speed-up reading by skiping loading assay and sample tables
        :return: a tuple consisting in ISA-Study obj, ISA-Investigation obj
                and path to the Study in the file system
        """
        std_path = self.wsc.get_study_location(study_id, api_key)
        try:
            i_filename = glob.glob(os.path.join(std_path, "i_*.txt"))[0]
            fp = open(i_filename)
            # loading tables also load Samples and Assays
            isa_inv = load(fp, skip_load_tables)
            isa_study = isa_inv.studies[0]
        except IndexError:
            logger.exception("Failed to find Investigation file from %s", study_id, std_path)
            abort(500)
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
            # Also save the MAF
            for maf in glob.glob(os.path.join(std_path, "m_*.tsv")):
                maf_file_name = os.path.basename(maf)
                src_file = maf
                dest_file = os.path.join(dest_path, maf_file_name)
                logger.info("Copying %s to %s", src_file, dest_file)
                copy_file(src_file, dest_file)

        logger.info("Writing %s to %s", self.inv_filename, std_path)
        dump(inv_obj, std_path, i_file_name=self.inv_filename, skip_dump_tables=False)

        return
