import glob
import os
import logging
import time
from flask_restful import abort
from isatools.convert import isatab2json
from isatools.isatab import load, dump
from app.ws.mtblsWSclient import WsClient
from app.ws.utils import copy_file, new_timestamped_folder
import json
from isatools.model import *
from isatools.isajson import ISAJSONEncoder

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

    def _get_isa_investigation(self, study_id, api_key):
        """
        Get an ISA-API Investigation object reading directly from the ISA-Tab files
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: an ISA-API Investigation object
        """
        path = self.wsc.get_study_location(study_id, api_key)
        try:
            i_filename = glob.glob(os.path.join(path, "i_*.txt"))[0]
            fp = open(i_filename)
            # isa_json = load(fp, skip_load_tables=True)
            isa_json = load(fp, skip_load_tables=False)
        except Exception:
            logger.exception("Failed to find i_*.txt file")
            abort(500)
        else:
            return isa_json

    def _get_isa_study(self, study_id, api_key):
        """
        Get the Study section from the Investigation ISA object
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: an ISA-API Study object
        """
        inv_obj = self._get_isa_investigation(study_id, api_key)
        std_obj = inv_obj.studies[0]
        return std_obj

    def get_study_title(self, study_id, api_key):
        """
        Get the Study title
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: a string with the study title
        """
        std_obj = self._get_isa_study(study_id, api_key)
        return std_obj.title

    def get_study_description(self, study_id, api_key):
        """
        Get the Study description
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: a string with the study description
        """
        std_obj = self._get_isa_study(study_id, api_key)
        return std_obj.description

    def write_study_json_title(self, study_id, api_key, new_title, save_audit_copy=True):
        """
        Write out a new Investigation file with the new Study title
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :param new_title: the new title for the Study
        :param save_audit_copy: Keep track of changes saving a copy of the unmodified files
        :return: the new title
        """
        inv_obj = self._get_isa_investigation(study_id, api_key)
        std_obj = inv_obj.studies[0]
        std_obj.title = new_title

        # write changes to ISA-tab file
        self._write_study_json(study_id, api_key, inv_obj, save_audit_copy)

        return new_title

    def write_study_json_description(self, study_id, api_key, new_description, save_audit_copy=True):
        """
        Write out a new Investigation file with the new Study title
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :param new_description: the new description for the Study
        :param save_audit_copy: Keep track of changes saving a copy of the unmodified files
        :return: the new description
        """
        inv_obj = self._get_isa_investigation(study_id, api_key)
        std_obj = inv_obj.studies[0]
        std_obj.description = new_description

        # write changes to ISA-tab file
        self._write_study_json(study_id, api_key, inv_obj, save_audit_copy)

        return new_description

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
                    raise RuntimeError("Validation error when trying to read the study.")
            else:
                logger.info('... get_isa_json() processing (I): %s sec.', time.time() - start)
                return isa_json
        else:
            logger.info('... get_isa_json() processing (II): %s sec.', time.time() - start)
            return isa_json

    def _write_study_json(self, study_id, api_key, inv_obj, save_audit_copy=True):

        std_path = self.wsc.get_study_location(study_id, api_key)

        # make a copy before applying changes
        if save_audit_copy:
            src = os.path.join(std_path, self.inv_filename)

            # dest folder name is a timestamp
            dest_path = self.wsc.get_study_updates_location(study_id, api_key)
            dest_folder = new_timestamped_folder(dest_path)
            dest = os.path.join(dest_folder, self.inv_filename)
            logger.info("Copying %s to %s", src, dest)
            copy_file(src, dest)

        # Using the new feature in isatools, implemented from issue #185
        # https://github.com/ISA-tools/isa-api/issues/185
        # isatools.isatab.dump() writes out the ISA as a string representation of the ISA-Tab,
        # skipping writing tables, i.e. only i_investigation.txt
        logger.info("Writing %s to %s", self.inv_filename, std_path)
        dump(inv_obj, std_path, i_file_name=self.inv_filename, skip_dump_tables=True)

        return

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
        extraction_protocol = Protocol(name='extraction', protocol_type=OntologyAnnotation(term="material extraction"))
        study.protocols.append(extraction_protocol)
        sequencing_protocol = Protocol(name='sequencing', protocol_type=OntologyAnnotation(term="material sequencing"))
        study.protocols.append(sequencing_protocol)
        study.assays.append(assay)

        return json.dumps(investigation, cls=ISAJSONEncoder, sort_keys=True, indent=4, separators=(',', ': '))

    def get_study_protocols(self, study_id, api_key):
        """
        Get the Study protocols
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: a string with the study protocols
        """
        std_obj = self._get_isa_study(study_id, api_key)
        protocols = std_obj.protocols
        return protocols

    def write_study_json_protocols(self, study_id, api_key, new_protocols, save_audit_copy=True):
        """
        Write out a new Investigation file with the new Study protocols
        :param study_id:
        :param api_key:
        :param new_protocols:
        :param save_audit_copy:
        :return:
        """
        inv_obj = self._get_isa_investigation(study_id, api_key)
        std_obj = inv_obj.studies[0]
        std_obj.protocols = new_protocols

        # write changes to ISA-tab file
        self._write_study_json(study_id, api_key, inv_obj, save_audit_copy)

        return std_obj.protocols

    def get_study_contacts(self, study_id, api_key):
        """
        Get the Study list of contacts
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: a string with the list of study contacts
        """
        std_obj = self._get_isa_study(study_id, api_key)
        contacts = std_obj.contacts
        return contacts

    def write_study_json_contacts(self, study_id, api_key, new_contacts, save_audit_copy=True):
        """
        Write out a new Investigation file with the new Study contacts
        :param study_id:
        :param api_key:
        :param new_contacts:
        :param save_audit_copy:
        :return:
        """
        inv_obj = self._get_isa_investigation(study_id, api_key)
        std_obj = inv_obj.studies[0]
        std_obj.contacts = new_contacts

        # write changes to ISA-tab file
        self._write_study_json(study_id, api_key, inv_obj, save_audit_copy)

        return std_obj.contacts

    def get_study_factors(self, study_id, api_key):
        """
        Get the Study list of factors
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: list of ISA StudyFactor objects.
        """
        std_obj = self._get_isa_study(study_id, api_key)
        factors = std_obj.factors
        return factors

    def write_study_json_factors(self, study_id, api_key, new_factors, save_audit_copy=True):
        """
        Write out a new Investigation file with the new Study factors
        :param study_id:
        :param api_key:
        :param new_factors:
        :param save_audit_copy:
        :return:
        """
        inv_obj = self._get_isa_investigation(study_id, api_key)
        std_obj = inv_obj.studies[0]
        std_obj.factors = new_factors

        # write changes to ISA-tab file
        self._write_study_json(study_id, api_key, inv_obj, save_audit_copy)

        return std_obj.factors

    def get_study_descriptors(self, study_id, api_key):
        """
        Get the Study list of design descriptors
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: list of design descriptors : ISA OntologyAnnotation objects.
        """
        std_obj = self._get_isa_study(study_id, api_key)
        descriptors = std_obj.design_descriptors
        return descriptors

    def write_study_json_descriptors(self, study_id, api_key, new_descriptors, save_audit_copy=True):
        """
        Write out a new Investigation file with the new Study design descriptors
        :param study_id:
        :param api_key:
        :param new_descriptors:
        :param save_audit_copy:
        :return:
        """
        inv_obj = self._get_isa_investigation(study_id, api_key)
        std_obj = inv_obj.studies[0]
        std_obj.design_descriptors = new_descriptors

        # write changes to ISA-tab file
        self._write_study_json(study_id, api_key, inv_obj, save_audit_copy)

        return std_obj.design_descriptors

    def get_study_publications(self, study_id, api_key):
        """
        Get the Study list of publications
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: list of publications
        """
        std_obj = self._get_isa_study(study_id, api_key)
        publications = std_obj.publications
        return publications

    def write_study_json_publications(self, study_id, api_key, new_publications, save_audit_copy=True):
        """
        Write out a new Investigation file with the new Study publications
        :param study_id:
        :param api_key:
        :param new_publications:
        :param save_audit_copy:
        :return:
        """
        inv_obj = self._get_isa_investigation(study_id, api_key)
        std_obj = inv_obj.studies[0]
        std_obj.publications = new_publications

        # write changes to ISA-tab file
        self._write_study_json(study_id, api_key, inv_obj, save_audit_copy)

        return std_obj.publications
