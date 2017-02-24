import glob
import os
from flask import abort
# TODO here we are using the develop branch of isatools. Replace with the pip version when released.
from isatools.convert import isatab2json
from isatools.isatab import load, dump
from app.ws.mtblsWSclient import WsClient

"""
MetaboLights ISA-API client

Use the Python-based ISA-API tools

author: jrmacias@ebi.ac.uk
date: 20170112
"""

# MetaboLights (Java-Based) WebService client
wsc = WsClient()


class IsaApiClient:

    def get_isa_investigation(self, study_id, api_key):
        """
        Get an ISA-API Investigation object reading directly from the ISA-Tab files
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: an ISA-API Investigation object
        """
        path = wsc.get_study_location(study_id, api_key)
        try:
            i_filename = glob.glob(os.path.join(path, "i_*.txt"))[0]
            fp = open(i_filename)
            isa_json = load(fp, skip_load_tables=True)
        except Exception:
            abort(500)
        else:
            return isa_json

    def get_isa_study(self, study_id, api_key):
        """
        Get the Study section from the Investigation ISA object
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: an ISA-API Study object
        """
        inv_obj = self.get_isa_investigation(study_id, api_key)
        std_obj = inv_obj.studies[0]
        return std_obj

    def get_study_title(self, study_id, api_key):
        """
        Get the Study title
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: a string with the study title
        """
        std_obj = self.get_isa_study(study_id, api_key)
        return std_obj.title

    def get_study_description(self, study_id, api_key):
        """
        Get the Study description
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: a string with the study description
        """
        std_obj = self.get_isa_study(study_id, api_key)
        return std_obj.description

    def write_study_json_title(self, study_id, api_key, new_title):
        """
        Write out a new Investigation file with the new Study title
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :param new_title: the new title for the Study
        :return: the new title
        """
        inv_obj = self.get_isa_investigation(study_id, api_key)
        std_obj = inv_obj.studies[0]
        std_obj.title = new_title

        path = wsc.get_study_updates_location(study_id, api_key)

        # Using the new feature in isaoools, implemented from issue #185
        # https://github.com/ISA-tools/isa-api/issues/185
        # isatools.isatab.dump() writes out the ISA as a string representation of the ISA-Tab,
        # skipping writing tables, i.e. only i_investigation.txt
        dump(inv_obj, path, skip_dump_tables=True)

        return new_title

    def write_study_json_description(self, study_id, api_key, new_description):
        """
        Write out a new Investigation file with the new Study title
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :param new_description: the new description for the Study
        :return: the new description
        """
        inv_obj = self.get_isa_investigation(study_id, api_key)
        std_obj = inv_obj.studies[0]
        std_obj.description = new_description

        path = wsc.get_study_updates_location(study_id, api_key)

        # Using the new feature in isaoools, implemented from issue #185
        # https://github.com/ISA-tools/isa-api/issues/185
        # isatools.isatab.dump() writes out the ISA as a string representation of the ISA-Tab,
        # skipping writing tables, i.e. only i_investigation.txt
        dump(inv_obj, path, skip_dump_tables=True)

        return new_description

    def get_isa_json(self, study_id, api_key):
        """
        Get an ISA-API Investigation object reading directly from the ISA-Tab files
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: an ISA-API Investigation object
        """
        path = wsc.get_study_location(study_id, api_key)
        # try the new parser first
        # isa_json = None
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
                return isa_json
        else:
            return isa_json
