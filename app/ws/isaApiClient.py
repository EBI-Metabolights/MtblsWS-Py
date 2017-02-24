import glob
import os
from flask import abort
# TODO here we are using the develop branch of isatools. Replace with the pip version when released.
from isatools.isatab import load, dumps
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

    def _get_isa_json(self, study_id, api_key):
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

    def get_json_study(self, study_id, api_key):
        inv_obj = self._get_isa_json(study_id, api_key)
        std_obj = inv_obj.studies[0]
        return std_obj

    def get_study_title(self, study_id, api_key):
        """
        Get the Study title
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: a string with the study title
        """
        std_obj = self.get_json_study(study_id, api_key)
        return std_obj.title

    def get_study_description(self, study_id, api_key):
        """
        Get the Study description
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: a string with the study description
        """
        std_obj = self.get_json_study(study_id, api_key)
        return std_obj.description

    def write_study_json_title(self, study_id, api_key, new_title):
        inv_obj = self._get_isa_json(study_id, api_key)
        std_obj = inv_obj.studies[0]
        std_obj.title = new_title

        # Using the new feature in isaoools, implemented from issue #185
        # https://github.com/ISA-tools/isa-api/issues/185
        # dumps() writes out the ISA as a string representation of the ISA-Tab,
        # skipping writing tables, i.e. only i_investigation.txt
        inv_str = dumps(inv_obj, skip_dump_tables=True)
        self.write_inv_file(study_id, api_key, inv_str)

        return new_title

    def write_study_json_description(self, study_id, api_key, new_description):
        inv_obj = self._get_isa_json(study_id, api_key)
        std_obj = inv_obj.studies[0]
        std_obj.description = new_description

        # Using the new feature in isaoools, implemented from issue #185
        # https://github.com/ISA-tools/isa-api/issues/185
        # dumps() writes out the ISA as a string representation of the ISA-Tab,
        # skipping writing tables, i.e. only i_investigation.txt
        inv_str = dumps(inv_obj, skip_dump_tables=True)
        self.write_inv_file(study_id, api_key, inv_str)

        return new_description

    def write_inv_file(self, study_id, api_key, inv_str):
        """
        Write an ISA object to a file
        :param inv_str:  ISA object as a string representation of the ISA-Tab
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: success
        """
        out_path = wsc.get_study_updates_location(study_id, api_key)
        out_full_filename = os.path.join(out_path, "i_investigation.txt")

        with open(out_full_filename, mode='w') as o_file:
            ok = o_file.write(inv_str)
        return ok
