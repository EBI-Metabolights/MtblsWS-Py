import glob
import os
from isatools.convert import isatab2json
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

    def get_study_location(self, study_id, api_key):
        """
        Get study info from MetaboLights (Java-Based) WebService
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: Actual location of the isa-tab files in the server file system
        """
        return wsc.get_study_location(study_id, api_key)

    def get_study_updates_location(self, study_id, api_key):
        """
        Get location for output updates in a MetaboLights study (possibly a user MTBLS-Labs folder)
        :param study_id:
        :param api_key:
        :return:
        """
        return wsc.get_study_updates_location(study_id, api_key)

    def get_isa_json(self, study_id, api_key):
        """
        Get an ISA-API Investigation object reading directly from the ISA-Tab files
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: an ISA-API Investigation object
        """
        path = self.get_study_location(study_id, api_key)
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
                    print(type(inst))  # the exception instance
                    print(inst.args)
                    # raise
                    raise RuntimeError("Validation error when trying to read the study.")
            else:
                return isa_json
        else:
            return isa_json

    def get_study_title(self, study_id, api_key):
        """
        Get the Study title
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: a string with the study title
        """
        inv_obj = self.get_isa_json(study_id, api_key)
        std_obj = inv_obj.get("studies")[0]  # assuming there is only one study per investigation file
        return std_obj.get("title")

    def get_study_title_noISATools(self, study_id, api_key):
        path = self.get_study_location(study_id, api_key)
        i_filename = glob.glob(os.path.join(path, "i_*.txt"))[0]
        with open(i_filename) as i_file:
            for line in i_file:
                if "Study Title" in line:
                    line = line.replace("Study Title", "")
                    line = line.replace("\t", "")
                    line = line.replace("\n", "")
                    line = line.replace("\"", "")
                    return line

    def write_study_title(self, study_id, api_key, new_title):
        i_path = self.get_study_location(study_id, api_key)
        o_path = self.get_study_updates_location(study_id, api_key)

        i_filename = glob.glob(os.path.join(i_path, "i_*.txt"))[0]
        o_filename = os.path.join(o_path, "i_investigation.txt")

        o_file = open(o_filename, mode='w')

        with open(i_filename) as i_file:
            for line in i_file:
                if "Study Title" not in line:
                    o_file.write(line)
                else:
                    o_file.write('Study Title' + '\t' + '"' + new_title + '"' + '\n')

        return new_title

    def get_study_description(self, study_id, api_key):
        """
        Get the Study description
        :param study_id: MTBLS study identifier
        :param api_key: User API key for accession check
        :return: a string with the study description
        """
        inv_obj = self.get_isa_json(study_id, api_key)
        std_obj = inv_obj.get("studies")[0]  # assuming there is only one study per investigation file
        return std_obj.get("description")

    def get_study_description_noISATools(self, study_id, api_key):
        path = self.get_study_location(study_id, api_key)
        i_filename = glob.glob(os.path.join(path, "i_*.txt"))[0]
        with open(i_filename) as i_file:
            for line in i_file:
                if "Study Description" in line:
                    line = line.replace("Study Description", "")
                    line = line.replace("\t", "")
                    line = line.replace("\n", "")
                    line = line.replace("\"", "")
                    return line

    def write_study_description(self, study_id, api_key, new_description):
        i_path = self.get_study_location(study_id, api_key)
        o_path = self.get_study_updates_location(study_id, api_key)

        i_filename = glob.glob(os.path.join(i_path, "i_*.txt"))[0]
        o_filename = os.path.join(o_path, "i_investigation.txt")

        o_file = open(o_filename, mode='w')

        with open(i_filename) as i_file:
            for line in i_file:
                if "Study Description" not in line:
                    o_file.write(line)
                else:
                    o_file.write('Study Description' + '\t' + '"' + new_description + '"' + '\n')

        return new_description
