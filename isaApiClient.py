from isatools.convert import isatab2json
from mtblsWSclient import WsClient

"""
MetaboLights ISA-API client

Use the Python-based ISA-API tools

author: jrmacias
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
