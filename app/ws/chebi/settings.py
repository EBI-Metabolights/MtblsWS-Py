from functools import lru_cache

from pydantic import BaseSettings


class ChebiWsSettings(BaseSettings):
    chebi_ws_wsdl: str = "https://www.ebi.ac.uk/webservices/chebi/2.0/webservice?wsdl"
    chebi_ws_wsdl_service: str = 'ChebiWebServiceService'
    chebi_ws_wsdl_service_port: str = 'ChebiWebServicePort'
    chebi_ws_strict: bool = False
    chebi_ws_xml_huge_tree: bool = True
    chebi_ws_service_binding_log_level: str = "ERROR"


    class Config:
        # read and set settings variables from this env_file
        env_file = ".env"


@lru_cache
def get_chebi_ws_settings():
    return ChebiWsSettings()
