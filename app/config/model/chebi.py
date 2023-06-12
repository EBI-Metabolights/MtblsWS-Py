from pydantic import BaseModel


class ChebiServiceConnection(BaseModel):
    chebi_ws_wsdl: str
    chebi_ws_wsdl_service: str
    chebi_ws_wsdl_service_port: str


class ChebiServiceConfiguration(BaseModel):
    chebi_ws_service_binding_log_level: str = "ERROR"
    chebi_ws_strict: bool = False
    chebi_ws_xml_huge_tree: bool = True


class ChebiPipelineSearchServices(BaseModel):
    classyfire_url: str
    opsin_url: str
    chemspider_url: str
    chem_plus_url: str
    unichem_url: str
    dime_url: str


class ChebiPipelineConfiguration(BaseModel):
    curated_metabolite_list_file_location: str
    chebi_upload_script: str
    chebi_pipeline_url: str
    chebi_pipeline_annotation_folder: str
    obo_file: str
    chebi_url_wait: int = 300
    removed_hs_mol_count: int = 500
    classyfire_mapping: str
    search_services: ChebiPipelineSearchServices


class ChebiServiceSettings(BaseModel):
    connection: ChebiServiceConnection
    configuration: ChebiServiceConfiguration


class ChebiSettings(BaseModel):
    pipeline: ChebiPipelineConfiguration
    service: ChebiServiceSettings
