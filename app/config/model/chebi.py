from pydantic import BaseModel


class ChebiServiceConnection(BaseModel):
    chebi_ws_wsdl: str = "https://www.ebi.ac.uk/webservices/chebi/2.0/webservice?wsdl"
    chebi_ws_wsdl_service: str = "ChebiWebServiceService"
    chebi_ws_wsdl_service_port: str = "ChebiWebServicePort"


class ChebiServiceConfiguration(BaseModel):
    chebi_ws_service_binding_log_level: str = "ERROR"
    chebi_ws_strict: bool = False
    chebi_ws_xml_huge_tree: bool = True


class ChebiPipelineSearchServices(BaseModel):
    classyfire_url: str = "http://classyfire.wishartlab.com"
    opsin_url: str = "https://opsin.ch.cam.ac.uk/opsin/"
    chemspider_url: str = "http://parts.chemspider.com/JSON.ashx?op="
    chem_plus_url: str = (
        "https://chem.nlm.nih.gov/api/data/inchikey/equals/INCHI_KEY?data=summary"
    )
    unichem_url: str = "https://www.ebi.ac.uk/unichem/rest"
    dime_url: str = "https://dimedb.ibers.aber.ac.uk/api/metabolites?where=%7B%22_id%22%20:%20%22INCHI_KEY%22%7D&projection=%7B%22External%20Sources%22%20:%201%7D"


class ChebiPipelineConfiguration(BaseModel):
    chebi_annotation_sub_folder: str = "chebi_pipeline_annotations"
    run_standalone_chebi_pipeline_python_file: str = "app/ws/chebi_pipeline_utils.py"
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
    connection: ChebiServiceConnection = ChebiServiceConnection()
    configuration: ChebiServiceConfiguration = ChebiServiceConnection()


class ChebiCacheSettings(BaseModel):
    images_cache_path: str = ""


class ChebiSettings(BaseModel):
    pipeline: ChebiPipelineConfiguration
    service: ChebiServiceSettings = ChebiServiceSettings()
    caches: ChebiCacheSettings = ChebiCacheSettings()
