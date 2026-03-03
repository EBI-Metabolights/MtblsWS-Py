from pydantic import BaseModel


class FileResources(BaseModel):
    mtbls_ontology_file: str = "./resources/Metabolights.owl"
    mtbls_zooma_file: str = "./resources/metabolights_zooma.tsv"
    mzml_xsd_schema_file_path: str = "./resources/mzML1.1.1_idx.xsd"
