from pydantic import BaseModel


class FileResources(BaseModel):
    mtbls_ontology_file: str
    mtbls_zooma_file: str
    mzml_xsd_schema_file_path: str
    validations_file: str
