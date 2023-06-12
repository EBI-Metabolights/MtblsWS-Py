from pydantic import BaseModel


class FileResources(BaseModel):
    mtbls_ontology_file: str
    mtbls_zooma_file: str
