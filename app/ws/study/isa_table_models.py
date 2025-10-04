from pydantic import BaseModel


class OntologyValue(BaseModel):
    term: str = ""
    term_source_ref: str = ""
    term_accession_number: str = ""


class NumericValue(BaseModel):
    value: str = ""
    unit: OntologyValue = OntologyValue()
