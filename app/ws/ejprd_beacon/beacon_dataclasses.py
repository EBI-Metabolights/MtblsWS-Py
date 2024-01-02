from dataclasses import dataclass
from typing import List


@dataclass
class OntologyCollection:
    name: str
    ontologies: List[dict]
    ontology_ids: List[int]

@dataclass
class ParsedOntology:
    id: str
    name: str
    url: str
    version: str
    namespace_prefix: str
    iri_prefix: str

@dataclass
class FilterTerm:
    id: str
    label: str
    type: str = "ontologyTerm"
