from typing import List

from app.ws.ejprd_beacon.beacon_dataclasses import OntologyCollection, ParsedOntology
from app.ws.ejprd_beacon.parsers.abstract_ontology_parser import AbstractOntologyParser


class OLSParser(AbstractOntologyParser):

    def parse(self, collection: OntologyCollection) -> List[ParsedOntology]:
        parsed = [
            ParsedOntology(
                id=ont['ontologyId'],
                name=ont['config']['title'],
                url=ont['config']['versionIri'],
                version=ont['config']['version'],
                namespace_prefix=ont['config']['preferredPrefix'],
                iri_prefix=''
            ) for ont in collection.ontologies
        ]
        return parsed