from typing import List

from app.ws.ejprd_beacon.beacon_dataclasses import OntologyCollection, ParsedOntology
from app.ws.ejprd_beacon.parsers.abstract_ontology_parser import AbstractOntologyParser


class BioPortalParser(AbstractOntologyParser):

    def parse(self, collection: OntologyCollection) -> List[ParsedOntology]:
        """"
        If latest submission
        pull information out
        if not, just pull everything out of 'ontology'
        and put in blanks for the rest.
        """
        parsed = [
            ParsedOntology(
                id=ont['ontology']['acronym'],
                name=ont['ontology']['name'],
                url=ont['ontology']['@id'],
                version=ont['latest_submission'].setdefault('version', '') if ont['latest_submission'] is not None else '',
                namespace_prefix=ont['ontology']['acronym'],
                iri_prefix=''
            ) for ont in collection.ontologies
        ]
        return parsed
