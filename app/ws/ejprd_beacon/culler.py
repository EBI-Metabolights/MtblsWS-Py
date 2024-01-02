from typing import List

from app.ws.ejprd_beacon.beacon_dataclasses import OntologyCollection


class Culler:

    def __init__(self, intersection: List[str]):
        self.intersection = intersection

    def cull(self, collection: OntologyCollection) -> OntologyCollection:
        return self.__getattribute__(collection.name)(collection)

    def bioportal(self, collection: OntologyCollection) -> OntologyCollection:
        new_collection = OntologyCollection(
            name='bioportal',
            ontologies=[ont for ont in collection.ontologies if ont['ontology']['acronym'].lower() in self.intersection],
            ontology_ids=[oid for oid in collection.ontology_ids if oid in self.intersection]
        )
        return new_collection

    def ols(self, collection: OntologyCollection) -> OntologyCollection:
        new_collection = OntologyCollection(
            name='ols',
            ontologies=[ont for ont in collection.ontologies if ont['ontologyId'] in self.intersection],
            ontology_ids=[oid for oid in collection.ontology_ids if oid in self.intersection]
        )
        return new_collection
