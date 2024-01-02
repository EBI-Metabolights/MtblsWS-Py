from abc import ABC

from app.ws.ejprd_beacon.beacon_dataclasses import OntologyCollection


class AbstractOntologyParser(ABC):

    def __init__(self):
        pass

    def parse(self, collection: OntologyCollection):
        raise NotImplementedError
