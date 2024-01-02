class AbstractOntologyParser(ABC):

    def __init__(self):
        pass

    def parse(self, collection: OntologyCollection):
        raise NotImplementedError
