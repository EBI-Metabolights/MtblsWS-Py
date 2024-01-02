

class BeaconDictUtils:

    @staticmethod
    def check(term: str, ontology_objects: dict) -> bool:
        """
        Check whether a given term exists as a value within a given dict. The dict is assumed to be representing ontology
        terms, with the keys of the dict being IDs such as ONT_123 and the values being terms such as 'health'.
        :param term: Search term
        :param ontology_objects: Dict full of ontology terms.
        :return: bool value indicating whether the term was found.
        """
        for key, value in ontology_objects.items():
            if value == term:
                print(f"Found lost term {term} in {key}")
                return True
        return False

    @staticmethod
    def to_camel_case(s):
        parts = s.split('_')
        return parts[0] + ''.join(word.capitalize() for word in parts[1:])

    @staticmethod
    def camel_case_keys(d):
        return {BeaconDictUtils.to_camel_case(k) if '_' in k else k: v for k, v in d.items()}
