import logging
from typing import List, Optional

from zeep import Client, Settings, helpers
from zeep.exceptions import Fault

from app.ws.chebi.models import Entity, LiteEntity, OntologyDataItem
from app.ws.chebi.settings import ChebiWsSettings, get_chebi_ws_settings
from app.ws.chebi.types import SearchCategory, StarsCategory, RelationshipType, StructureType, \
    StructureSearchCategory


class ChebiWsException(Exception):
    def __init__(self, message):
        self.message = message


def ws_fault_exception_handler(func):
    def exception_handler(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Fault as e:
            raise ChebiWsException(message=e.message)

    return exception_handler


class ChebiWsProxy(object):

    def __init__(self, settings: ChebiWsSettings = None):

        self.settings = settings
        if not self.settings:
            self.settings = ChebiWsSettings()

        if self.settings.chebi_ws_service_binding_log_level == "WARNING":
            zeep_operation_log_level = logging.WARNING
        elif self.settings.chebi_ws_service_binding_log_level == "ERROR":
            zeep_operation_log_level = logging.ERROR
        else:
            zeep_operation_log_level = logging.WARNING

        client_settings = Settings(strict=self.settings.chebi_ws_strict, xml_huge_tree=self.settings.chebi_ws_xml_huge_tree)
        logging.getLogger('zeep.wsdl.bindings.soap').setLevel(zeep_operation_log_level)
        try:
            self.client = Client(self.settings.chebi_ws_wsdl, settings=client_settings)
            self.service = self.client.bind(self.settings.chebi_ws_wsdl_service, self.settings.chebi_ws_wsdl_service_port)
        except Fault as e:
            raise ChebiWsException(message=e.message)
        except ValueError as e:
            raise ChebiWsException(message=e.args)


    @ws_fault_exception_handler
    def get_complete_entity(self, chebi_id: str) -> Optional[Entity]:
        result = self.service.getCompleteEntity(chebi_id)
        serialized_result = helpers.serialize_object(result, dict)

        if serialized_result:
            data = Entity.parse_obj(serialized_result)
            return data
        return None

    @ws_fault_exception_handler
    def get_lite_entity_list(self, search_text: str, search_category: SearchCategory,
                             maximum_results: int, stars: StarsCategory) -> Optional[List[LiteEntity]]:
        result = self.service.getLiteEntity(search_text, search_category.value, maximum_results, stars.value)
        serialized_result = helpers.serialize_object(result, dict)
        if serialized_result:
            result = [LiteEntity.parse_obj(data) for data in serialized_result]
            return result
        return None

    @ws_fault_exception_handler
    def get_complete_entity_by_list(self, chebi_id_list: List[str]) -> Optional[List[Entity]]:
        service_result = self.service.getCompleteEntityByList(chebi_id_list)
        serialized_result = helpers.serialize_object(service_result, dict)
        if serialized_result:
            result = [Entity.parse_obj(data) for data in serialized_result]
            return result
        return None

    @ws_fault_exception_handler
    def get_ontology_parents(self, chebi_id: str) -> Optional[List[OntologyDataItem]]:
        service_result = self.service.getOntologyParents(chebi_id)
        serialized_result = helpers.serialize_object(service_result, dict)
        if serialized_result:
            result = [OntologyDataItem.parse_obj(data) for data in serialized_result]
            return result
        return None

    @ws_fault_exception_handler
    def get_ontology_children(self, chebi_id: str) -> Optional[List[OntologyDataItem]]:
        service_result = self.service.getOntologyChildren(chebi_id)
        serialized_result = helpers.serialize_object(service_result, dict)
        if serialized_result:
            result = [OntologyDataItem.parse_obj(data) for data in serialized_result]
            return result
        return None

    @ws_fault_exception_handler
    def get_all_ontology_children_in_path(self, chebi_id: str, relationship_type: RelationshipType,
                                          structure_only: bool) -> Optional[List[LiteEntity]]:
        service_result = self.service.getAllOntologyChildrenInPath(chebi_id, relationship_type.value, structure_only)
        serialized_result = helpers.serialize_object(service_result, dict)
        if serialized_result:
            result = [LiteEntity.parse_obj(data) for data in serialized_result]
            return result
        return None

    @ws_fault_exception_handler
    def get_structure_search(self, structure: str, structure_type: StructureType,
                             structure_search_category: StructureSearchCategory,
                             total_results: int, tanimoto_cutoff: float) -> Optional[List[LiteEntity]]:

        service_result = self.service.getStructureSearch(structure, structure_type,
                                                         structure_search_category,
                                                         total_results, tanimoto_cutoff)

        serialized_result = helpers.serialize_object(service_result, dict)
        if serialized_result:
            result = [Entity.parse_obj(data) for data in serialized_result]
            return result
        return None


if __name__ == "__main__":
    def test_chebi_ws():
        chebi_ws_proxy = ChebiWsProxy(get_chebi_ws_settings())

        ws_result = chebi_ws_proxy.get_structure_search("C[C@H](CCC=C(C)C)c1ccc(C)cc1",
                                                        structure_type=StructureType.SMILES.value,
                                                        structure_search_category=StructureSearchCategory.SIMILARITY.value,
                                                        total_results=200,
                                                        tanimoto_cutoff=float(0.8))
        print(ws_result)

        ws_result = chebi_ws_proxy.get_complete_entity_by_list(["CHEBI:10225", "CHEBI:102265"])
        print(ws_result)
        ws_result = chebi_ws_proxy.get_complete_entity("CHEBI:10225000")
        print(ws_result)
        ws_result = chebi_ws_proxy.get_ontology_parents("CHEBI:10225")
        print(ws_result)
        ws_result = chebi_ws_proxy.get_ontology_children("CHEBI:10225")
        print(ws_result)
        ws_result = chebi_ws_proxy.get_all_ontology_children_in_path("CHEBI:10225",
                                                                     RelationshipType.HAS_ROLE, structure_only=False)
        print(ws_result)
        ws_result = chebi_ws_proxy.get_lite_entity("CHEBI:10225", SearchCategory.CHEBI_ID, 200, StarsCategory.ALL)
        print(ws_result)


    test_chebi_ws()
