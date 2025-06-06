import logging
from functools import lru_cache
from typing import List, Union

from zeep import Client, Settings, helpers
from zeep.exceptions import Fault
from flask import current_app as app
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

    def __init__(self, settings: Union[None, ChebiWsSettings] = None):
        self.settings = settings
        self._service = None

    def get_service(self):
        if not self._service:
            self._service = self.setup()
        return self._service

    service = property(get_service)

    def setup(self):
        if not self.settings:
            self.settings = get_chebi_ws_settings()
        if self.settings.chebi_ws_service_binding_log_level == "WARNING":
            zeep_operation_log_level = logging.WARNING
        elif self.settings.chebi_ws_service_binding_log_level == "ERROR":
            zeep_operation_log_level = logging.ERROR
        else:
            zeep_operation_log_level = logging.WARNING

        client_settings = Settings(strict=self.settings.chebi_ws_strict,
                                   xml_huge_tree=self.settings.chebi_ws_xml_huge_tree)
        logging.getLogger('zeep.wsdl.bindings.soap').setLevel(zeep_operation_log_level)
        logging.getLogger('zeep.transports').setLevel(logging.INFO)
        try:
            client = Client(self.settings.chebi_ws_wsdl, settings=client_settings)
            return client.bind(self.settings.chebi_ws_wsdl_service, self.settings.chebi_ws_wsdl_service_port)
        except Fault as e:
            raise ChebiWsException(message=e.message)
        except ValueError as e:
            raise ChebiWsException(message=e.args)

    @ws_fault_exception_handler
    def get_complete_entity(self, chebi_id: str) -> Union[None, Entity]:
        result = self.service.getCompleteEntity(chebi_id)
        serialized_result = helpers.serialize_object(result, dict)

        if serialized_result:
            data = Entity.model_validate(serialized_result)
            return data
        return None

    @ws_fault_exception_handler
    def get_lite_entity_list(self, search_text: str, search_category: SearchCategory,
                             maximum_results: int, stars: StarsCategory) -> Union[None, List[LiteEntity]]:
        result = self.service.getLiteEntity(search_text, search_category.value, maximum_results, stars.value)
        serialized_result = helpers.serialize_object(result, dict)
        if serialized_result:
            result = [LiteEntity.model_validate(data) for data in serialized_result if data]
            return result
        return None

    @ws_fault_exception_handler
    def get_complete_entity_by_list(self, chebi_id_list: List[str]) -> Union[None, List[Entity]]:
        service_result = self.service.getCompleteEntityByList(chebi_id_list)
        serialized_result = helpers.serialize_object(service_result, dict)
        if serialized_result:
            result = [Entity.model_validate(data) for data in serialized_result if data]
            return result
        return None

    @ws_fault_exception_handler
    def get_ontology_parents(self, chebi_id: str) -> Union[None, List[OntologyDataItem]]:
        service_result = self.service.getOntologyParents(chebi_id)
        serialized_result = helpers.serialize_object(service_result, dict)
        if serialized_result:
            result = [OntologyDataItem.model_validate(data) for data in serialized_result if data]
            return result
        return None

    @ws_fault_exception_handler
    def get_ontology_children(self, chebi_id: str) -> Union[None, List[OntologyDataItem]]:
        service_result = self.service.getOntologyChildren(chebi_id)
        serialized_result = helpers.serialize_object(service_result, dict)
        if serialized_result:
            result = [OntologyDataItem.model_validate(data) for data in serialized_result if data]
            return result
        return None

    @ws_fault_exception_handler
    def get_all_ontology_children_in_path(self, chebi_id: str, relationship_type: RelationshipType,
                                          structure_only: bool) -> Union[None, List[LiteEntity]]:
        service_result = self.service.getAllOntologyChildrenInPath(chebi_id, relationship_type.value, structure_only)
        serialized_result = helpers.serialize_object(service_result, dict)
        if serialized_result:
            result = [LiteEntity.model_validate(data) for data in serialized_result if data]
            return result
        return None

    @ws_fault_exception_handler
    def get_structure_search(self, structure: str, structure_type: StructureType,
                             structure_search_category: StructureSearchCategory,
                             total_results: int, tanimoto_cutoff: float) -> Union[None, List[LiteEntity]]:

        service_result = self.service.getStructureSearch(structure, structure_type,
                                                         structure_search_category,
                                                         total_results, tanimoto_cutoff)

        serialized_result = helpers.serialize_object(service_result, dict)
        if serialized_result:
            result = [LiteEntity.model_validate(data) for data in serialized_result if data]
            return result
        return None


@lru_cache(1)
def get_chebi_ws_proxy() -> ChebiWsProxy:
    return ChebiWsProxy()