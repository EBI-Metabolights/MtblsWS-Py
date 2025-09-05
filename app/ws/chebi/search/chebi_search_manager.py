import logging
from urllib import parse

from flask import abort

from app.ws.chebi.chebi_utils import chebi_search_v2, get_complete_chebi_entity_v2
from app.ws.chebi.models import OntologyDataItem
from app.ws.chebi.search import commons
from app.ws.chebi.search import utils

from app.ws.chebi.search.curated_metabolite_table import CuratedMetaboliteTable
from app.ws.chebi.search.models import CompoundSearchResponseModel, CompoundSearchResultModel, SearchResource
from app.ws.chebi.types import SearchCategory, StarsCategory
from app.ws.chebi.wsproxy import ChebiWsException


logger = logging.getLogger(__file__)


class ChebiSearchManager(object):

    def __init__(self, ws_proxy=None, curated_metabolite_table=None):
        self.ws_proxy = ws_proxy
        self.curated_metabolite_table = curated_metabolite_table

    def search_by_type(self, search_type, search_value):
        search_type_lower = search_type.lower()
        if search_type_lower == "name":
            return self.get_compound_by_name(search_value)
        elif search_type_lower == "databaseid":
            return self.get_compound_by_database_id(search_value)
        else:
            encoded_search_value = search_value
            if search_type_lower == "inchi":
                return self.get_compound_by_inchi(encoded_search_value)
            elif search_type_lower == "smiles":
                return self.get_compound_by_smiles(encoded_search_value)
            else:
                response = CompoundSearchResponseModel()
                response.message = f"Not valid search type: {search_type}"
                return response

    def get_compound_by_database_id(self, database_id: str):
        logger.debug("Searching by compound database id %s" % database_id)

        response = CompoundSearchResponseModel()
        if "chebi" in database_id.lower():
            try:
                complete_entity = get_complete_chebi_entity_v2(chebi_id=database_id)
                if not complete_entity:
                    return abort(404, message=f"Entity not found with ChEBI id {database_id}")
                compound_search_result_v2 = CompoundSearchResultModel(
                    name=complete_entity["name"],
                    inchi=complete_entity["default_structure"]["standard_inchi"] or "",
                    databaseId=database_id,
                    formula=complete_entity["chemical_data"]["formula"] or "",
                    smiles=complete_entity["default_structure"]["smiles"] or "",
                    search_resource="CHEBI"
                )
                response.content.append(compound_search_result_v2)
            except ChebiWsException as e:
                abort(501, message=f"Remote server error {e.message}")

        if not response.content:
            response.message = f"No match found for {database_id}"
        else:
            if len(response.content) > 1:
                response.content.sort(key=lambda x: x.score(), reverse=True)
        return response

    def get_compound_by_inchi(self, inchi: str):
        logger.debug("Searching by compound inchi %s" % inchi)
        compound_inchi = inchi
        return self._search_by_category(compound_inchi, inchi,
                                        SearchCategory.INCHI_INCHI_KEY, CuratedMetaboliteTable.INCHI_INDEX)

    def get_compound_by_smiles(self, smiles: str):
        logger.debug("Searching by compound smiles %s" % smiles)
        compound_smiles = parse.unquote(smiles)
        compound_smiles = self._check_for_smiles_prefix(compound_smiles)
        return self._search_by_category(compound_smiles, smiles,
                                        SearchCategory.SMILES, CuratedMetaboliteTable.SMILES_INDEX)

    def _search_by_category(self, filtered_search_value, search_value,
                            category: SearchCategory, curation_table_index: int):
        response = CompoundSearchResponseModel()
        commons.search_hits_with_search_category(filtered_search_value, category, curation_table_index,
                                                 response, StarsCategory.ALL, self.ws_proxy,
                                                 self.curated_metabolite_table)
        if not response.content:
            response.message = f"No match found for {category.value} {search_value}"
        else:
            if len(response.content) > 1:
                response.content.sort(key=lambda x: x.score(), reverse=True)
        return response

    def _check_for_smiles_prefix(self, smiles: str):
        smiles_lower = smiles.lower()
        if smiles_lower.startswith("smiles=") or "smiles" in smiles_lower:
            return smiles_lower.replace("smiles=", "")
        return smiles_lower

    def get_compound_by_name(self, compound_name: str):
        logger.debug("Searching by compound name %s" % compound_name)

        response = CompoundSearchResponseModel()
        if not compound_name or compound_name.lower() in ("unknown", "unidentified"):
            compound_search_result = CompoundSearchResultModel(search_resource=SearchResource.CURATED, name="unknown")
            response.content.append(compound_search_result)
            return response

        decoded_name = utils.decode_compound_name(compound_name)
        self._search_hits_from_list_and_chebi_only(decoded_name, response)
        if not response.content:
            response.message = f"No match found for {compound_name}"
        return response

    def _search_hits_from_list_and_chebi_only(self, compound_name: str, response: CompoundSearchResponseModel):
        curated_metabolite_table = self.curated_metabolite_table

        name_match = curated_metabolite_table.get_matching_rows(CuratedMetaboliteTable.COMPOUND_INDEX, compound_name)
        if name_match:
            self._search_and_fill_by_name(compound_name, name_match, response)
        else:
            chebi_id = chebi_search_v2(search_term=compound_name)
            if not chebi_id:
                abort(400, message="Invalid ChEBI id")
            try:
                complete_entity = get_complete_chebi_entity_v2(chebi_id=chebi_id)
                if not complete_entity:
                    return abort(404, message=f"Entity not found with ChEBI id {chebi_id}")
                compound_search_result_v2 = CompoundSearchResultModel(
                    name=complete_entity["name"],
                    inchi=complete_entity["default_structure"]["standard_inchi"] or "",
                    databaseId=chebi_id,
                    formula=complete_entity["chemical_data"]["formula"] or "",
                    smiles=complete_entity["default_structure"]["smiles"] or "",
                    search_resource="CHEBI"
                )
                response.content.append(compound_search_result_v2)
            except ChebiWsException as e:
                abort(501, message=f"Remote server error {e.message}")

    def _search_and_fill_by_name(self, compound_name, name_match, response):
        name_match_compound_name = name_match[CuratedMetaboliteTable.COMPOUND_INDEX]
        name_match_chebi_id = name_match[CuratedMetaboliteTable.CHEBI_ID_INDEX]
        if "|" in name_match_compound_name and "|" in name_match_chebi_id:
            result = CompoundSearchResultModel()
            index = utils.find_term_index_in_source(name_match, compound_name)
            commons.fill_from_metabolite_table(index, name_match, result)
            return

        if name_match_chebi_id:
            index = utils.find_term_index_in_source(name_match_compound_name, compound_name)
            chebi_id = utils.get_term_in_source(name_match_chebi_id, index)
            result = commons.fill_with_complete_entity(chebi_id, self.ws_proxy)
            if result:
                if result.formula:
                    name_match_formula = name_match[CuratedMetaboliteTable.FORMULA_INDEX]
                    result.formula = utils.get_term_in_source(name_match_formula, index)

                if not result.is_complete():
                    commons.fill_from_metabolite_table(index, name_match, result)

                response.content.append(result)

    def _check_for_anion_case(self, compound_name, chebi_id):
        if compound_name.endswith("ate"):
            ontology_items = self.ws_proxy.get_ontology_children(chebi_id)
            if ontology_items:
                for item in ontology_items:
                    data_item: OntologyDataItem = item
                    data_item_type = data_item.type.lower()
                    if data_item_type == "is conjugate acid of" or data_item_type == "is conjugate base of":
                        return data_item.chebiId

        return chebi_id

    def _search_name_and_synonyms(self, compound_name, result):
        compound_name_to_compare = utils.remove_few_characters_for_consistency(compound_name)
        for lite_entity in result:
            entity_name = lite_entity.chebiAsciiName
            entity_name_to_compare = utils.remove_few_characters_for_consistency(entity_name)
            if entity_name_to_compare.lower() == compound_name_to_compare.lower():
                return lite_entity.chebiId

        for lite_entity in result:
            complete_entity = self.ws_proxy.get_complete_entity(lite_entity.chebiId)

            if complete_entity:
                for item in complete_entity.synonyms:
                    item_name = item.data
                    item_name_to_compare = utils.remove_few_characters_for_consistency(item_name)
                    if item_name_to_compare.lower() == compound_name_to_compare.lower():
                        return complete_entity.chebiId

                for item in complete_entity.iupacNames:
                    item_name = item.data
                    item_name_to_compare = utils.remove_few_characters_for_consistency(item_name)
                    if item_name_to_compare.lower() == compound_name_to_compare.lower():
                        return complete_entity.chebiId
        return None
