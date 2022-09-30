from app.ws.chebi.search.curated_metabolite_table import CuratedMetaboliteTable
from app.ws.chebi.search.models import CompoundSearchResultModel, SearchResource, CompoundSearchResponseModel
from app.ws.chebi.search.utils import get_term_in_source, find_term_index_in_source
from app.ws.chebi.types import SearchCategory, StarsCategory
from app.ws.chebi.wsproxy import get_chebi_ws_proxy, ChebiWsProxy, ChebiWsException


def fill_with_complete_entity(chebi_id, chebi_ws=None):
    if not chebi_ws:
        chebi_ws = get_chebi_ws_proxy()
    chebi_entity = None
    try:
        chebi_entity = chebi_ws.get_complete_entity(chebi_id)
    except ChebiWsException as e:
        return None

    if chebi_entity:
        result = CompoundSearchResultModel()
        result.search_resource = SearchResource.CHEBI
        result.databaseId = chebi_entity.chebiId
        result.smiles = chebi_entity.smiles
        result.inchi = chebi_entity.inchi
        result.name = chebi_entity.chebiAsciiName
        formula = None
        if chebi_entity.formulae:
            formula = chebi_entity.formulae[0].data
            charge = chebi_entity.charge
            if formula and charge and charge != "0":
                if charge == "+1":
                    formula = formula + "+"
                elif charge == "-1":
                    formula = formula + "-"
                else:
                    formula = formula + charge

        result.formula = formula
        return result
    return None


def fill_from_metabolite_table(index, row, result: CompoundSearchResultModel):
    result.search_resource = SearchResource.CURATED

    name_match_formula = row[CuratedMetaboliteTable.FORMULA_INDEX]
    result.formula = get_term_in_source(name_match_formula, index) if name_match_formula else None

    name_match_inchi = row[CuratedMetaboliteTable.INCHI_INDEX]
    result.inchi = get_term_in_source(name_match_inchi, index) if name_match_inchi and isinstance(name_match_inchi, str) else None

    name_match_chebi_id = row[CuratedMetaboliteTable.CHEBI_ID_INDEX]
    result.databaseId = get_term_in_source(name_match_chebi_id, index) if name_match_chebi_id else None

    name_match_compound_name = row[CuratedMetaboliteTable.COMPOUND_INDEX]
    result.name = get_term_in_source(name_match_compound_name, index) if name_match_compound_name else None

    name_match_smiles = row[CuratedMetaboliteTable.SMILES_INDEX]
    result.smiles = get_term_in_source(name_match_smiles, index) if name_match_smiles and isinstance(name_match_smiles, str) else None


def search_hits_with_search_category(search_name: str,
                                     search_category: SearchCategory,
                                     curation_table_index: int,
                                     response: CompoundSearchResponseModel,
                                     stars: StarsCategory = StarsCategory.ALL,
                                     ws_proxy: ChebiWsProxy = None,
                                     curated_metabolite_table: CuratedMetaboliteTable = None
                                     ):
    if not curated_metabolite_table:
        curated_metabolite_table = CuratedMetaboliteTable.get_instance()

    name_match = curated_metabolite_table.get_matching_rows(curation_table_index, search_name)
    if name_match:
        source = name_match[curation_table_index]
        index = find_term_index_in_source(source, search_name)
        result = CompoundSearchResultModel()
        fill_from_metabolite_table(index, name_match, result)
        response.content.append(result)
    else:
        if not ws_proxy:
            ws_proxy = get_chebi_ws_proxy()
        try:
            result = ws_proxy.get_lite_entity_list(search_text=search_name, search_category=search_category,
                                                   maximum_results=200, stars=stars)
        except ChebiWsException as e:
            response.message = "An error was occurred"
            response.err = e.message
            return

        if result:
            entity = result[0]
            result_model = fill_with_complete_entity(entity.chebiId, ws_proxy)
            if result_model:
                response.content.append(result_model)
