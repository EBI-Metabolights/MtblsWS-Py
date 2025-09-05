import requests
from app.ws.chebi.settings import get_chebi_ws_settings
import logging
from zeep import helpers

logger = logging.getLogger(__file__)

def chebi_search_v2(search_term=""):
    chebi_ws2_url = get_chebi_ws_settings().chebi_ws_wsdl
    chebi_es_search = f'{chebi_ws2_url}/public/es_search/?'
    try:
        log(f"-- Querying ChEBI web services v2 with term {search_term}")
        url = f'{chebi_es_search}term={search_term}&page=1&size=15'
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            json_resp = resp.json()
            result = json_resp['results'][0]
            if json_resp['results']:
                result = json_resp['results'][0]
                chebi_id = result['_source']['chebi_accession']
            return chebi_id
    except Exception as e:
        log(' -- Error querying ChEBI ws2. Error ' + str(e), mode='error')
        return ""
        
def get_complete_chebi_entity_v2(chebi_id=""):
    chebi_ws2_url = get_chebi_ws_settings().chebi_ws_wsdl
    chebi_compound_api = f'{chebi_ws2_url}/public/compound/'
    try:
        log(f"-- Querying ChEBI web services v2 with ChebiID {chebi_id}")
        url = f'{chebi_compound_api}{chebi_id}/?only_ontology_parents=false&only_ontology_children=false'
        resp = requests.get(url, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data:
                entity = helpers.serialize_object(data, dict)
                return entity
    
    except Exception as e:
        log(' -- Error querying ChEBI ws2. Error ' + str(e), mode='error')
        return {}

def log(message, silent=False, mode='info'):
    if not silent:
        print(str(message))
        if mode == 'info':
            logger.info(str(message))
        elif mode == 'error':
            logger.error(str(message))
        else:
            logger.warning(str(message))
            
            