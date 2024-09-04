from app.config import get_settings
from app.tasks.system_monitor_tasks.integrity_tests.utils import check_result
import requests

@check_result(category="chebi")
def check_classyfire():
    url = get_settings().chebi.pipeline.search_services.classyfire_url
    response = requests.get(url + '/queries/20228.json', headers={"Content-Type": "application/json"}, timeout=10)
    response.raise_for_status()
    return str(response.json()['id'])


@check_result(category="chebi")
def check_chebi_wsdl():
    url = get_settings().chebi.service.connection.chebi_ws_wsdl
    response = requests.get(url, headers={"Content-Type": "application/json"}, timeout=10)
    response.raise_for_status()
    return str(response.json()['id'])