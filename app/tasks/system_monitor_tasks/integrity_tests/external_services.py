from app.config import get_settings
from app.tasks.system_monitor_tasks.integrity_tests.utils import check_result
import requests

@check_result(category="external_dependencies")
def check_ols():
    base_url = get_settings().external_dependencies.api.ols_api_url
    url = f"{base_url.rstrip("/")}/search?q=lung&queryFields=label,synonym&fieldList=iri,label,short_form,obo_id,ontology_name"
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    
    return f"{url}: {str(response.json()['response']['docs'][0])}"

