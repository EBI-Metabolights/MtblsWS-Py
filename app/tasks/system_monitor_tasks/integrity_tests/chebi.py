import requests

from app.config import get_settings
from app.tasks.system_monitor_tasks.integrity_tests.utils import check_result


@check_result(category="chebi")
def check_chebi_wsdl():
    url = get_settings().chebi.service.connection.chebi_ws_wsdl
    response = requests.get(
        url, headers={"Content-Type": "application/json"}, timeout=5
    )
    response.raise_for_status()
    return f"{url}"
