from smtplib import SMTP
from app.config import get_settings
from app.tasks.system_monitor_tasks.integrity_tests.utils import check_result
from app.ws.elasticsearch.elastic_service import ElasticsearchService
from app.ws.study.user_service import UserService
import requests

@check_result(category="app-component")
def check_postgresql():
    mtbls_submitter_user = get_settings().auth.service_account.email
    return UserService.get_instance().get_simplified_user_by_username(mtbls_submitter_user)

@check_result(category="app-component")
def check_elasticsearch():
    return ElasticsearchService.get_instance().get_study("MTBLS1")

@check_result(category="app-component")
def check_email():
    host = get_settings().email.email_service.connection.host
    port = str(get_settings().email.email_service.connection.port)
    
    with SMTP(host=host, port=port) as smtp:
        return  smtp.noop()

@check_result(category="app-component")
def check_mtblsws_py():
    host = get_settings().server.service.mtbls_ws_host
    context = get_settings().server.service.resources_path

    url = f"{host}{context}"
        
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    return response.text
