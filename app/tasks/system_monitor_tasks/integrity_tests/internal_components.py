import os
from smtplib import SMTP
from app.config import get_settings
from app.tasks.common_tasks.basic_tasks.execute_commands import execute_bash_command as common_worker_execute
from app.tasks.datamover_tasks.basic_tasks.file_management import get_permission
from app.tasks.system_monitor_tasks.integrity_tests.utils import check_result
from app.utils import current_time
from app.ws.elasticsearch.elastic_service import ElasticsearchService
from app.ws.redis.redis import RedisStorage, get_redis_server
from app.ws.study.user_service import UserService
import requests

@check_result(category="database")
def check_postgresql():
    mtbls_submitter_user = get_settings().auth.service_account.email
    user = UserService.get_instance().get_simplified_user_by_username(mtbls_submitter_user)
    return user.userName

@check_result(category="services")
def check_email_service():
    host = get_settings().email.email_service.connection.host
    port = str(get_settings().email.email_service.connection.port)
    
    with SMTP(host=host, port=port) as smtp:
        result = smtp.noop()
    return str(result)

@check_result(category="elasticsearch")
def check_elasticsearch():
    return ElasticsearchService.get_instance().get_study("MTBLS1", request_timeout=5)

@check_result(category="redis")
def check_redis():
    redis: RedisStorage = get_redis_server()
    key = "test_redis_connection:value"
    value = str(current_time().timestamp())
    
    try:
        redis.set_value(key, value, ex=10)
    except Exception as exc:
        raise Exception(f"Set key operation failed: {str(exc)}")

    try:
        actual = redis.get_value(key).decode()
    except Exception as exc:
        raise Exception(f"Get key operation failed: {str(exc)}")
    
    if actual != value:
        raise Exception(f"Result contains unexpected value: Actual: {actual} Expected: {value}")
    return actual

@check_result(category="mtblsws_py")
def check_mtblsws_py_version():
    host = get_settings().server.service.app_host_url
    context = get_settings().server.service.resources_path

    url = f"{host}{context}"
        
    response = requests.get(url, timeout=5)
    response.raise_for_status()
    return response.json()


    
@check_result(category="mtblsws_py")
def check_mtblsws_py_datamover_worker():
    settings = get_settings()
    ftp_path = settings.hpc_cluster.datamover.mounted_paths.cluster_private_ftp_root_path
    
    inputs = {"source_path": ftp_path}
    task = get_permission.apply_async(kwargs=inputs, expires=5)
    return task.get(timeout=5)

    
@check_result(category="mtblsws_py")
def check_mtblsws_py_common_worker():
    metadata_paths = get_settings().study.mounted_paths.study_metadata_files_root_path
    study_path = os.path.join(metadata_paths, "MTBLS1")
    inputs = {"command": f"ls -l {study_path}"}
    task = common_worker_execute.apply_async(kwargs=inputs, expires=5)

    return task.get(timeout=5)