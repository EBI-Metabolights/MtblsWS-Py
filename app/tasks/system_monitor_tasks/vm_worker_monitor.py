import logging
import os
import socket
from typing import Any, Dict, List, Set

from app.config import get_settings
from app.config.model.worker import HostWorkerConfiguration
from app.tasks.bash_client import BashClient
from app.tasks.system_monitor_tasks.utils import check_and_get_monitor_session, generate_random_name
from app.tasks.worker import celery
from app.ws.redis.redis import get_redis_server
from app.ws.settings.utils import get_cluster_settings

logger = logging.getLogger("beat")


def maintain_vm_workers(
    host: HostWorkerConfiguration, registered_workers: Dict[str, Any] = None
) -> Dict[str, str]:
    settings = get_settings()
    common_message_key = f"{host.hostname}_common"
    
    results: Dict[str, List[str]] = {common_message_key: []}
    cluster_settings = settings.hpc_cluster.configuration
    worker_settings = settings.workers.vm_workers

    monitor_task_status_key = worker_settings.monitor_task_status_key_prefix
    monitor_task_timeout =  worker_settings.monitor_task_timeout
    key = f"{monitor_task_status_key}:{host.hostname}"
    locked = check_and_get_monitor_session(key, monitor_task_timeout)
    if not locked:
        results[common_message_key].append("Too early to maintain workers")
        return results
    try:
        number_of_workers = host.maximim_vm_workers
        hostname = host.hostname
        if number_of_workers < 0:
            number_of_workers = 0

        if not registered_workers:
            registered_workers = celery.control.inspect().stats()

        worker_name_prefix = f"{cluster_settings.job_project_name}_vm_worker"
        current_worker_identifiers = set()
        current_worker_names = set()
        current_workers: List[str] = []
        current_workers_map: Dict[str, Dict[str, Any]] = {}
        for worker in registered_workers:   
            if worker.startswith(worker_name_prefix) and worker.endswith(hostname):
                results[worker] = ["Worker is up and runnning"]
                current_workers.append(worker)
                identifier = worker.replace(f"{worker_name_prefix}_", "")
                identifier = identifier.replace(f"@{hostname}", "")
                current_worker_names.add(worker.replace(f"@{hostname}", ""))
                current_worker_identifiers.add(identifier)
                if worker not in current_workers_map:
                    current_workers_map[worker] = {}
        initiated_vm_workers = get_initiated_vm_workers(hostname)
        activated_workers = initiated_vm_workers.union(current_worker_names)
        
        if len(activated_workers) < number_of_workers:
            start_vm_worker(host, current_names=current_worker_identifiers, results=results)

        return results
    finally:
        redis = get_redis_server()
        redis.set_value(key, "0", ex=monitor_task_timeout)

def get_initiated_vm_workers(hostname: str) -> Set[str]:
    settings = get_settings()
    cluster_settings = get_settings().hpc_cluster.configuration
    initiate_vm_worker_key_prefix = settings.workers.vm_workers.initiate_vm_worker_key_prefix

    worker_name_prefix = f"{cluster_settings.job_project_name}_vm_worker"
    redis = get_redis_server()
    pattern_prefix = f"{initiate_vm_worker_key_prefix}:{hostname}:"
    results = redis.search_keys(f"{pattern_prefix}{worker_name_prefix}_*")
    initiated_vm_workers = set()
    if results:
        for item in results:
            key = item.decode()
            status = redis.get_value(key)
            if status and status.decode() == "1":
                initiated_vm_workers.add(key.replace(pattern_prefix, ""))
    return initiated_vm_workers
    
def start_vm_worker(host: HostWorkerConfiguration, current_names: Set[str], results: Dict[str, List[str]]):
    settings = get_settings()
    cluster_settings = settings.hpc_cluster.configuration
    worker_settings = settings.workers.vm_workers
    initiate_vm_worker_key_prefix = worker_settings.initiate_vm_worker_key_prefix
    initiate_vm_worker_wait_timeout = worker_settings.initiate_vm_worker_wait_timeout
    hostname = host.hostname
    worker_name_prefix = f"{cluster_settings.job_project_name}_vm_worker"

    random_name = generate_random_name(current_names=current_names)
    name = f"{worker_name_prefix}_{random_name}"
    redis = get_redis_server()
    redis_key = f"{initiate_vm_worker_key_prefix}:{hostname}:{name}"
    status = redis.get_value(redis_key)
    if not status or status.decode() != "1":
        port = str(get_settings().server.service.rest_api_port)
        paramters = {
            "application_deployment_path": host.deployment_path,
            "worker_name": name,
            "worker_queue": host.worker_queue_names,
            "conda_environment": host.conda_environment,
            "server_port": port
        }
        template = get_settings().workers.vm_workers.start_vm_worker_script_template_name
        file_path = BashClient.prepare_script_from_template(template, **paramters)
        localhost = socket.gethostname()
        success = False
        result = None
        if localhost == hostname:
            result = BashClient.execute_command(f"{file_path}")
        else:
            username = get_settings().hpc_cluster.datamover.connection.username
            ssh_command = BashClient.build_ssh_command(hostname, username)
            result = BashClient.execute_command(f"{ssh_command} bash < {file_path}")
        success = True if result and result.returncode == 0 else False
        if success:
            redis.set_value(redis_key, "1", ex=initiate_vm_worker_wait_timeout)
            results[name] = ["Worker was started."]
        else:
            results[name] = [f"Worker was not initiated. Err: {', '.join(result.stdout)}"]
        try:
            os.remove(file_path)
        except Exception as exc:
            logger.warning(f"Error while deleting temp file {file_path}")
    
        return success
    return False

if __name__ == "__main__":
    # check_additional_vm_workers()
    result = maintain_vm_workers("wp-np3-15.ebi.ac.uk", 1)
    # result = check_additional_vm_workers()
    print(result)
