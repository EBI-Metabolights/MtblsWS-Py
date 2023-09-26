import datetime
import hashlib
import logging
import os
import random
import shutil
import socket
import string
from typing import Any, Dict, List, Set, Union
from app.config import get_settings
from app.tasks.bash_client import BashClient

from app.tasks.lsf_client import HpcJob, LsfClient
from app.tasks.worker import MetabolightsTask, celery
from app.ws.redis.redis import get_redis_server
from app.ws.settings.utils import get_cluster_settings

logger = logging.getLogger("beat")


@celery.task(bind=True, base=MetabolightsTask, name="app.tasks.system_monitor_tasks.worker_maintenance.check_all_workers")
def check_all_workers(
    self,
    number_of_datamover_workers: int = -1,
    number_of_additional_localhost_workers=-1,
    remote_vm_hostnames: Union[str, List[str]] = None,
    number_of_remote_vm_workers=-1,
    maximum_shutdown_signal_per_time: int = -1,
):
    results = {}
    registered_workers = celery.control.inspect().stats()
    result_1 = check_datamover_workers(
        number_of_workers=number_of_datamover_workers,
        maximum_shutdown_signal_per_time=maximum_shutdown_signal_per_time,
        registered_workers=registered_workers,
    )

    result_2 = check_additional_vm_workers(
        number_of_additional_localhost_workers=number_of_additional_localhost_workers,
        remote_vm_hostnames=remote_vm_hostnames,
        number_of_remote_vm_workers=number_of_remote_vm_workers,
        maximum_shutdown_signal_per_time=maximum_shutdown_signal_per_time,
        registered_workers=registered_workers,
    )
    results.update(result_1)
    results.update(result_2)
    return results


@celery.task(bind=True, base=MetabolightsTask, name="app.tasks.system_monitor_tasks.worker_maintenance.check_additional_vm_workers")
def check_additional_vm_workers(
    self,
    number_of_additional_localhost_workers: int = -1,
    number_of_remote_vm_workers: int = -1,
    remote_vm_hostnames: Union[str, List[str]] = None,
    maximum_shutdown_signal_per_time: int = -1,
    registered_workers: Dict[str, Any] = None,
    start_maximum_worker_on_vm_per_time=1,
) -> Dict[str, str]:
    results = {}
    started_localhost_workers = 0
    started_remote_vm_workers = {}
    cluster_settings = get_cluster_settings()
    if number_of_additional_localhost_workers < 0:
        number_of_additional_localhost_workers = max(cluster_settings.number_of_additional_localhost_workers, 0)

    if not registered_workers:
        registered_workers = celery.control.inspect().stats()

    if maximum_shutdown_signal_per_time < 0:
        maximum_shutdown_signal_per_time = max(cluster_settings.maximum_shutdown_signal_per_time, 0)

    if not remote_vm_hostnames:
        remote_vm_hostnames = cluster_settings.vm_worker_hostnames.split(",")

    if number_of_remote_vm_workers < 0:
        number_of_remote_vm_workers = max(cluster_settings.number_of_remote_vm_workers, 0)

    local_hostname = socket.gethostname()
    worker_name_prefix = f"{cluster_settings.job_project_name}_vm_worker"
    for i in range(number_of_additional_localhost_workers):
        messages = []
        index = i + 1
    
        full_worker_name = f"{worker_name_prefix}_{index}@{local_hostname}"
        logger.info(f"Check {full_worker_name}")
        worker_name = f"{worker_name_prefix}_{index}"
        
        
        if full_worker_name not in registered_workers:
            if started_localhost_workers < start_maximum_worker_on_vm_per_time:
                paramters = {
                    "application_deployment_path": os.getcwd(),
                    "worker_name": worker_name,
                    "worker_queue": "common-tasks",
                    "conda_environment": cluster_settings.localhost_conda_environment,
                }
                file_path = BashClient.prepare_script_from_template(
                    cluster_settings.start_vm_worker_script_template_name, **paramters
                )
                result = BashClient.execute_command(f"bash < {file_path}")
                try:
                    os.remove(file_path)
                except Exception as exc:
                    logger.warning(f"Error while deleting temp file {file_path}")
                        
                started_localhost_workers += 1
                messages.append("Worker was started.")
            else:
                messages.append("Worker will start next time.")
        else:
            messages.append("Worker is already registered.")

        results[full_worker_name] = " ".join(messages)

    for remote_hostname in remote_vm_hostnames:
        started_remote_vm_workers[remote_hostname] = 0
        for i in range(number_of_remote_vm_workers):
            messages = []
            index = i + 1
            worker_name = f"{worker_name_prefix}_{index}"
            full_worker_name = f"{worker_name_prefix}_{index}@{remote_hostname}"
            if full_worker_name not in registered_workers:
                if started_remote_vm_workers[remote_hostname] < start_maximum_worker_on_vm_per_time:
                    ssh_command = BashClient.build_ssh_command(hostname=remote_hostname, username=get_settings().hpc_cluster.datamover.connection.username)
                    paramters = {
                        "application_deployment_path": cluster_settings.remote_vm_deployment_path,
                        "worker_name": worker_name,
                        "worker_queue": "common-tasks",
                        "conda_environment": cluster_settings.remote_vm_conda_environment,
                    }
                    file_path = BashClient.prepare_script_from_template(
                        cluster_settings.start_vm_worker_script_template_name, **paramters
                    )
                    result = BashClient.execute_command(f"{ssh_command} bash < {file_path}")
                    try:
                        os.remove(file_path)
                    except Exception as exc:
                        logger.warning(f"Error while deleting temp file {file_path}. {str(exc)}")
                    started_remote_vm_workers[remote_hostname] += 1
                    messages.append("Worker was started.")
                else:
                    messages.append("Worker will start next time.")
            else:
                messages.append("Worker is already registered.")
            
            results[full_worker_name] = " ".join(messages)

    return results


def generate_random_name(length=4, current_names: Set[str]=None):    
    current_datetime = str(datetime.datetime.now())
    # Update the hash object with the datetime string
    name = None
    sha1 = hashlib.sha1()
    while True:
        sha1.update(current_datetime.encode())
        sha1_hash = sha1.hexdigest()
        name = sha1_hash[:length]
        if not current_names or name not in current_names:
            return name
        
def create_new_datamover_worker(worker_name: str):
        client: LsfClient = LsfClient()
        settings = get_settings()
        docker_config = settings.hpc_cluster.singularity
        command = os.path.join(docker_config.docker_deployment_path, settings.hpc_cluster.datamover.worker.start_datamover_worker_script)
        command_arguments = settings.hpc_cluster.datamover.worker.broker_queue_names
        client.run_singularity(worker_name, command, command_arguments, unique_task_name=False)
            
@celery.task(bind=True, base=MetabolightsTask, name="app.tasks.system_monitor_tasks.worker_maintenance.check_datamover_workers")
def check_datamover_workers(
    self,
    number_of_workers: int = -1,
    maximum_shutdown_signal_per_time: int = 1,
    registered_workers: Dict[str, Any] = None,
) -> Dict[str, str]:
    
    common_message_key = "common"
    results = {common_message_key: []}
    cluster_settings = get_cluster_settings()
    redis = get_redis_server()
    
    try:
        monitor_status = redis.get_value("metabolights:system_monitor_tasks.worker_maintenance")
        if monitor_status and monitor_status.decode() == "1":
            results[common_message_key].append("Too early to maintain workers")
            return results
        redis.set_value("metabolights:monitor:datamover_workers", "1", ex=40)
        if number_of_workers < 0:
            number_of_workers = max(get_settings().hpc_cluster.datamover.worker.number_of_datamover_workers, 1)
        if maximum_shutdown_signal_per_time <= -1:
            maximum_shutdown_signal_per_time = max(cluster_settings.maximum_shutdown_signal_per_time, 0)

        client = LsfClient()
        worker_name_prefix = (
            f"{cluster_settings.job_project_name}_{get_settings().hpc_cluster.datamover.queue_name}_worker"
        )
        
        current_worker_identifiers = set()
        try:
            jobs = client.get_job_status()
        except Exception as exc:
            logger.error("There is no datamover job")
            results[common_message_key].append("Failed to get current job list on HPC.")
            return results
        current_workers: List[HpcJob] = []
        current_workers_map: Dict[str, Dict[str, Any]] = {}
        for job in jobs:
            if job.name.startswith(worker_name_prefix):
                results[job.name] = []
                current_workers.append(job)
                identifier = job.name.replace(f"{worker_name_prefix}_", "")
                current_worker_identifiers.add(identifier)
                if job.name not in current_workers_map:
                    current_workers_map[job.name] = {}
                    current_workers_map[job.name]["job"] = []
                current_workers_map[job.name]["job"].append(job)
                
        
        kill_duplicate_jobs(results, current_workers_map)
        
        if not current_workers:
            create_datamover_worker_with_name(results, worker_name_prefix, current_worker_identifiers)
            return results
        
        unexpected_state_jobs: List[HpcJob]  = []
        pending_jobs: List[HpcJob]  = []
        runnig_jobs: List[HpcJob]  = []
        for worker in current_workers:
            status: str = worker.status
            if status.upper() == "PEND":
                pending_jobs.append(worker)
                results[worker.name].append("It is in pending state.")
            elif status.upper() == "RUN":
                results[worker.name].append("It is up and running.")
                runnig_jobs.append(worker)   
            else:
                unexpected_state_jobs[worker]
                results[worker.name].append(f"It is not running. Its state is {worker.status}.")
        
        active_job_count = len(runnig_jobs) + len(pending_jobs)
        if active_job_count < number_of_workers:
            create_datamover_worker_with_name(results, worker_name_prefix, current_worker_identifiers)
            return results
        else:
            maintained = maintain_expired_workers(results, registered_workers, current_workers, current_workers_map)
            if maintained:
                return results
            else:
                maintained = maintain_extra_workers(results, number_of_workers, current_workers_map, runnig_jobs)                 
        if unexpected_state_jobs:
            job_ids = [x.job_id for x in unexpected_state_jobs]
            client.kill_jobs(job_id_list=job_ids, failing_gracefully=True)
    finally:
        redis.set_value("metabolights:monitor:datamover_workers", "0", ex=30)
        
    return results

def create_datamover_worker_with_name(results, worker_name_prefix, worker_identifiers):
    random_name = generate_random_name(current_names=worker_identifiers)
    name = f"{worker_name_prefix}_{random_name}"
    create_new_datamover_worker(name)
    message = f"New worker is triggered."
    results[name] = [message]
    logger.info(message)

def kill_duplicate_jobs(results, current_workers_map):
    for job_name in current_workers_map:
            # kill all duplicate name tasks. Kepp the oldest one
        if len(current_workers_map[job_name]["job"]) > 1:
            duplicate_jobs: List[HpcJob] = current_workers_map[job_name]["job"]
            duplicate_jobs.sort(key=lambda x: x.submit_time, reverse=False)
            job_ids = [x.job_id for x in duplicate_jobs if x != duplicate_jobs[0]]
            client = LsfClient()
            client.kill_jobs(job_id_list=job_ids, failing_gracefully=True)
            name = current_workers_map[job_name]['job'][0].name
            message = f"Duplicate jobs are killed. {name}"
            results[name].append(message)
            logger.warning(message)

def maintain_extra_workers(results: Dict[str, List[str]], number_of_workers, current_workers_map: Dict[str, Any], runnig_jobs: List[HpcJob]):
    unexpected_worker_jobs = []
    if len(runnig_jobs) > number_of_workers:
        runnig_jobs.sort(key=lambda x: x.submit_time, reverse=False)
        unexpected_worker_jobs = runnig_jobs[:(len(runnig_jobs) - number_of_workers)]
                
    if unexpected_worker_jobs:
        redis = get_redis_server()
        for worker_name in current_workers_map:
            key = current_workers_map[worker_name]["key"]
            redis_key_prefix = f"metabolights:datamover_workers:signal"
            status = redis.get_value(f"{redis_key_prefix}:{key}")
            if not status or status.decode() != "1":
                celery.control.broadcast("shutdown", destination=[key])
                redis.set_value(f"metabolights:datamover_workers:signal:{key}", "1", ex=60*10)
                
                results[worker_name].append("Shutdown signal was sent to unxepected worker")
                return True
    return False

def maintain_expired_workers(results: Dict[str, List[str]], registered_workers, current_workers: List[HpcJob], current_workers_map):
    expired_worker_names: List[str] = check_registered_workers(current_workers_map, registered_workers)
    selected_worker = None
    redis_key_prefix = f"metabolights:datamover_workers:signal"

    expired_active_worker_names = None
    expired_active_worker_keys = None
    if expired_worker_names:
        expired_workers: List[HpcJob] = []
        for worker in current_workers:
            if worker.name in expired_worker_names:
                expired_workers.append(worker)
        expired_workers.sort(key=lambda x: x.submit_time, reverse=False)
        map = current_workers_map
        expired_active_worker_keys =  [map[x.name]["key"] for x in expired_workers if x.name in map and map[x.name] and "key" in map[x.name] and map[x.name]["key"]]
        expired_active_worker_names =  [x.name for x in expired_workers if x.name in map and map[x.name] and "key" in map[x.name] and map[x.name]["key"]]
        inactive_worker_job_ids = [ x.job_id for x in expired_workers if x.name in map and map[x.name] and ("key" not in map[x.name] or not map[x.name]["key"])]
        if inactive_worker_job_ids:
            client = LsfClient()
            client.kill_jobs(job_id_list=inactive_worker_job_ids, failing_gracefully=True)
                
    if expired_active_worker_keys:    
        selected_worker = expired_active_worker_keys[0]
        selected_worker_name = expired_active_worker_names[0]
        redis = get_redis_server()
        status = redis.get_value(f"{redis_key_prefix}:{selected_worker}")
        if not status or status.decode() != "1":
            celery.control.broadcast("shutdown", destination=[selected_worker])
            redis.set_value(f"metabolights:datamover_workers:signal:{selected_worker}", "1", ex=60*10)
            results[selected_worker_name].append("Shutdown signal was sent to expired worker")
            return True
    return False

def check_registered_workers(current_workers_map, registered_workers: Dict[Any, Any]) -> List[str]:
    if not registered_workers:
        registered_workers = celery.control.inspect().stats()
    max_uptime = get_settings().hpc_cluster.datamover.worker.datamover_worker_maximum_uptime_in_seconds
    expired_worker_names = []
    for key in registered_workers:
        worker_name = key.split("@")[0]
                
        if worker_name in current_workers_map:
            current_workers_map[worker_name]["key"] = key
            current_workers_map[worker_name]["uptime"] = 0
            stat = registered_workers[key]
            if "uptime" in stat:
                uptime = stat["uptime"]
                current_workers_map[worker_name]["uptime"] = uptime
                if uptime >= max_uptime:
                    expired_worker_names.append(worker_name)
    return expired_worker_names




if __name__ == "__main__":
    # check_additional_vm_workers()
    # result = check_datamover_workers()
    result = check_additional_vm_workers()
    print(result)
    # cluster_settings = get_cluster_settings()
    # project_name = cluster_settings.job_project_name
    # worker_name_prefix = (
    #     f"{cluster_settings.job_project_name}_{get_settings().hpc_cluster.datamover.queue_name}_worker"
    # )
    # local_hostname = socket.gethostname()
    # results = celery.control.inspect().stats()
    # for key in results:
    #     if f"{worker_name_prefix}_1" in key:
    #         celery.control.broadcast("shutdown", destination=[key])
    #     print(results[key])
