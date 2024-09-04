import logging
import os
from typing import Any, Dict, List, Set
from app.config import get_settings

from app.services.cluster.hpc_client import HpcJob
from app.services.cluster.hpc_utils import get_new_hpc_datamover_client
from app.tasks.system_monitor_tasks.utils import (
    check_and_get_monitor_session,
    generate_random_name,
)
from app.tasks.worker import celery
from app.ws.redis.redis import get_redis_server

logger = logging.getLogger("beat")


def maintain_datamover_workers(
    registered_workers: Dict[str, Any] = None,
) -> Dict[str, str]:
    common_message_key = "datamover_common"
    results: Dict[str, List[str]] = {common_message_key: []}
    settings = get_settings()
    redis = get_redis_server()
    cluster_settings = settings.hpc_cluster
    worker_settings = settings.workers.datamover_workers
    number_of_workers = worker_settings.minimum_datamover_workers
    job_scope_prefix = cluster_settings.datamover.job_prefix
    job_name_prefix = "datamover"
    monitor_task_status_key = worker_settings.monitor_task_status_key
    monitor_task_timeout = worker_settings.monitor_task_timeout
    locked = check_and_get_monitor_session(
        monitor_task_status_key, monitor_task_timeout
    )
    if not locked:
        results[common_message_key].append("Too early to maintain workers")
        return results

    try:
        if number_of_workers < 0:
            number_of_workers = max(number_of_workers, 1)

        client = get_new_hpc_datamover_client()
        job_name = f"{job_scope_prefix}-{job_name_prefix}"
        try:
            jobs = client.get_job_status()
        except Exception as exc:
            logger.error("There is no datamover job")
            results[common_message_key].append("Failed to get current job list on HPC.")
            return results

        current_workers: List[HpcJob] = []
        current_workers_map: Dict[str, Dict[str, Any]] = {}
        current_worker_identifiers = set()
        for job in jobs:
            if job.name.startswith(job_name):
                results[job.name] = []
                current_workers.append(job)
                identifier = job.name.replace(f"{job_name}_", "")
                current_worker_identifiers.add(identifier)
                if job.name not in current_workers_map:
                    current_workers_map[job.name] = {}
                    current_workers_map[job.name]["job"] = []
                current_workers_map[job.name]["job"].append(job)

        kill_duplicate_jobs(results, current_workers_map)

        if not current_workers:
            create_datamover_worker(
                results=results, job_name=job_name, worker_identifiers=current_worker_identifiers
            )
            return results

        unexpected_state_jobs: List[HpcJob] = []
        pending_jobs: List[HpcJob] = []
        runnig_jobs: List[HpcJob] = []
        for worker in current_workers:
            status: str = worker.status
            if status.upper() == "PEND":
                pending_jobs.append(worker)
                results[worker.name].append("It is in pending state.")
            elif status.upper() == "RUN":
                results[worker.name].append("It is up and running.")
                runnig_jobs.append(worker)
            else:
                unexpected_state_jobs.append(worker)
                results[worker.name].append(
                    f"It is not running. Its state is {worker.status}."
                )

        active_job_count = len(runnig_jobs) + len(pending_jobs)
        if active_job_count < number_of_workers:
            create_datamover_worker(
                results=results, job_name=job_name, worker_identifiers=current_worker_identifiers
            )
            return results
        else:
            maintained = maintain_expired_workers(
                results, registered_workers, current_workers, current_workers_map
            )
            if maintained:
                return results
            else:
                maintained = maintain_extra_workers(
                    results, number_of_workers, current_workers_map, runnig_jobs
                )
        if unexpected_state_jobs:
            job_ids = [x.job_id for x in unexpected_state_jobs]
            client.kill_jobs(job_id_list=job_ids, failing_gracefully=True)
    finally:
        redis.set_value(monitor_task_status_key, "0", ex=monitor_task_timeout)

    return results


def create_datamover_worker(
    results: Dict[str, List[str]], job_name: str, worker_identifiers: Set[str]
):
    logger.info("Create_datamover_worker request received")
    random_name = generate_random_name(current_names=worker_identifiers)
    name = f"{job_name}_{random_name}"
    client = get_new_hpc_datamover_client()
    settings = get_settings()
    job_track_email = settings.hpc_cluster.datamover.job_track_email
    hpc_queue_name =  settings.hpc_cluster.datamover.default_queue
    worker_config = settings.workers.datamover_workers
    command = os.path.join(
        worker_config.singularity_image_configuration.docker_deployment_path,
        worker_config.start_datamover_worker_script,
    )
    args = worker_config.broker_queue_names
    client.run_singularity(task_name=name, 
                           command=command, 
                           command_arguments=args,
                           hpc_queue_name=hpc_queue_name,
                           account=job_track_email)
    
    message = f"New worker is triggered."
    results[name] = [message]
    logger.info(message)


def kill_duplicate_jobs(results: Dict[str, List[str]], current_workers_map):
    for job_name in current_workers_map:
        # kill all duplicate name tasks. Kepp the oldest one
        if len(current_workers_map[job_name]["job"]) > 1:
            duplicate_jobs: List[HpcJob] = current_workers_map[job_name]["job"]
            duplicate_jobs.sort(key=lambda x: x.submit_time, reverse=False)
            job_ids = [x.job_id for x in duplicate_jobs if x != duplicate_jobs[0]]
            client = get_new_hpc_datamover_client()
            client.kill_jobs(job_id_list=job_ids, failing_gracefully=True)
            name = current_workers_map[job_name]["job"][0].name
            message = f"Duplicate jobs are killed. {name}"
            results[name].append(message)
            logger.warning(message)


def maintain_extra_workers(
    results: Dict[str, List[str]],
    number_of_workers: int,
    current_workers_map: Dict[str, Any],
    runnig_jobs: List[HpcJob],
):
    worker_settings = get_settings().workers.datamover_workers
    redis_key_prefix = worker_settings.shutdown_signal_wait_key_prefix
    shutdown_signal_wait_time = worker_settings.shutdown_signal_wait_time
    unexpected_worker_jobs = []
    if len(runnig_jobs) > number_of_workers:
        runnig_jobs.sort(key=lambda x: x.elapsed, reverse=False)
        unexpected_worker_jobs = runnig_jobs[: (len(runnig_jobs) - number_of_workers)]

    if unexpected_worker_jobs:
        redis = get_redis_server()
        for worker_name in current_workers_map:
            key = current_workers_map[worker_name]["key"]
            redis_key = f"{redis_key_prefix}:{key}"
            status = redis.get_value(redis_key)
            if not status or status.decode() != "1":
                celery.control.broadcast("shutdown", destination=[key])
                redis.set_value(redis_key, "1", ex=shutdown_signal_wait_time)
                message = "Shutdown signal was sent to unxepected worker"
                results[worker_name].append(message)
                return True
    return False


def maintain_expired_workers(
    results: Dict[str, List[str]],
    registered_workers,
    current_workers: List[HpcJob],
    current_workers_map,
):
    worker_settings = get_settings().workers.datamover_workers
    redis_key_prefix = worker_settings.shutdown_signal_wait_key_prefix
    shutdown_signal_wait_time = worker_settings.shutdown_signal_wait_time
    expired_worker_names: List[str] = check_registered_workers(
        current_workers_map, registered_workers
    )
    selected_worker = None

    expired_active_worker_names = None
    expired_active_worker_keys = None
    if expired_worker_names:
        expired_workers: List[HpcJob] = [
            x for x in current_workers if x.name in expired_worker_names
        ]
        expired_workers.sort(key=lambda x: x.elapsed, reverse=False)
        map = current_workers_map
        expired_active_worker_keys = [
            map[x.name]["key"]
            for x in expired_workers
            if x.name in map
            and map[x.name]
            and "key" in map[x.name]
            and map[x.name]["key"]
        ]
        expired_active_worker_names = [
            x.name
            for x in expired_workers
            if x.name in map
            and map[x.name]
            and "key" in map[x.name]
            and map[x.name]["key"]
        ]
        inactive_worker_job_ids = [
            x.job_id
            for x in expired_workers
            if x.name in map
            and map[x.name]
            and ("key" not in map[x.name] or not map[x.name]["key"])
        ]
        if inactive_worker_job_ids:
            client = get_new_hpc_datamover_client()
            client.kill_jobs(inactive_worker_job_ids, failing_gracefully=True)

    if expired_active_worker_keys:
        selected_worker = expired_active_worker_keys[0]
        selected_worker_name = expired_active_worker_names[0]
        redis = get_redis_server()
        redis_key = f"{redis_key_prefix}:{selected_worker}"
        status = redis.get_value(redis_key)
        wait_time = shutdown_signal_wait_time
        if not status or status.decode() != "1":
            celery.control.broadcast("shutdown", destination=[selected_worker])
            redis.set_value(redis_key, "1", ex=wait_time)
            message = "Shutdown signal was sent to expired worker"
            results[selected_worker_name].append(message)
            return True
    return False


def check_registered_workers(
    current_workers_map, registered_workers: Dict[Any, Any]
) -> List[str]:
    if not registered_workers:
        registered_workers = celery.control.inspect().stats()
    worker_settings = get_settings().workers.datamover_workers
    max_uptime = worker_settings.maximum_uptime_in_seconds
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
