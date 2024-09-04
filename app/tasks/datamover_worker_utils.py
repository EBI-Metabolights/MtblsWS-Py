import logging
import os
import socket
import time
from typing import List

import kombu

from app.config import get_settings
from app.services.cluster.hpc_client import HpcClient, HpcJob
from app.services.cluster.hpc_utils import get_new_hpc_datamover_client
from app.tasks.system_monitor_tasks import heartbeat
from app.tasks.worker import celery as app

logger = logging.getLogger('wslog_datamover')

def get_status(worker_name: str):
    client: HpcClient = get_new_hpc_datamover_client()
    project_name = get_settings().hpc_cluster.datamover.job_prefix
    name = f"{project_name}_{worker_name}"
    jobs: List[HpcJob] = client.get_job_status([name])
    return jobs
    
def create_datamover_worker(worker_name: str):
    project_name = get_settings().hpc_cluster.datamover.job_prefix
    name = f"{project_name}_{worker_name}"
    client: HpcClient = get_new_hpc_datamover_client()
    settings = get_settings()
    worker_config = settings.workers.datamover_workers
    command = os.path.join(
        worker_config.singularity_image_configuration.docker_deployment_path,
        worker_config.start_datamover_worker_script,
    )
    args = worker_config.broker_queue_names.split(",")
    # args.append(name)
    sif_image_file_url = os.environ.get("SINGULARITY_IMAGE_FILE_URL")
    config_file_path = os.environ.get("DATAMOVER_CONFIG_FILE_PATH", default="")
    if not config_file_path:
        config_file_path = os.path.realpath(worker_config.singularity_image_configuration.config_file_path)
        if not config_file_path:
            config_file_path = os.path.realpath("datamover-config.yaml")
    
    secrets_folder_path = os.environ.get("DATAMOVER_SECRETS_PATH", default="")
    if not secrets_folder_path:
        secrets_folder_path = os.path.realpath(worker_config.singularity_image_configuration.secrets_path)
        if not secrets_folder_path:
            secrets_folder_path = os.path.realpath(".datamover-secrets")
    time_limit = settings.workers.datamover_workers.maximum_uptime_in_seconds             
    job_id, messages = client.run_singularity(
        task_name=name, 
        command=command, 
        command_arguments=",".join(args) + f" {name}", 
        singularity_image_configuration=worker_config.singularity_image_configuration,
        unique_task_name=False,
        sif_image_file_url=sif_image_file_url, 
        source_config_file_path=config_file_path, 
        source_secrets_folder_path=secrets_folder_path,
        maximum_uptime_in_seconds=time_limit,
        temp_directory_path=get_settings().server.temp_directory_path,
        mem_in_mb=settings.workers.datamover_workers.worker_memory_in_mb,
        cpu=settings.workers.datamover_workers.worker_cpu,
    )

    for message in messages:
        print(message)
    return job_id

def create_queue(name: str):
    if not app.conf.task_queues:
        app.conf.task_queues = []
    queue_exists = False
    for item in app.conf.task_queues:
        queue: kombu.Queue = item
        if queue.name == name:
            queue_exists = True
            break
    if not queue_exists:
        print(f"Queue is created for datamover worker: {name}")
        app.conf.task_queues.append(kombu.Queue(name=name, routing_key="heartbeat"))


def delete_queue(name: str):
    if not app.conf.task_queues:
        return
    target = None
    for item in app.conf.task_queues:
        queue: kombu.Queue = item
        if queue.name == name:
            target = queue
            break
    if not target:
        print(f"Queue will be deleted for datamover worker: {name}")
        app.conf.task_queues.remove(queue)


def delete_current_workers(worker_name: str):
    logger.info(f"Kill current {worker_name} workers")
    jobs: List[HpcJob] = get_status(worker_name)
    if jobs:
        job_ids = [job.job_id for job in jobs if "RUN" in job.status.upper()]
        client: HpcClient = get_new_hpc_datamover_client()
        result = client.kill_jobs(
            job_ids, failing_gracefully=True
        )
        if len(result.job_ids) < len(job_ids):
            logger.warning(f"{result.stdout}, {result.stderr}")
            return False
        else:
            logger.info("Current workers were killed")
            logger.info(f"{result.stdout}, {result.stderr}")
    else:
        logger.info("No worker runs on datamover")

    return True


def start_worker(worker_name: str):
    up = False
    for i in range(5):
        logger.warning(f"Create datamover worker. Attempt {i + 1}.")
        create_datamover_worker(worker_name)
        time.sleep(5)
        started = False
        for _ in range(3):
            jobs = get_status(worker_name)
            if jobs and "RUN" in jobs[0].status.upper():
                started = True
                break
            time.sleep(10)
        if started:
            up = True
            break
    if not up:
        logger.warning("Datamover worker is not running.")
        return False
    else:
        logger.info("Datamover worker is running.")
        return True


def restart_datamover_worker(worker_name: str):
    project_name = get_settings().hpc_cluster.datamover.job_prefix
    name = f"{project_name}_{worker_name}"

    success = delete_current_workers(worker_name)
    if not success:
        return False

    success = start_worker(worker_name)
    if not success:
        return False

    create_queue(name)
    # return True
    worker_version = ping_datamover_worker(worker_name, retry=5, timeout=10, wait_period=5, initial_wait_period=20)
    if not worker_version:
        print("Datamover worker is not active.")
        return False
    else:
        print(f"Datamover worker '{name}' is active now.")
        return True


def ping_datamover_worker(worker_name: str, retry=1, timeout=5, wait_period=1, initial_wait_period=0):
    project_name = get_settings().hpc_cluster.datamover.job_prefix
    name = f"{project_name}_{worker_name}"
    if initial_wait_period > 0:
        time.sleep(initial_wait_period)
    input_value = socket.gethostname()
    for _ in range(retry):
        try:
            task = heartbeat.ping.apply_async(queue=name, args=[input_value])
            result = task.get(timeout=timeout)
            if result and "reply_for" in result and result["reply_for"] == input_value:
                if result and "worker_version" in result:
                    return result["worker_version"]
                else:
                    return None
            else:
                time.sleep(wait_period)
        except Exception as ex:
            print(f"No response from datamover worker {name}: {str(ex)}")

    return None
